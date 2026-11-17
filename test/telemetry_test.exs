defmodule TelemetryTest do
  use ExUnit.Case, async: true

  alias Postgrex.{Query, Result, TextQuery}

  @events [
    [:postgrex, :query, :start],
    [:postgrex, :query, :stop],
    [:postgrex, :query, :exception]
  ]

  setup do
    handler_id = make_ref()
    :telemetry.attach_many(handler_id, @events, &__MODULE__.send_to_test/4, self())
    on_exit(fn -> :telemetry.detach(handler_id) end)

    conn = start_supervised!({Postgrex, database: "postgrex_test", backoff_type: :stop})
    {:ok, conn: conn}
  end

  def send_to_test(event, measurements, metadata, test) do
    if self() == test, do: send(test, {:telemetry, event, measurements, metadata})
  end

  defp stops do
    receive do
      {:telemetry, [:postgrex, :query, :start], _, %{telemetry_span_context: span_context}} ->
        assert_receive {:telemetry, [:postgrex, :query, :stop], _,
                        %{telemetry_span_context: ^span_context} = metadata}

        [metadata | stops()]
    after
      100 -> []
    end
  end

  test "a query emits one start and one stop in the calling process", %{conn: conn} do
    assert {:ok, %Result{rows: [[1], [2]]}} =
             Postgrex.query(conn, "SELECT * FROM generate_series(1, $1::int)", [2])

    assert_receive {:telemetry, [:postgrex, :query, :start], %{system_time: _, monotonic_time: _},
                    start_metadata}

    assert_receive {:telemetry, [:postgrex, :query, :stop], %{duration: duration}, stop_metadata}

    assert duration > 0

    assert %{
             query: %Query{statement: "SELECT * FROM generate_series(1, $1::int)"},
             database: "postgrex_test",
             hostname: "localhost",
             port: 5432,
             source: nil,
             telemetry_span_context: span_context
           } = start_metadata

    assert is_reference(span_context)

    assert stop_metadata ==
             Map.merge(start_metadata, %{error: nil, num_rows: 2, returned_rows: 2})

    assert stops() == []
  end

  test "returned rows are only the rows the server sent back", %{conn: conn} do
    Postgrex.transaction(conn, fn conn ->
      Postgrex.query!(conn, "CREATE TEMP TABLE telemetry_rows (a int)", [])
      stops()

      Postgrex.query!(conn, "INSERT INTO telemetry_rows VALUES (1), (2)", [])
      assert [%{num_rows: 2, returned_rows: nil}] = stops()

      Postgrex.query!(conn, "INSERT INTO telemetry_rows VALUES (3) RETURNING a", [])
      assert [%{num_rows: 1, returned_rows: 1}] = stops()

      Postgrex.query!(conn, "UPDATE telemetry_rows SET a = a + 1", [])
      assert [%{num_rows: 3, returned_rows: nil}] = stops()

      Postgrex.query!(conn, "SELECT * FROM telemetry_rows WHERE a > 100", [])
      assert [%{num_rows: 0, returned_rows: 0}] = stops()
    end)
  end

  test "the query's source option is in the metadata", %{conn: conn} do
    assert {:ok, _} = Postgrex.query(conn, "SELECT 1", [], source: "events")
    assert [%{source: "events"}] = stops()
  end

  test "preparing and executing a named query are one query each", %{conn: conn} do
    assert {:ok, query} = Postgrex.prepare(conn, "telemetry_named", "SELECT $1::int")
    assert [%{query: %Query{name: "telemetry_named"}, num_rows: nil, error: nil}] = stops()

    assert {:ok, _, %Result{rows: [[7]]}} = Postgrex.execute(conn, query, [7])
    assert [%{query: %Query{name: "telemetry_named"}, num_rows: 1, error: nil}] = stops()

    assert :ok = Postgrex.close(conn, query)
    assert stops() == []
  end

  test "a prepared query whose parameters fail to encode ends with the encode error", %{
    conn: conn
  } do
    Enum.each(
      [
        fn -> Postgrex.query(conn, "SELECT $1::int2", [100_000]) end,
        fn -> Postgrex.prepare_execute(conn, "", "SELECT $1::int2", [100_000]) end,
        fn -> Postgrex.prepare_execute(conn, "telemetry_encode", "SELECT $1::int2", [100_000]) end
      ],
      fn run ->
        assert_raise DBConnection.EncodeError, run

        assert [
                 %{
                   query: %Query{statement: "SELECT $1::int2"},
                   num_rows: nil,
                   error: %DBConnection.EncodeError{}
                 }
               ] =
                 stops()
      end
    )

    assert {:ok, _} = Postgrex.query(conn, "SELECT 1", [])
    assert [%{query: %Query{statement: "SELECT 1"}}] = stops()
  end

  test "prepare_execute is one query, prepared or cached", %{conn: conn} do
    Enum.each(["", "telemetry_prepare_execute", "telemetry_prepare_execute"], fn name ->
      assert {:ok, _, %Result{rows: [[7]]}} =
               Postgrex.prepare_execute(conn, name, "SELECT $1::int", [7])

      assert [%{query: %Query{name: ^name}, num_rows: 1, error: nil}] = stops()
    end)
  end

  test "a failed prepare in prepare_execute ends the query", %{conn: conn} do
    assert {:error, %Postgrex.Error{postgres: %{code: :syntax_error}}} =
             Postgrex.prepare_execute(conn, "telemetry_bad", "SELEC 1", [])

    assert [%{error: %Postgrex.Error{}}] = stops()

    assert {:ok, _, _} = Postgrex.prepare_execute(conn, "telemetry_good", "SELECT 1", [])
    assert [%{query: %Query{name: "telemetry_good"}, num_rows: 1}] = stops()
  end

  test "a failed query reports the Postgres error on stop", %{conn: conn} do
    assert {:error, %Postgrex.Error{postgres: %{code: :syntax_error}}} =
             Postgrex.query(conn, "SELEC 1", [])

    assert [%{query: %Query{statement: "SELEC 1"}, error: error, num_rows: nil}] = stops()
    assert %Postgrex.Error{postgres: %{code: :syntax_error}} = error
  end

  test "a transaction reports its BEGIN and COMMIT", %{conn: conn} do
    assert {:ok, _} = Postgrex.transaction(conn, &Postgrex.query!(&1, "SELECT 1", []))

    assert [
             %{query: %TextQuery{statement: "BEGIN"}, error: nil},
             %{query: %Query{statement: "SELECT 1"}, num_rows: 1},
             %{query: %TextQuery{statement: "COMMIT"}, error: nil}
           ] = stops()
  end

  test "a stream is one query from declare, or prepare, to deallocate", %{conn: conn} do
    statement = "SELECT * FROM generate_series(1, 5)"

    Postgrex.transaction(conn, fn conn ->
      query = Postgrex.prepare!(conn, "", statement)
      stops()

      assert conn |> Postgrex.stream(query, [], max_rows: 2) |> Enum.to_list() |> length() == 3
      assert [%{query: %Query{statement: ^statement}, num_rows: 5, returned_rows: 5}] = stops()

      assert conn |> Postgrex.stream(statement, [], max_rows: 2) |> Enum.to_list() |> length() ==
               3

      assert [%{query: %Query{statement: ^statement}, num_rows: 5, returned_rows: 5}] = stops()

      assert [_] = conn |> Postgrex.stream(statement, [], max_rows: 2) |> Enum.take(1)
      assert [%{query: %Query{statement: ^statement}, num_rows: 2, returned_rows: 2}] = stops()
    end)
  end

  test "a COPY FROM STDIN is one query from open to finish", %{conn: conn} do
    Postgrex.transaction(conn, fn conn ->
      Postgrex.query!(conn, "CREATE TEMP TABLE telemetry_copy (a int)", [])
      query = Postgrex.prepare!(conn, "", "COPY telemetry_copy FROM STDIN")
      stops()

      Enum.into(["1\n", "2\n", "3\n"], Postgrex.stream(conn, query, []))
    end)

    assert [
             %{
               query: %Query{statement: "COPY telemetry_copy FROM STDIN"},
               num_rows: 3,
               returned_rows: nil
             },
             %{query: %TextQuery{statement: "COMMIT"}}
           ] = stops()
  end

  @tag :capture_log
  test "a dropped connection ends every open stream", %{conn: conn} do
    killer = start_supervised!({Postgrex, database: "postgrex_test"}, id: :killer)
    statement = "SELECT * FROM generate_series(1, 5)"

    assert_raise Postgrex.Error, fn ->
      Postgrex.transaction(conn, fn conn ->
        %{rows: [[pid]]} = Postgrex.query!(conn, "SELECT pg_backend_pid()", [])
        stops()

        Enum.zip_with(
          Postgrex.stream(conn, statement, [], max_rows: 1),
          Postgrex.stream(conn, statement, [], max_rows: 1),
          fn _, _ -> Postgrex.query!(killer, "SELECT pg_terminate_backend($1)", [pid]) end
        )
      end)
    end

    assert [error, error] =
             stops()
             |> Enum.filter(&match?(%{query: %Query{statement: ^statement}}, &1))
             |> Enum.map(& &1.error)

    assert %Postgrex.Error{postgres: %{code: :admin_shutdown}} = error
  end

  @tag :capture_log
  # The query times out and the pool times out the checkout at the same moment: whichever comes
  # first ends the query, the other is ignored.
  test "a timed out query is stopped once, with the cancellation or the checkout timeout", %{
    conn: conn
  } do
    attach_from_any_process()

    assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
             Postgrex.query(conn, "SELECT pg_sleep(1)", [], timeout: 50)

    assert_receive {:telemetry, [:postgrex, :query, :start], _,
                    %{query: %Query{statement: "SELECT pg_sleep(1)"}, telemetry_span_context: ref}}

    assert_receive {:ended, :stop, ^ref, %{error: error}}, 1_000
    refute_receive {:ended, _, ^ref, _}, 200

    assert match?(%Postgrex.Error{postgres: %{code: :query_canceled}}, error) or
             match?(%DBConnection.ConnectionError{}, error)
  end

  def send_from_any_process(event, _measurements, metadata, test) do
    send(test, {:ended, List.last(event), metadata.telemetry_span_context, metadata})
  end

  defp attach_from_any_process do
    handler_id = make_ref()

    :telemetry.attach_many(
      handler_id,
      [[:postgrex, :query, :stop], [:postgrex, :query, :exception]],
      &__MODULE__.send_from_any_process/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  @tag :capture_log
  test "a stream whose checkout times out is stopped once, by the pool, with the error", %{
    conn: conn
  } do
    attach_from_any_process()
    statement = "SELECT * FROM generate_series(1, 1000)"

    assert_raise DBConnection.ConnectionError, fn ->
      Postgrex.transaction(
        conn,
        fn conn ->
          conn
          |> Postgrex.stream(statement, [], max_rows: 1)
          |> Stream.each(fn _ -> Process.sleep(50) end)
          |> Stream.run()
        end,
        timeout: 300
      )
    end

    assert_receive {:telemetry, [:postgrex, :query, :start], _,
                    %{query: %Query{statement: ^statement}, telemetry_span_context: ref}}

    assert_receive {:ended, :stop, ^ref, %{error: %DBConnection.ConnectionError{}}}, 1_000
    refute_receive {:ended, _, ^ref, _}, 200
  end
end
