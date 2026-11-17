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
      {:telemetry, [:postgrex, :query, :stop], _, metadata} -> [metadata | stops()]
      {:telemetry, _, _, _} -> stops()
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
    assert stop_metadata == Map.merge(start_metadata, %{error: nil, num_rows: 2})
    assert stops() == []
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

  test "a stream is one query from declare to deallocate", %{conn: conn} do
    Postgrex.transaction(conn, fn conn ->
      query = Postgrex.prepare!(conn, "", "SELECT * FROM generate_series(1, 5)")
      stops()

      chunks = conn |> Postgrex.stream(query, [], max_rows: 2) |> Enum.to_list()
      assert length(chunks) == 3
    end)

    assert [
             %{query: %Query{statement: "SELECT * FROM generate_series(1, 5)"}, num_rows: 5},
             %{query: %TextQuery{statement: "COMMIT"}}
           ] = stops()
  end

  test "a COPY FROM STDIN is one query from open to finish", %{conn: conn} do
    Postgrex.transaction(conn, fn conn ->
      Postgrex.query!(conn, "CREATE TEMP TABLE telemetry_copy (a int)", [])
      query = Postgrex.prepare!(conn, "", "COPY telemetry_copy FROM STDIN")
      stops()

      Enum.into(["1\n", "2\n", "3\n"], Postgrex.stream(conn, query, []))
    end)

    assert [
             %{query: %Query{statement: "COPY telemetry_copy FROM STDIN"}, num_rows: 3},
             %{query: %TextQuery{statement: "COMMIT"}}
           ] = stops()
  end

  @tag :capture_log
  test "a timed out query reports the cancellation on stop", %{conn: conn} do
    assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
             Postgrex.query(conn, "SELECT pg_sleep(1)", [], timeout: 50)

    assert [%{query: %Query{statement: "SELECT pg_sleep(1)"}, error: error}] = stops()
    assert %Postgrex.Error{postgres: %{code: :query_canceled}} = error
  end
end
