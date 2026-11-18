defmodule Postgrex.Telemetry do
  @moduledoc false

  @type t :: %{
          start_time: integer,
          metadata: map,
          num_rows: non_neg_integer | nil,
          returned_rows: non_neg_integer | nil,
          command: atom | nil,
          ended: :atomics.atomics_ref()
        }

  @spec start(map) :: t
  def start(metadata) do
    metadata = Map.put(metadata, :telemetry_span_context, make_ref())
    start_time = System.monotonic_time()
    measurements = %{system_time: System.system_time(), monotonic_time: start_time}
    :telemetry.execute([:postgrex, :query, :start], measurements, metadata)

    %{
      start_time: start_time,
      metadata: metadata,
      num_rows: nil,
      returned_rows: nil,
      command: nil,
      ended: :atomics.new(1, [])
    }
  end

  @spec stop(t, term) :: :ok
  def stop(telemetry, error) do
    if end_once(telemetry) do
      %{start_time: start_time, metadata: metadata} = telemetry
      stop_time = System.monotonic_time()
      measurements = %{duration: stop_time - start_time, monotonic_time: stop_time}
      result = Map.take(telemetry, [:num_rows, :returned_rows, :command])
      metadata = metadata |> Map.merge(result) |> Map.put(:error, error)
      :telemetry.execute([:postgrex, :query, :stop], measurements, metadata)
    end

    :ok
  end

  @spec stop_all(%{reference => t}, term) :: :ok
  def stop_all(pending, error) do
    Enum.each(pending, fn {_ref, telemetry} -> stop(telemetry, error) end)
  end

  @spec add_result(t, term) :: t
  def add_result(telemetry, %Postgrex.Result{command: command} = result) do
    %{telemetry | command: if(is_atom(command), do: command)}
    |> add_rows(result)
  end

  def add_result(telemetry, _result), do: telemetry

  defp add_rows(telemetry, %Postgrex.Result{num_rows: num_rows, rows: rows})
       when is_integer(num_rows) do
    telemetry
    |> Map.update!(:num_rows, &((&1 || 0) + num_rows))
    |> add_returned_rows(rows)
  end

  defp add_rows(telemetry, _result), do: telemetry

  defp add_returned_rows(telemetry, rows) when is_list(rows),
    do: Map.update!(telemetry, :returned_rows, &((&1 || 0) + length(rows)))

  defp add_returned_rows(telemetry, nil), do: telemetry

  @spec run(t, (-> result)) :: result when result: var
  def run(%{start_time: start_time, metadata: metadata} = telemetry, fun) do
    fun.()
  catch
    kind, reason ->
      if end_once(telemetry) do
        stop_time = System.monotonic_time()
        measurements = %{duration: stop_time - start_time, monotonic_time: stop_time}
        metadata = Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: __STACKTRACE__})
        :telemetry.execute([:postgrex, :query, :exception], measurements, metadata)
      end

      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp end_once(%{ended: ended}), do: :atomics.compare_exchange(ended, 1, 0, 1) == :ok
end
