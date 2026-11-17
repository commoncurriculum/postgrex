defmodule Postgrex.Telemetry do
  @moduledoc false

  @type t :: %{start_time: integer, metadata: map, num_rows: non_neg_integer | nil}

  @spec start(map) :: t
  def start(metadata) do
    metadata = Map.put(metadata, :telemetry_span_context, make_ref())
    start_time = System.monotonic_time()
    measurements = %{system_time: System.system_time(), monotonic_time: start_time}
    :telemetry.execute([:postgrex, :query, :start], measurements, metadata)
    %{start_time: start_time, metadata: metadata, num_rows: nil}
  end

  @spec stop(t, term) :: :ok
  def stop(%{start_time: start_time, metadata: metadata, num_rows: num_rows}, error) do
    stop_time = System.monotonic_time()
    measurements = %{duration: stop_time - start_time, monotonic_time: stop_time}
    metadata = Map.merge(metadata, %{error: error, num_rows: num_rows})
    :telemetry.execute([:postgrex, :query, :stop], measurements, metadata)
  end

  @spec add_rows(t, Postgrex.Result.t() | term) :: t
  def add_rows(telemetry, %Postgrex.Result{num_rows: rows}) when is_integer(rows),
    do: %{telemetry | num_rows: (telemetry.num_rows || 0) + rows}

  def add_rows(telemetry, _result), do: telemetry

  @spec run(t, (-> result)) :: result when result: var
  def run(%{start_time: start_time, metadata: metadata}, fun) do
    fun.()
  catch
    kind, reason ->
      stop_time = System.monotonic_time()
      measurements = %{duration: stop_time - start_time, monotonic_time: stop_time}
      metadata = Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: __STACKTRACE__})
      :telemetry.execute([:postgrex, :query, :exception], measurements, metadata)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end
end
