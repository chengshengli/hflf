# AGENTS.md

This registry defines all autonomous agents used in the High-Performance Real-Time K-Line Aggregation Service written in Go. Each agent is composable and fully documented for LLM-assisted orchestration.

---

## Agent: tick_parser

description: Parse raw .tick file lines into Tick structs with epoch timestamps and price data.
input_format:
  line: string   # Raw line from .tick file, wrapped in 【】
output_format:
  tick:
    EpochMs: int64
    Price: float64
    TotalVol: int64
dependencies: []
tags: [parser, io, tick]
example:
  input:
    line: "【2024-12-02T18:00:00Z,6196.25,100,5,2024-12-02T18:00:00Z,6200.0,6100.0,6200】"
  output:
    tick:
      EpochMs: 1733143200000
      Price: 6196.25
      TotalVol: 100

## Agent: kline_aggregator

description: Aggregate a stream of Tick objects into a current 1-minute OHLCV bar in memory.
input_format:
  tick:
    EpochMs: int64
    Price: float64
    TotalVol: int64
  state:
    LastTotalVolume: int64
    CurrentBar: nullable<KLine>
output_format:
  state:
    UpdatedTotalVolume: int64
    UpdatedCurrentBar: KLine
dependencies: [tick_parser]
tags: [aggregation, ohlcv, stateful]
example:
  input:
    tick:
      EpochMs: 1733143201500
      Price: 6196.75
      TotalVol: 110
    state:
      LastTotalVolume: 100
      CurrentBar:
        StartTimeMs: 1733143200000
        Open: 6196.25
        High: 6196.25
        Low: 6196.25
        Close: 6196.25
        Volume: 0
  output:
    state:
      UpdatedTotalVolume: 110
      UpdatedCurrentBar:
        StartTimeMs: 1733143200000
        Open: 6196.25
        High: 6196.75
        Low: 6196.25
        Close: 6196.75
        Volume: 10

## Agent: finalize_kline

description: Finalize a current KLine when a new minute starts and persist it to .1m log.
input_format:
  bar: KLine
  path: string   # Output .1m file path
output_format:
  status: string  # "ok" or error string
dependencies: [kline_aggregator]
tags: [finalization, file-io]
example:
  input:
    bar:
      StartTimeMs: 1733143200000
      Open: 6196.25
      High: 6196.75
      Low: 6196.25
      Close: 6196.75
      Volume: 10
    path: "/output/EP.1m"
  output:
    status: "ok"

## Agent: flush_current_bar

description: Write current KLine state to .1m.current file for crash recovery.
input_format:
  bar: KLine
  path: string
output_format:
  status: string
dependencies: [kline_aggregator]
tags: [file-io, snapshot]
example:
  input:
    bar:
      StartTimeMs: 1733143200000
      Open: 6196.25
      High: 6196.75
      Low: 6196.25
      Close: 6196.75
      Volume: 10
    path: "/output/EP.1m.current"
  output:
    status: "ok"

## Agent: save_state

description: Persist ServiceState (offset, last KLine epoch, volume) to JSON .state file with atomic rename.
input_format:
  state:
    TickFileLastReadOffset: int64
    LastCompletedBarEpochMs: int64
    LastTotalVolume: int64
  path: string
output_format:
  status: string
dependencies: []
tags: [state, persistence, json]
example:
  input:
    state:
      TickFileLastReadOffset: 408219
      LastCompletedBarEpochMs: 1733143200000
      LastTotalVolume: 110
    path: "/output/EP.state"
  output:
    status: "ok"

## Agent: bootstrap_state

description: Rebuild ServiceState from .1m and .tick if .state is missing or corrupt.
input_format:
  tick_file: string
  log_file: string
output_format:
  state:
    TickFileLastReadOffset: int64
    LastCompletedBarEpochMs: int64
    LastTotalVolume: int64
dependencies: [tick_parser]
tags: [recovery, bootstrap, file-io]
example:
  input:
    tick_file: "/data/EP.tick"
    log_file: "/output/EP.1m"
  output:
    state:
      TickFileLastReadOffset: 0
      LastCompletedBarEpochMs: 1733143140000
      LastTotalVolume: 950

## Agent: graceful_shutdown

description: Finalize any current bar, flush snapshot, persist state, and exit cleanly.
input_format:
  state: ServiceState
  bar: KLine
  paths:
    bar_file: string
    snapshot_file: string
    state_file: string
output_format:
  status: string
dependencies: [finalize_kline, flush_current_bar, save_state]
tags: [shutdown, consistency]
example:
  input:
    state:
      TickFileLastReadOffset: 419502
      LastCompletedBarEpochMs: 1733143260000
      LastTotalVolume: 125
    bar:
      StartTimeMs: 1733143260000
      Open: 6197.50
      High: 6198.25
      Low: 6197.00
      Close: 6197.75
      Volume: 15
    paths:
      bar_file: "/output/EP.1m"
      snapshot_file: "/output/EP.1m.current"
      state_file: "/output/EP.state"
  output:
    status: "ok"

## Agent: tick_watcher

description: Watch .tick file with fsnotify, debounce changes, and trigger tick processing.
input_format:
  path: string
  debounce_ms: int
output_format:
  event: string
dependencies: []
tags: [fsnotify, event-driven, debounce]
example:
  input:
    path: "/data/EP.tick"
    debounce_ms: 500
  output:
    event: "tick_updated"

## Agent: process_new_ticks

description: Seek to last read offset, read new lines, parse, aggregate, and persist results.
input_format:
  state: ServiceState
  tick_path: string
  output_dir: string
output_format:
  status: string
dependencies: [tick_parser, kline_aggregator, finalize_kline, flush_current_bar, save_state]
tags: [tick, batch, io, ohlcv]
example:
  input:
    state:
      TickFileLastReadOffset: 419502
      LastCompletedBarEpochMs: 1733143260000
      LastTotalVolume: 125
    tick_path: "/data/EP.tick"
    output_dir: "/output"
  output:
    status: "ok"

## Agent: main_orchestrator

description: Main loop that watches .tick, loads or bootstraps state, processes ticks and handles shutdown.
input_format:
  args:
    input_tick_file: string
    output_dir: string
output_format:
  status: string
dependencies: [bootstrap_state, process_new_ticks, graceful_shutdown]
tags: [cli, lifecycle, orchestrator]
example:
  input:
    args:
      input_tick_file: "/data/EP.tick"
      output_dir: "/output"
  output:
    status: "running"

