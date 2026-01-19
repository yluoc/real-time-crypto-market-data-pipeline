# Real-Time Crypto Market Data Pipeline

A high-performance, production-ready WebSocket pipeline for streaming real-time cryptocurrency market data from OKX exchange. Features microsecond-precision latency instrumentation and quant-grade metrics reporting.

## Features

- **Real-time market data streaming** from OKX WebSocket API
- **Microsecond-precision latency tracking** for internal pipeline stages
- **Rolling window metrics** with percentile statistics (p50/p95/p99)
- **Zero-latency detection** and reporting
- **Multiple output formats**: stdout, JSONL files, CSV metrics export
- **Production-ready logging** with resume-friendly single-line output

## Prerequisites

- Python 3.12+ (tested on Ubuntu/WSL and Windows)
- pip package manager

## Setup

### 1. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install msgspec websockets
```

## Usage

### Basic Usage

Run with default settings (BTC-USDT and ETH-USDT on books5 channel):

```bash
python3 -m src.app
```

This will:
- Connect to OKX WebSocket API
- Subscribe to `BTC-USDT` and `ETH-USDT` on the `books5` channel
- Print events to stdout
- Save data to JSONL files in `data/okx/books5/YYYY-MM-DD/`

### Command-Line Options

#### Trading Pairs (`--symbols`)

Specify comma-separated trading pairs:

```bash
# Single symbol
python3 -m src.app --symbols "BTC-USDT"

# Multiple symbols
python3 -m src.app --symbols "BTC-USDT,ETH-USDT,SOL-USDT"

# Different quote currencies
python3 -m src.app --symbols "BTC-USDT,ETH-BTC"
```

#### Channels (`--channels`)

Specify comma-separated channels:

```bash
# Order book (default)
python3 -m src.app --channels "books5"

# Trades only
python3 -m src.app --channels "trades"

# Multiple channels
python3 -m src.app --channels "books5,trades"
```

#### Output Control

**Disable stdout output** (only save to files):

```bash
python3 -m src.app --no-stdout
```

**Disable JSONL file output** (only print to stdout):

```bash
python3 -m src.app --no-jsonl
```

**Disable both outputs** (useful for testing metrics only):

```bash
python3 -m src.app --no-stdout --no-jsonl
```

#### CSV Metrics Export

Export metrics to CSV file periodically:

```bash
# Export every 30 seconds (default)
python3 -m src.app --csv-export reports/metrics_summary.csv

# Export every 60 seconds
python3 -m src.app --csv-export reports/metrics_summary.csv --csv-export-interval 60.0

# Export every 5 seconds (for high-frequency analysis)
python3 -m src.app --csv-export reports/metrics_summary.csv --csv-export-interval 5.0
```

#### WebSocket URL (`--url`)

Use custom WebSocket endpoint:

```bash
python3 -m src.app --url "wss://ws.okx.com:8443/ws/v5/public"
```

### Common Use Cases

#### 1. Monitor Single Trading Pair

```bash
python3 -m src.app --symbols "BTC-USDT" --channels "books5"
```

#### 2. Track Trades Only

```bash
python3 -m src.app --symbols "BTC-USDT,ETH-USDT" --channels "trades" --no-jsonl
```

#### 3. Production Mode (Files Only, No Console Output)

```bash
python3 -m src.app --no-stdout --csv-export reports/metrics.csv --csv-export-interval 60.0
```

#### 4. Development/Testing (Console Only, No Files)

```bash
python3 -m src.app --no-jsonl
```

#### 5. High-Frequency Analysis with Metrics Export

```bash
python3 -m src.app \
  --symbols "BTC-USDT,ETH-USDT" \
  --channels "books5" \
  --csv-export reports/hf_metrics.csv \
  --csv-export-interval 10.0
```

## Output Format

### Stdout Events

Each market data event is printed as a single line:

```
ETH-USDT | bid=3205.85 ask=3205.86 spread=0.01 | Ex→Recv=318ms Recv→Decode=23.714us Decode→Proc=26.317us
BTC-USDT | bid=92578.70 ask=92578.80 spread=0.10 | Ex→Recv=317ms Recv→Decode=8.554us Decode→Proc=14.747us
```

Format: `SYMBOL | bid=PRICE ask=PRICE spread=SPREAD | Ex→Recv=LATENCY_MS Recv→Decode=LATENCY_US Decode→Proc=LATENCY_US`

### Metrics Output

Periodic metrics (every 1 second) show latency percentiles:

```
Metrics | Ex→Recv p50=344.0ms p95=458.0ms p99=544.0ms | Recv→Decode p50=10.743us p95=15.977us p99=17.228us (zero=0.0%) | Decode→Proc p50=15.735us p95=24.410us p99=26.317us (zero=0.0%) | Msgs: BTC-USDT:12, ETH-USDT:10
```

- **Ex→Recv**: Exchange to receive latency (milliseconds)
- **Recv→Decode**: Receive to decode latency (microseconds)
- **Decode→Proc**: Decode to process latency (microseconds)
- **zero=X.X%**: Percentage of events with zero latency (should be 0.0% on proper systems)

### JSONL Files

Data is saved to: `data/okx/{channel}/{YYYY-MM-DD}/{SYMBOL}.jsonl`

Each line is a JSON object with normalized event data.

### CSV Metrics Export

When `--csv-export` is specified, metrics are exported with columns:
- `generated_at_utc`: Timestamp
- `symbol`: Trading pair
- `channel`: Channel name
- `lat_count`, `lat_mean_ms`, `lat_std_ms`, `lat_min_ms`, `lat_max_ms`: Latency statistics
- `stale_count`, `stale_mean_ms`, `stale_std_ms`, `stale_min_ms`, `stale_max_ms`: Staleness statistics

## Stopping the Application

Press `Ctrl+C` to gracefully shutdown. The application will:
- Close WebSocket connections
- Flush remaining data to files
- Print final metrics summary
- Export final CSV (if enabled)

## Project Structure

```
real-time-crypto-market-data-pipeline/
├── src/
│   ├── app.py              # Main application entry point
│   ├── okx_ws.py           # WebSocket client
│   ├── normalizer.py       # Message normalization
│   ├── time_helpers.py     # Clock domain helpers
│   ├── metrics/
│   │   └── rolling.py      # Rolling window metrics
│   └── sinks/
│       ├── stdout.py       # Stdout output sink
│       └── jsonl.py         # JSONL file sink
└── README.md
```

## Technical Details

### Latency Instrumentation

The pipeline tracks three latency stages:

1. **Exchange→Receive**: Network latency from exchange to local receive (epoch clock, milliseconds)
2. **Receive→Decode**: JSON decoding latency (monotonic clock, nanoseconds → displayed as microseconds)
3. **Decode→Process**: Normalization latency (monotonic clock, nanoseconds → displayed as microseconds)

All internal stage latencies are stored as **integer nanoseconds** to preserve precision, especially important on Windows where `time.monotonic_ns()` has microsecond precision.

### Invariant Checks

When debug mode is enabled (`_DEBUG = True` in source), the pipeline validates:
- `decoded_ns >= recv_ns` (monotonic clock ordering)
- `proc_ns >= decoded_ns` (monotonic clock ordering)

Violations raise `RuntimeError` immediately.

## Troubleshooting

### Connection Issues

If connection fails, check:
- Internet connectivity
- OKX WebSocket endpoint availability
- Firewall settings

The application will automatically retry with exponential backoff.

### Zero Latency Warnings

If zero-rate > 0.0% for internal stages:
- On Windows: Expected due to microsecond clock precision
- On Linux: May indicate instrumentation issue
- Check metrics output for zero-rate percentage

### High Memory Usage

For long-running sessions, consider:
- Using `--no-jsonl` if you don't need file output
- Adjusting rolling window size in `RollingMetrics` (default: 5 seconds)
- Periodic restarts for very long sessions