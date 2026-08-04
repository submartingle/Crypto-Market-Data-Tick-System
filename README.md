# Crypto Market Data Tick System (kdb+/q + Python)

Personal portfolio project: real-time crypto market-data ingestion + kdb+/q analytics focused on **microstructure** and **order book dynamics**.

## Note
This repository is a personal portfolio project created independently outside any client work.
It is shared for review/evaluation only. Please do not copy, redistribute, or use any part of this code in commercial or client deliverables without my written permission.

## How This Was Built
Built on KX's standard kdb+tick architecture (`tick.q`, `tick/r.q`, `tick/u.q`), extended with a
crypto schema, the RTE analytics engine and `util.q`. The Python feed handlers were built with the
help of **Claude Code**.


## What it does

- Ingests **Binance Spot & Perp** market data
  - **WebSocket**: continuous tick feed (trades + incremental book updates)
  - **REST**: periodic order book snapshots (used for initialization + verification)
- Publishes normalized rows into kdb+ **Tickerplant (TP)** tables
- Runs all analytics in a dedicated **RTE (Real-time Engine)** subscribed to TP:
  - maintains a live limited-depth order book
  - builds `tq` (trades joined with prevailing quotes at trade time)
  - maintains `quoteState` (best bid/ask qty + price)
  - timer-driven snapshots via `.z.ts` for BBO metrics (mid, spread, OBI, L10 depth)
  - verifies book correctness by comparing FH REST snapshots vs live book (best levels for now)

---
## Status
- Binance perp contract data feeds completed, further work pending to sync with spot data for analysis
- Coinbase integration is in progress.
- No systemd/Docker/cron wiring yet.



## Run examples

#### 5 symbols, REST snapshot every 300 seconds:

```bash
# Tickerplant (TP) - start first; the feed handler connects to it on startup
q tick.q sym . -p 5010

# Real-time database (RDB) - connects to TP on :5010
q tick/r.q :5010 -p 5011

# Feed handler - publishes to TP on :5010
python3 binanceFH_REST.py \
  --symbols BTCUSDT ETHUSDT SOLUSDT DOGEUSDT XRPUSDT \
  --rest-snap-interval-s 300

# Real-time engine (RTE) - connects to TP on :5010
q RTE.q :5010 -symbols BTCUSDT ETHUSDT SOLUSDT DOGEUSDT XRPUSDT
```

Important: the RTE -symbols list must match the feed-handler --symbols list at startup, so the RTE subscribes to the same set of instruments.

## Architecture


```mermaid
flowchart LR
  subgraph EXCH["Exchanges"]
    subgraph BIN["Binance"]
      WS["WebSocket streams<br/>(trades + incremental book updates)"]
      REST["REST API<br/>(order book snapshot)"]
    end
    subgraph CB["Coinbase (in progress)"]
      CBW["WebSocket streams<br/>(in progress)"]
      CBR["REST snapshot<br/>(in progress)"]
    end
  end

  subgraph PY["Ingestion: Python feed-handlers"]
    FH["Feed Handler<br/>(asyncio)"]
    GAP["Gap detect + reconnect<br/>(sequence / lastUpdateId)"]
    NORM["Normalize + type casting<br/>(schema-aligned rows)"]
    BATCH["Batch & flush loop<br/>(.u.upd publishing path)"]
  end

  subgraph KDB["kdb+ / KDB-X Processes"]
    TP["Tickerplant<br/>(.u.upd)"]
    RDB["RDB"]
    HDB["HDB"]
  end

  subgraph TBL["Published tables (TP)"]
    subgraph SPOT["Spot"]
      BSNAP["bsnap<br/>(order book snapshots)"]
      BUPD["bupd<br/>(incremental book updates)"]
      TRD["trades"]
    end
    subgraph PERP["Perp"]
      PBSNAP["pbsnap<br/>(order book snapshots)"]
      PBUPD["pbupd<br/>(incremental book updates)"]
      PMARK["pmark<br/>(mark/index/funding/etc.)"]
      PTRD["ptrades"]
    end
  end

  subgraph CORE["CORE: RTE Real-time Engine (q)"]
    RTE["RTE<br/>subscribes to TP<br/>calculates live microstructure metrics"]
    subgraph RTEF["RTE responsibilities"]
      OB["1) Maintains live LOB (limited depth)<br/>bid/ask updated from ticks"]
      TQ["2) Builds tq: trades joined with prevailing quotes<br/>(as-of at trade time)"]
      QS["3) Maintains quoteState (BBO level/qty)<br/>updated from best bid/ask ticks"]
      BBO["4) Timer-driven snapshots (.z.ts):<br/>mid, spread, OBI, L10 depth<br/>persist to BBO table"]
      VER["5) Verification:<br/>init correctness + periodic checks<br/>compare FH REST snapshot vs live LOB<br/>(best levels for now)"]
    end
  end

  WS --> FH
  REST --> FH
  CBW -.-> FH
  CBR -.-> FH

  FH --> GAP --> NORM --> BATCH --> TP

  TP --> BSNAP
  TP --> BUPD
  TP --> TRD
  TP --> PBSNAP
  TP --> PBUPD
  TP --> PMARK
  TP --> PTRD

  TP --> RTE
  TP --> RDB
  RDB --> HDB

  RTE --> OB
  RTE --> TQ
  RTE --> QS
  RTE --> BBO
  RTE --> VER
