/ ---------- raw ingestion tables (types match Python tuples) ----------
/ trade rows from Python: (exch_us; recv_us; sym; px; qty; side; tid)
trades:([] 
  exch_us:`long$();
  trade_us:`s#`long$();
  recv_us:`long$();
  sym:`g#`symbol$();
  px:`float$();
  qty:`float$();
  side:`symbol$();     / aggressor side `B or `S
  tid:`long$());

/ bupd rows: (exch_us; recv_us; sym; U; u; bids; asks)
/ bids/asks are nested lists (as Binance sends), keep as general initially
bupd:([] 
  exch_us:`long$();
  recv_us:`long$();
  sym:`g#`symbol$();
  U:`long$();
  u:`long$();
  b:();                / general: list of (px;qty) pairs (strings or floats)
  a:());

/ bsnap rows: (recv_us; sym; lastUpdateId; bids; asks)
bsnap:([] 
  recv_us:`long$();
  sym:`symbol$();
  lastupdateid:`long$();
  bids:();             / general list of (px;qty)
  asks:());


ptrades:([] 
  exch_us:`long$();      / event time (E) in microseconds
  trade_us:`s#`long$();     / trade time (T) in microseconds
  recv_us:`long$();      / local receive time in microseconds
  sym:`g#`symbol$(); 
  px:`float$(); 
  qty:`float$();         / aggregate quantity
  nq:`float$();          / normal quantity (ex-RPI) if provided else 0n
  side:`symbol$();         / aggressor side 'B'/'S' inferred from m
  agg_id:`long$();       / aggregate trade id (a)
  first_tid:`long$();    / first trade id (f)
  last_tid:`long$());      / last trade id (l)


pbupd:([] 
  exch_us:`long$();      / event time (E) in microseconds
  tx_us:`long$();        / transaction time (T) in microseconds (if present)
  recv_us:`long$(); 
  sym:`g#`symbol$();
  U:`long$();            / first update id
  u:`long$();            / final update id
  pu:`long$();           / previous final update id
  b:();                  / bids: list of (px;qty) float pairs
  a:());                   / asks: list of (px;qty) float pairs 

pbsnap:([] 
  recv_us:`long$();
  sym:`g#`symbol$();
  lastUpdateId:`long$();
  bids:();               / list of (px;qty) float pairs
  asks:());                / list of (px;qty) float pairs


pmark:([] 
  exch_us:`long$();      / event time (E) in microseconds
  recv_us:`long$();
  sym:`g#`symbol$();
  mark:`float$();        / mark price (p)
  index:`float$();       / index price (i)
  est_settle:`float$();  / estimated settle price (P) (may be 0n outside last hour)
  funding:`float$();     / funding rate (r)
  next_funding_us:`long$()); / next funding time (T) in microseconds

