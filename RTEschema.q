asklivebk:([sym:`symbol$();px:`float$()]qty:`float$())
bidlivebk:([sym:`symbol$();px:`float$()]qty:`float$())

/ Per-symbol book state (keyed)
/ - mode: `BOOT`LIVE`RESYNC
/ - lastu: last applied depth update id (u)
/ - lastU: last applied event's U (optional but useful)
/ - lasteventus: exch_us of last applied update
/ - lastrecv_us: recv_us of last applied update
/ - lastsnapu: snapshot lastupdateid used to bootstrap
/ - gapcount/desync_count: diagnostics
/ - updatedus: local timestamp when we last updated this state (optional)

bookstate:([
  sym:`g#`symbol$();
  mode:`symbol$()];
  lastu:`long$();
  lastU:`long$();
  lasteventus:`long$();
  lastrecv_us:`long$();
  lastsnapu:`long$();
  gapcount:"i"$();
  updatedus:"p"$());


quotestate:([]
     sym:`g#`symbol$();
     exch_us:`long$();
     lastupdid:`long$();
     bbid:`float$();
     bbidsize:`float$();
     bask:`float$();
     basksize:`float$());
     
tq:([]
    exch_us:`long$();
    trade_us:`s#`long$();
    recv_us:`long$();
    sym:`g#`symbol$();
    lastupdid:`long$();
    px:`float$();
    qty:`float$();
    side:`symbol$();
    tid:`long$();
    bbid:`float$();
    bbidsize:`float$();
    bask:`float$();
    basksize:`float$());
    

bbo:([] ts:`s#`timestamp$(); sym:`g#`symbol$();
     bestbid:`float$(); bestask:`float$(); mid:`float$(); spread:`float$();OBI10:`float$();depth_bid_10:`float$();depth_ask_10:`float$())
     
