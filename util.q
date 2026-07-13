
/ ---------- time helpers ----------
/ Python sends Unix microseconds (since 1970-01-01) as long
.util.us2ts:{[u] 1970.01.01D00:00:00.000000000 + 1000*u};      / -> timestamp
.util.us2date:{`date$.util.us2ts x};  / -> date
.util.binance24hvolumecurl:{[sym]  
    cmd:"curl -s \"https://api.binance.com/api/v3/ticker/24hr?symbol=",string[sym],"\""; 
     res:raze system cmd;  :"F"$(.j.k res)`volume}
     
.util.binancevolume:symlist!.util.binance24hvolumecurl@/:symlist;
.util.numbucket:50;     
.util.v:.util.binancevolume%.util.numbucket;     

.util.tsize:{(tables`.)!(count each value each tables `.)};
.util.hcheck:{ show .util.tsize[]; -1 "memory used - ", string[(.Q.w[]%4 xexp 10)`used], " MB";};

.util.vwap:{[tab;syms;start_time;end_time]
        select vwap:wavg[qty;px], volume:sum[qty] by sym from
         (update sym,trade_us from .util.viewtab[tab]) 
          where sym in syms, trade_us within (start_time;end_time)};   
             
.util.twap:{[tab;s;start_time;end_time]
  if[1<count s;:raze .util.twap[tab;;start_time;end_time] each s];
      obs: select sym,trade_us,px from .util.viewtab[tab] where sym in s, trade_us<=end_time;
      if[(0=count obs) or start_time>max obs`trade_us;'`$"time window unavailable for query"];  
      t:asc distinct (start_time,(exec trade_us from obs where trade_us>start_time,trade_us<end_time),end_time);
      grid:([]sym:count[t]#s;trade_us:t);
      res: aj[`sym`trade_us; grid; obs];
      / need a price at t0 (i.e. an obs at/before t0)
      if[null first res`px; :0n];  / or throw / resync logic   -- check !
      dt: 1_deltas res`trade_us; 
      :([sym:enlist[s]]twap:enlist (sum((-1_ res`px) * dt)) % (end_time-start_time));
    };

.util.viewtab:{[tab] 
       / convert the long type time fields into timestamp
       tcol: cols tab;
       tscol:tcol where (tcol like "*_us") and "j"=(meta tab)[;`t] each tcol;
       ![tab;();0b;((),tscol)!(`.util.us2ts),'((),tscol)]};     

                                                                
.util.vpinstat:([sym:()] latest:();p50:();p90:());         
       
.util.vpin:{
    vpintrades:update cumvol:sums qty by sym from trades;
    vpintrades:update bucketID:cumvol div .util.v[sym] by sym from vpintrades; 
    buckets:: select bvol:sum qty*(side=`B), svol:sum qty*(side=`S) by sym,bucketID from vpintrades; 
    update imbalance:abs[bvol-svol]%.util.v[sym] from `buckets;
    update vpin:mavg[.util.numbucket;imbalance] from `buckets;
 
    {d: asc exec vpin from buckets where sym=x;
    `.util.vpinstat upsert (x;(select by sym from buckets)[x;`vpin]; d floor 0.5*count[d];d floor 0.9*count[d])} each  symlist;
       
      }
      
      
.util.rvar:{
   / realized variance calculated over timeframe as in quoteState, 
  if[0<count quotestate;
     qs:.util.viewtab quotestate;
     tmark:xcol[`sym`etime; `sym`exch_us#0!select by sym from qs],' select sym,stime:exch_us from qs where exch_us=(min;exch_us) fby sym;
     tmark[`dt]:(tmark[`etime]-tmark[`stime])%60*1000000000; / convert to minute  
     g: select mid:log last 0.5*bbid+bask by sym, time:0D00:00:01 xbar exch_us from qs;   /resample to 1s grid
     g: update r: deltas[first[mid];mid] by sym from g;
     rv: select rsquared:sum r*r by sym, bucket: 0D00:01:00 xbar time from g;  / 1-min realized total variation
     :select sym,totVar,annual_Vol:sqrt(525600*totVar%dt), daily_Vol:sqrt(1440*totVar%dt) from ((select totVar:sum rsquared by sym from rv),'tmark)];
    }
    
    

