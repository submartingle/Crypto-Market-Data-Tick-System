.stat.msgCount:0;
.stat.reportinterval:10000000000;  / report interval in ns (10s)
.stat.startTime:.stat.lastReport:.z.P;
.stat.throughput:0b;

.latency.latency:([]id:`long$();fhTime:"p"$();tpTime:"p"$();rdbTime:"p"$());
.latency.stats:([]id:`long$();recTime:"p"$();latency_FH:`int$();latency_TP:`int$());
.latency.sampleRate:0.4

/ insert latency rows, sampling per-hop latencies (microseconds) into .latency.stats
.latency.upd:{[t;x]
    x:update rdbTime:.z.P from x;
    t insert x;
    if[.latency.sampleRate > rand 1.0;
        latencyfromFH:`int$((x`rdbTime)-x`fhTime)%1000;
        latencyfromTP:`int$((x`rdbTime)-x`tpTime)%1000;
        `.latency.stats insert ([]id:x`id;recTime:x`rdbTime;latency_FH:latencyfromFH;latency_TP:latencyfromTP)]
    }

/ percentile summary of sampled latencies per route (FH->RDB, TP->RDB)
.latency.report:{[]
    num:count .latency.stats;
    latency_FH: asc .latency.stats`latency_FH;
    r1:`p50`p95`p99`maxlat`minlat`avglat!(latency_FH floor 0.5*num;latency_FH floor 0.95*num;latency_FH floor 0.99*num;max latency_FH;min latency_FH;avg latency_FH);
    latency_TP: asc .latency.stats`latency_TP;
    r2:`p50`p95`p99`maxlat`minlat`avglat!(latency_TP floor 0.5*num;latency_TP floor 0.95*num;latency_TP floor 0.99*num;max latency_TP;min latency_TP;avg latency_TP);
    ([]route:`FH_RDB`TP_RDB),'(flip r1,'r2)}
