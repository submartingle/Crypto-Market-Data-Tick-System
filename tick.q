/   q tick.q sym . -p 5010

/ coins: `BTCUSDT`ETHUSDT`SOLUSDT`DOGEUSDT`XRPUSDT

system"l tick/",(src:first .z.x,enlist"sym"),".q"  

if[not system"p";system "p 5010"];
/ if[not system"t";system "t 1000"];

\l tick/u.q



\d .u


tick:{[sch;logf]
       init[];

	  @[;`sym;`g#] each lst:tables `.;
	  d::.z.D;
	 /create log file path
	  if[count logf;
	   logpath::`$":",logf,"/",sch,10#".";
	   l::ld d];
	   
	  }
	  
ld:{if[not type key L::`$(-10_string logpath),"_", string x;   /x is the date
    .[L;();:;()]];  / create the logfile
    i::j::-11!(-2;L);
    if[1<count i;
	 -2 (string L)," is a corrupt log. Truncate to length ",(string last i)," and restart";
     exit 1];
	 hopen L}



upd:{[t;x]
    l enlist (`upd;t;x);
    .u.i+:1;         / i total number of valid chunks   
	 pub[t;x];
	 }



ts:{if[x>d;if[d<x-1;system"t 0";'"more than one day?"];eod[d]]} 

.z.ts:{ts .z.D}	  
	

\d .
.u.tick[src;.z.x 1];
system"c 20 150";

