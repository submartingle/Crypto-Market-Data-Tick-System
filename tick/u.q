/ utility functions for tickerplant


\d .u
init:{w::t!(count t::(tables[] except `latency))#()}   

add:{[tab;s]
    if[`~tab;:"emptytablename"];
     $[not .z.w in raze w[tab;;0];
	    w[tab],:enlist (.z.w;s);
		[i:w[tab;;0]?.z.w; $[`~s; [w[tab] _: i;add[tab;s]];if[not `~w[tab;i;1];.[`.u.w;(tab;i;1);union;s]]]]]; /need to apply to w symbol to persist the change
        /no need to check if any s in the table as tables are dynamically updated and can contain new symbols and client can add subscription before new symbol arrives
	   (tab;$[99=type v:value tab;select from tab where sym in s;@[0#v;`sym;`g#]])
	   }
     

del:{[tab;h]
      w[tab] _: w[tab;;0]?h}
	  
.z.pc:{[h]  del[;h]each key w}


pub:{[tab;x]
       {[tab;x;c] 
	   /c is the subscriber list

/  send latency   
/  if[tab~`trades;
/ 	lat:([]id:x[;7];fhTime:us2ts x[;2];tpTime:.z.P;rdbTime:0Np);
/    neg[c 0] (`.latency.upd;`.latency.latency;lat)];
    
     $[`~c 1; neg[c 0] (`upd;tab;x); if[count d:select from x where sym in c 1; neg[c 0] (`upd;tab;d)]];  / need to revise the code! x is not a table
	   }[tab;x;] each w tab	   

	  }



sub:{[tab;s] 
    if[tab~`;:sub[;s] each t];
    if[not tab in t;'tab];
	add[tab;s]} 
		
eod:{(neg union/[w[;;0]])@\:(`.u.eod;x)}





\d .
