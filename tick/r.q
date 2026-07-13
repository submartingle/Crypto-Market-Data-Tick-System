/q tick/r.q [host]:port[:usr:pwd] [host]:port[:usr:pwd]  
/   q tick/r.q :5010 -p 5011

\d .u

eod:{t: tables[`.]; t@:where `g=attr each t@\:`sym;.Q.hdpf[`$":",.u.x 1;`:.;x;`sym];@[;`sym;`g#] each t;
    (hopen `::5012) (`.u.eod`)}  
x:.z.x,(count .z.x)_(":5010";":5012");
rep:{(.[;();:;].)each x;
     if[null first y; :()];
	 -11!y;
     system "cd ",1_-10_string first reverse y;
    }
    
tsize:{(tables`.)!(count each value each tables `.)};
hcheck:{ show tsize[]; -1 "memory used - ", string[(.Q.w[]%4 xexp 10)`used], " MB";};

\d .

upd:{[t;x] 
    t upsert x;  / must use upsert since x is not a table

	if[.stat.throughput;
		$[99h=type x;.stat.msgCount+:1;.stat.msgCount+:count x];
		if[.stat.reportinterval < .z.P - .stat.lastReport;
		elapsed:`int$(.z.P - .stat.lastReport) % 1000000000;  // seconds
		throughput:.stat.msgCount % elapsed;
		memUsed:.Q.w[][`used] % 4 xexp 10;
		-1 "Throughput: ",string[throughput]," rows/sec";
		-1 "Memory used: ", string[memUsed]," MB  ";
		.stat.lastReport:.z.P;
		.stat.msgCount:0]]
 } 
	

.u.rep .(hopen `$":",.u.x 0)"(.u.sub[`;`];`.u `i`L)"; 
system "c 20 150";

