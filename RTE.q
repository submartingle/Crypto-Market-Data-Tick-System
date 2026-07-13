system"c 20 150";

isbookready:0b;
isvalidated:0b;

.u.x:.z.x; / 5010 TP, 5012 HDB    
.u.rep:{ (.[;();:;].) each x; 
     if[null first y; :()];
	 -11!y}  /y - valid number of chunks and log file path


.u.updformat:{[d]  
        bidupd:raze {y[x;2],'value each/: string y[x;5]}[;d] each til count d; / bid update
        askupd:raze {y[x;2],'value each/: string y[x;6]}[;d] each til count d; / ask update
        :(bidupd;askupd)}
                
                
.u.validatelive:{[d] 
        if[(asc symlist)~asc exec distinct sym from d;  
          u:flip value flip select from d where sym in symlist,i=(first;i) fby sym;  / since it is the first row of each sym, once validated the whole chunk of data should be applied
             $[1=prd ({y[1]=(x[(y[0],`BOOT);`lastu]+1)}[bookstate;] each u[;2 3]);
                 [-1 "validated for live streaming!";
                 `isvalidated set 1b;
                 `bookstate upsert flip (u[;2];count[symlist]#`LIVE;u[;4];u[;3];u[;0];u[;1];u[;4];0;.z.P);
                 vd:.u.updformat[flip value flip d];        /note here need to upsert all d not just u
                `asklivebk upsert vd[1];
                `bidlivebk upsert vd[0];
                delete from `bupd];['`$"validation fail, restart process...";exit 2]]]}
                         

.u.bbo:{ 
       if[(0=count bidlivebk) or 0=count asklivebk; -1 "orderbook not available!"; :()];
       t:(select bestbid:max px by sym from bidlivebk where qty<>0),'(select bestask:min px by sym from asklivebk where qty<>0);
       t:update depth_bid_10:0.0, depth_ask_10:0.0 from t;           
       bidvalue:select sum 10#qty by sym from bidlivebk where sym in symlist;      
       askvalue:select sum 10#qty by sym from asklivebk where sym in symlist;
       t:update ts:.z.P, OBI10:(depth_bid_10-depth_ask_10)%(depth_bid_10+depth_ask_10) from 
           update mid:0.5*bestask+bestbid,spread:bestask-bestbid,depth_bid_10:bidvalue[sym;`qty], depth_ask_10:askvalue[sym;`qty] by sym from t;
       t:xcols[`ts`sym`bestbid`bestask`mid`spread`OBI10`depth_bid_10`depth_ask_10;0!t];
       `bbo upsert t;
       }            
        
   
.u.bkclean:{ [symbols]
      `bidlivebk set 2!select from (0!bidlivebk) where 500>(rank neg@;px) fby sym;
      `asklivebk set 2!select from (0!asklivebk) where 500>(rank;px) fby sym};
         
.u.eod:{}; /placeholder
                                                                              
	  
bookinit:{
                 
    {`asklivebk upsert ((bsnapt`sym) x),'(bsnapt`asks) x; `bidlivebk upsert ((bsnapt`sym) x),'(bsnapt`bids) x}@/: til count symlist;
      
     {[s] 
       symlastid:.[!;(flip (0!select by sym from `bsnap))`sym`lastupdateid];
       lastu:symlastid@s;
       bupd_sym:select from bupd where sym=s;
       start_id:first exec i from bupd_sym where (U<=lastu) and u>=lastu; / sometimes U=u on one line
       if[(null start_id) or bupd_sym[start_id+1;`U]<> 1+lastu; '`$"invalid data, resync"];          
        
         bupd::bupd except bupd_sym;
        {`asklivebk upsert (x,'value each/: string y`a)}[s;] each (1+start_id)_(bupd_sym);   /replay all rows from replayed data bupd
        {`bidlivebk upsert (x,'value each/: string y`b)}[s;] each (1+start_id)_(bupd_sym);            
            
        `bookstate upsert (s;`BOOT;(last bupd_sym)`u;(last bupd_sym)`U;(last bupd_sym)`exch_us; (last bupd_sym)`recv_us;lastu;0;.z.P);
          } each symlist;     
            
      `px xasc  delete from `asklivebk where qty=0;
      `px xdesc delete from `bidlivebk where qty=0;  
      
       t:(select bid:max px by sym from bidlivebk),'(select ask:min px by sym from asklivebk);       
       if[not all (t`bid)<=t`ask;'`$"corrupted bidask data!"];
       
         `isbookready set 1b;
         -1 "orderbook initialization complete"; 
     }
  

upd: upsert;
.u.rep .(hopen `$":",.u.x 0)"(.u.sub[`;`];`.u `i`L)";


symlist:$[first null inputsym:`$(.Q.opt .z.x)`symbols;exec distinct sym from bsnap;inputsym];
system"l RTEschema.q"
system"l util.q";

bsnap: select from bsnap where sym in symlist;
bupd:  select from bupd  where sym in symlist;
bsnapt:neg[count symlist]#bsnap;
symlastupdid:()!(); / dictionary keeping track of lastupdateId for each symbol



bookinit[];
nextchecktime:.z.P;


upd:{[t;x]

    if[t~`bupd;
       if[not isvalidated;
           if[(0=count bupd) or count[symlist]>count exec distinct sym from bupd;
                      -1 "incomplete symbols, adding to bupd table..."; :t upsert x];           
           :.u.validatelive[bupd]];
       
          d:.u.updformat[x];        
         `asklivebk upsert d[1];
         `bidlivebk upsert d[0];
         
        `sym`px xasc  delete from `asklivebk where qty=0;
        `sym`px xdesc delete from `bidlivebk where qty=0;
        
        qs:1!xcol[`sym`exch_us`lastupdid;`sym`exch_us`u#0!select by sym from (flip cols[bupd]!flip x)];
        bmkt: (select bbid:first px, bbidsize:first qty by sym from `bidlivebk where sym in distinct x[;2], px=(max;px) fby sym),'(select bask:first px, basksize:first qty by sym from `asklivebk where sym in distinct x[;2], px=(min;px) fby sym);
        `quotestate upsert 0!lj[qs;bmkt];
        
        ];

        
    if[t~`trades;
        t upsert x;   
        `tq upsert aj[`sym`exch_us;neg[count x]#trades;quotestate];     
        ];


    if[t~`bsnap;        
      if[all all not null bookstate@/: (;`LIVE) each symlist;  /   state verification 
         t upsert x;
         syms:distinct x[;1];
         bsnapt::neg[count syms]#bsnap;
         symlastupdid::(bsnapt`sym)!bsnapt`lastupdateid;
         bidbk::askbk::0#bidlivebk;       
         {`askbk upsert ((bsnapt`sym) x),'(bsnapt`asks) x;  `bidbk upsert ((bsnapt`sym) x),'(bsnapt`bids) x}@/: til count syms;
          {if[symlastupdid[x] in quotestate`lastupdid;    
           if[(value exec bbid,bbidsize,bask,basksize from `quotestate where lastupdid=symlastupdid[x])~(value exec px,qty from `bidbk where sym=x, qty>0, px=max px),(value exec px,qty from `askbk where sym=x, qty>0, px=min px);
             -1 string[x]," best bid-offer verified"];]
            } each syms;
           ]];  
           
        }
       


.z.ts:{ 
     .u.bbo[]; 
    if[.z.P >= nextchecktime;
         .u.bkclean[symlist];
          .util.vpin[];
     / - free up memory, disable for vpin calc
     / if[10000<count trades; `trades set 0#trades];  
     / if[10000<count quotestate; `quotestate set 0#quotestate];
        if[not all (bbo`bestbid)<=(bbo`bestask);'`$"corrupted bbo table!"];
        
       nextchecktime::.z.P+0D00:10;
         ];         
      };
     

