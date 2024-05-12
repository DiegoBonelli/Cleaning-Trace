
####
####
#  R Code to load and clean TRACE Enhanced
#   
# Author: Diego Bonelli
# Date: 25/01/2021
# Preprocessing: Code 1, 2, 3  
#               
# Notes: 
#     1)  
#
####
####

rm(list=ls())

# set wd and load functions
# trace files are in Data/Trace file path
setwd("")

# Load Libraries
suppressWarnings(suppressMessages({
  require(lubridate, quietly = T) # Useful package for dates
  require(zoo, quietly = T) # Useful package for dates (yearmon format)
  require(dplyr, quietly = T) # usefull for ntile function
  require(data.table, quietly = T) # data.table
  require(haven, quietly = T) # read sas files
  require(bizdays, quietly = T) # package for business days
}))

# cutoff for reversal and median price filter
cutoff=0.1
load_rmetrics_calendars(1970:2030)


##
## load trace  ----
##

trace=fread("Data/Trace/trace_enhanced.csv",nThread=setDTthreads(0L))


table(trace$trc_st)
# filter after 6 feb 2012 and create dt for trades,cancellations and reversals
post_TR=trace[cusip_id!=""& trd_rpt_dt >= 20120206][trc_st%in%c("T","R")]
post_XC=trace[cusip_id!=""& trd_rpt_dt >= 20120206][trc_st%in% c("X", "C")]
post_Y=trace[cusip_id!=""& trd_rpt_dt >= 20120206][trc_st%in%c("Y")]

# filter before 6 feb 2012 and create dt for trades,cancellations and reversals
pre_T=trace[cusip_id!=""& trd_rpt_dt <  20120206][trc_st=="T"]
pre_C=trace[cusip_id!=""& trd_rpt_dt <  20120206][trc_st=="C"]
pre_W=trace[cusip_id!=""& trd_rpt_dt <  20120206][trc_st=="W"]



# Replication of the cleaning steps from Dick-Nielsen (2009, 2014) 
# Differently from DN code, here we keep all trades not only dealers and customers

##
## Clean Post 2012  ----
##

# Takes out all cancellations and corrections;
# These transactions should be deleted together with the original report;

# Delete the cancellations and corrections found, i.e. remove all trades from the data.table,
# which are included in post_XC
#  These transactions can be matched by message sequence number
#  and date. I furthermore match on cusip, volume, price, date,
#  time, buy-sell side, contra party;

post_TR=post_TR[!post_XC[,.(cusip_id,
                            entrd_vol_qt,
                            rptd_pr,
                            trd_exctn_dt,
                            trd_exctn_tm,
                            rpt_side_cd,
                            cntra_mp_id,
                            msg_seq_nb)], 
                on=list(cusip_id,
                        entrd_vol_qt,
                        rptd_pr,
                        trd_exctn_dt,
                        trd_exctn_tm,
                        rpt_side_cd,
                        cntra_mp_id,
                        msg_seq_nb)]

# Remove Reversals
post_TR <- post_TR[!post_Y[, .(cusip_id,
                               entrd_vol_qt,
                               rptd_pr,
                               trd_exctn_dt,
                               trd_exctn_tm,
                               rpt_side_cd,
                               cntra_mp_id,
                               msg_seq_nb)],,
                   on = list(cusip_id,
                             entrd_vol_qt,
                             rptd_pr,
                             trd_exctn_dt,
                             trd_exctn_tm,
                             rpt_side_cd,
                             cntra_mp_id,
                             msg_seq_nb)]

rm(post_Y,post_XC)

# Before filtering the data 186428707
# after filtering 182424313, which means 4004394 rows are removed

##
## Clean Pre 2012  ----
##

# Match Cancellation by the 7 keys: Cusip_ID, Execution Date and Time,
# Quantity, Price, Buy/Sell Indicator and Reported Date

# Fewer obs canceled using this left join than the ones labeled as C in the original dataset. 
# No of TRC_ST = C in data = 1382511
# No of cases matched = 1356250
# Reason: some TRC_ST=C cases show an ORIG_MSG_SEQ_NB that doesn't exist in the orignal dataset.

# remove Cancellations
pre_T=pre_T[!pre_C[,.(cusip_id,
                      orig_msg_seq_nb,
                      rptd_pr,
                      trd_rpt_dt,
                      entrd_vol_qt,
                      trd_exctn_dt,
                      trd_exctn_tm)]
            , on = list(msg_seq_nb = orig_msg_seq_nb,
                        cusip_id,
                        trd_rpt_dt,
                        rptd_pr,
                        entrd_vol_qt,
                        trd_exctn_dt,
                        trd_exctn_tm)]

rm(pre_C)

# remove corrections
# NOTE: a trade can have more than one round of correction
#  One W to correct an older W, which then corrects the original T
# create one dt with both msg and omsg
pre_w_msg=copy(pre_W)[,flag:="msg"]
pre_w_omsg=copy(pre_W)[,":="(flag="omsg",msg_seq_nb=NULL)]
setnames(pre_w_omsg,"orig_msg_seq_nb","msg_seq_nb")
pre_w_tot=rbind(pre_w_msg,pre_w_omsg,fill=T)
rm(pre_w_msg,pre_w_omsg)

# Count the number of appearance (napp) of a msg_seq_nb: 
# If appears more than once then it is part of later correction;
pre_w_n=copy(pre_w_tot)[,list( cusip_id, trd_exctn_dt, trd_exctn_tm, msg_seq_nb)
][,napp:=.N,by=list( cusip_id, trd_exctn_dt, trd_exctn_tm, msg_seq_nb)]

# Check whether one msg_seq_nb is associated with both msg and orig_msg or only to orig_msg;
# If msg_seq_nb appearing more than once is associated with only orig_msg - 
# It means that more than one msg_seq_nb is linked to the same orig_msg_seq_nb for correction. 
# If ntype=2 then a msg_seq_nb is associated with being both msg_seq_nb and orig_msg_seq_nb;
pre_w_t=unique(copy(pre_w_tot)[,list( cusip_id, trd_exctn_dt, trd_exctn_tm, msg_seq_nb,flag)])
pre_w_t[,ntype:=.N,by=list( cusip_id, trd_exctn_dt, trd_exctn_tm, msg_seq_nb)]

# merge back         
pre_w_comb=unique(pre_w_t[,list(cusip_id,trd_exctn_dt,trd_exctn_tm,msg_seq_nb,ntype,flag)
                        ][pre_w_n,
                          on=list(cusip_id,trd_exctn_dt,trd_exctn_tm,msg_seq_nb),nomatch=NULL])


# Map back by matching CUSIP Execution Date and Time to remove msg_seq_nb that appears more than once;
# If napp=1 or (napp>1 but ntype=1);
pre_w_keep=unique(pre_w_comb[napp==1|(napp>1&ntype==1)
][pre_w_tot[,list(cusip_id,trd_exctn_dt,trd_exctn_tm,msg_seq_nb,flag)],
  on=list(cusip_id,trd_exctn_dt,trd_exctn_tm,msg_seq_nb),nomatch=NULL])

pre_w_keep[,npair:=.N/2,by=list( cusip_id, trd_exctn_dt, trd_exctn_tm)]

# For records with only one pair of entry at a given time stamp - transpose using the flag information;
pre_w_keep_1=dcast(pre_w_keep[npair==1],cusip_id+trd_exctn_dt+trd_exctn_tm~flag,value.var="msg_seq_nb")
setnames(pre_w_keep_1,"msg","msg_seq_nb")
setnames(pre_w_keep_1,"omsg","orig_msg_seq_nb")

# For records with more than one pair of entry at a given time stamp - join back the original msg_seq_nb;
pre_w_keep_2=unique(pre_w_keep[(napp>1&flag=="msg"),list(cusip_id,trd_exctn_dt,trd_exctn_tm,msg_seq_nb)
][pre_W[,list(cusip_id,trd_exctn_dt,trd_exctn_tm,msg_seq_nb,orig_msg_seq_nb)],
  on=list(cusip_id,trd_exctn_dt,trd_exctn_tm,msg_seq_nb),nomatch=NULL])

# join together 
pre_w_keep=rbind(pre_w_keep_1,pre_w_keep_2)
rm(pre_w_keep_1,pre_w_keep_2)

# Join back to get all the other information;
pre_w_keep=unique(pre_w_keep[pre_W[,orig_msg_seq_nb:=NULL],
                             on=list(cusip_id,trd_exctn_dt,trd_exctn_tm,msg_seq_nb),nomatch=NULL])

# Match up with Trade Record data to delete the matched T record */;
#  Matching by Cusip_ID, Date, and MSG_SEQ_NB;
#  W records show ORIG_MSG_SEQ_NB matching orignal record MSG_SEQ_NB;

setnames(pre_w_keep,"msg_seq_nb","mod_msg_seq_nb")
setnames(pre_w_keep,"orig_msg_seq_nb","mod_orig_msg_seq_nb")
setnames(pre_w_keep,"trc_st","trc_st_w")

pre_T=unique(pre_w_keep[,list(cusip_id,trd_exctn_dt,mod_msg_seq_nb,mod_orig_msg_seq_nb,trc_st_w)][pre_T,
                                                                                                  on=list(cusip_id,trd_exctn_dt,mod_orig_msg_seq_nb=msg_seq_nb),])
# keep records
del_w=pre_T[trc_st_w=="W"]
# delete matched Trades records
setnames(pre_T,"mod_orig_msg_seq_nb","msg_seq_nb")
pre_T=pre_T[trc_st_w!="W"|is.na(trc_st_w)][,c("mod_msg_seq_nb","trc_st_w"):=NULL]

#  Replace T records with corresponding W records;
# Filter out W records with valid matching T from the previous step
setnames(pre_w_keep,"trc_st_w","trc_st")
rep_w=unique(del_w[,list(cusip_id,trd_exctn_dt,mod_msg_seq_nb,trc_st_w)
][pre_w_keep,on=list(cusip_id,trd_exctn_dt,mod_msg_seq_nb),])
setnames(rep_w,"mod_msg_seq_nb","msg_seq_nb")
setnames(rep_w,"mod_orig_msg_seq_nb","orig_msg_seq_nb")

rep_w=unique(rep_w[trc_st_w=="W"|is.na(trc_st_w)][,c("trc_st_w"):=NULL])
pre_T=rbind(pre_T,rep_w)
# house cleaning
rm(pre_W,pre_w_comb,pre_w_keep,pre_w_n,pre_w_t,pre_w_tot,rep_w,del_w)

### correct reversals
# R (reversal) D (Delayed dissemination) and X (delayed reversal)
pre_rev=pre_T[asof_cd=="R",list(cusip_id,bond_sym_id,trd_exctn_dt,trd_exctn_tm,
                                trd_rpt_dt,trd_rpt_tm,entrd_vol_qt,rptd_pr,rpt_side_cd,cntra_mp_id)]

# Match by only 6 keys: CUSIP_ID, Execution Date, Vol, Price, B/S and C/D
pre_rev=pre_rev[order(cusip_id,bond_sym_id,trd_exctn_dt,entrd_vol_qt,rptd_pr,
                      rpt_side_cd,cntra_mp_id,trd_exctn_tm,trd_rpt_dt,trd_rpt_tm)]
# create ordering
pre_rev[,seq:=1:.N,by=list(cusip_id,bond_sym_id,trd_exctn_dt,entrd_vol_qt,rptd_pr,rpt_side_cd,cntra_mp_id)]

# Create the same ordering among the non-reversal records;
# Remove records that are R (reversal) D (Delayed dissemination) and X (delayed reversal);
pre_T1=pre_T[!asof_cd%in%c("R","X","D"),list(cusip_id,bond_sym_id,trd_exctn_dt,
                                             trd_exctn_tm,entrd_vol_qt,rptd_pr,rpt_side_cd,cntra_mp_id,
                                             trd_rpt_dt,trd_rpt_tm,msg_seq_nb)]

# Match by only 6 keys: CUSIP_ID, Execution Date, Vol, Price, B/S and C/D
pre_T1=pre_T1[order(cusip_id,bond_sym_id,trd_exctn_dt,entrd_vol_qt,rptd_pr,
                    rpt_side_cd,cntra_mp_id,trd_exctn_tm,trd_rpt_dt,trd_rpt_tm)]

pre_T1[,seq6:=1:.N,by=list(cusip_id,bond_sym_id,trd_exctn_dt,entrd_vol_qt,rptd_pr,rpt_side_cd,cntra_mp_id)]

pre_rev[,rev_seq6:=seq]

pre_T1=pre_rev[,list(cusip_id,trd_exctn_dt,entrd_vol_qt,rptd_pr,rpt_side_cd,cntra_mp_id,seq,rev_seq6)
][pre_T1,on=list(cusip_id,trd_exctn_dt,entrd_vol_qt,
                 rptd_pr,rpt_side_cd,cntra_mp_id,seq=seq6)]
# reversal records matched (94%)
pre_T1[!is.na(rev_seq6)][,.N]/pre_rev[,.N]

pre_T1=pre_T1[is.na(rev_seq6)][,rev_seq6:=NULL]


pre_T=pre_T[pre_T1[,list(cusip_id,trd_exctn_dt,trd_exctn_tm,entrd_vol_qt,rptd_pr,
                         rpt_side_cd,cntra_mp_id,msg_seq_nb,trd_rpt_dt,trd_rpt_tm)
],on=list(cusip_id,trd_exctn_dt,trd_exctn_tm,entrd_vol_qt,
          rptd_pr,rpt_side_cd,cntra_mp_id,msg_seq_nb,trd_rpt_dt,trd_rpt_tm),nomatch=NULL]

# merge all together
trace=rbind(post_TR,pre_T)

# house clean
rm(pre_T1,pre_rev,post_TR,pre_T)

##
## Clean Agency trades  ----
##

# Match B and S records by using CUSIP_ID, Trd_exctn_dt, price, vol;
#  Do not match by trd_exctn_tm as it is self-reported and hence is less error free;
# using fintersect so duplicates are removed
trace_d_buys_unmatched=fintersect(
  trace[rpt_side_cd == 'B' & cntra_mp_id == 'D'][,list(cusip_id,entrd_vol_qt,rptd_pr,trd_exctn_dt)],
  trace[rpt_side_cd == 'S' & cntra_mp_id == 'D'][,list(cusip_id,entrd_vol_qt,rptd_pr,trd_exctn_dt)])

# remove trades that exist in both datasets
trace_d_buys_unmatched=trace[rpt_side_cd == 'B' & cntra_mp_id == 'D'
][!trace_d_buys_unmatched,
  on=list(cusip_id,entrd_vol_qt,rptd_pr,trd_exctn_dt)]

# bind together (Difference with DN14 code here: keep all trades also not D/C)
trace=rbind(trace[cntra_mp_id!="D"],
            trace[rpt_side_cd == 'S' & cntra_mp_id == 'D'],
            trace_d_buys_unmatched)

trace=trace[,list(cusip_id,trd_exctn_dt,trd_exctn_tm,trc_st,wis_fl,
                  cmsn_trd,entrd_vol_qt,rptd_pr,days_to_sttl_ct,
                  rpt_side_cd,buy_cpcty_cd,sell_cpcty_cd,cntra_mp_id,
                  spcl_trd_fl,trdg_mkt_cd,lckd_in_ind,rptg_party_type,
                  stlmnt_dt,yld_sign_cd,yld_pt)]
rm(trace_d_buys_unmatched)


# Remove agency trades without commission
# # create agency dummy
# trace[,agency:=fifelse(rpt_side_cd=='B',buy_cpcty_cd,sell_cpcty_cd)]
# 
# # delete agency without commission
# trace=trace[!trace[agency=="A"&cntra_mp_id == "C"& cmsn_trd == "N"],
#          on = .(cusip_id,entrd_vol_qt,rptd_pr,trd_exctn_dt,
#                 trd_exctn_tm,rpt_side_cd,cntra_mp_id,agency,cmsn_trd)]

# trace$trd_exctn_ts=paste(trace$trd_exctn_dt, trace$trd_exctn_tm)
# trace$trd_exctn_ts=as.POSIXct(trace$trd_exctn_ts, format="%Y-%m-%d %H:%M:%S")


# save file
save(trace,file="Data/temp/trace_clean.rdata")


##
## Median price and reversal filter  ----
##

load("Data/temp/trace_clean.rdata")

trace[,.N]
# set dates

## remove if settlement is more than 2 days after transaction
# trace[,stlmnt_dt:=ymd(as.character(stlmnt_dt))]
# trace[,diff := bizdays(trd_exctn_dt, stlmnt_dt, cal = "Rmetrics/NYSE")]
# trace[,diff := fcoalesce(diff-1,days_to_sttl_ct)]
# summary(trace$diff)
# trace=trace[diff>=0&diff<=2]
# trace=trace[,-c("diff","days_to_sttl_ct","stlmnt_dt","wis_fl","lckd_in_ind","spcl_trd_fl")]

# reversal filter
# get dates
trace[,trd_exctn_dt:=as.Date(trd_exctn_dt)]

# trace[,trd_exctn_dt:=as.Date(as.character(trd_exctn_dt),format="%Y%m%d")]
# get price changes (lead and lag for all cusip and also median daily price)
trace[order(trd_exctn_dt,trd_exctn_tm),price_change:=(rptd_pr/shift(rptd_pr))-1,by="cusip_id"
][order(trd_exctn_dt,trd_exctn_tm),":="(price_change_lag=shift(price_change,n=1),
                                        price_change_lead=shift(price_change,n=-1)),
  by="cusip_id"]

# The reversal filter eliminates any transaction with
# an absolute price change deviating from the lead,
# lag and average lead/lag price change by at least 10%.
trace[,remove:=fifelse(abs(price_change)>cutoff&abs(abs((1+price_change)*(1+price_change_lead)-1))<cutoff,1,0)
][,remove:=fifelse(is.na(remove),0,remove)]

# remove absolute price deviation larger than 10% of median price of 9d windows
# or larger than 10% of median daily price 
# or an absolute price change deviating from the lead, lag and average lead/lag price change by at least 10%.
trace=trace[remove==0]


# The median filter eliminates any transaction where the price deviates by more than 10% from the daily median
# or from a nine-trading-day median centered at the trading day.
# median price calculation x each bond-day


# # create a range of 9 trading days by vreating start and end date of the 9days window for each trade
trace1=unique(trace[,c("cusip_id","trd_exctn_dt")],by=c("cusip_id","trd_exctn_dt"))
trace1[,c("start","end"):=.(offset(trd_exctn_dt,-4,"Rmetrics/NYSE"),
                            offset(trd_exctn_dt,+4,"Rmetrics/NYSE"))]

# merge back the file adding all transactions of the same bond in the 9day window
# and compute median price
setkey(trace1,"cusip_id","trd_exctn_dt")
setkey(trace,"cusip_id","trd_exctn_dt")
# 
trace1=trace1[trace[,list(cusip_id,trd_exctn_dt,rptd_pr)],on=.(cusip_id,
                                                               start<=trd_exctn_dt,
                                                               end>=trd_exctn_dt)
][ ,.(median_9=stats::median(rptd_pr,na.rm=T)),by=c("cusip_id","trd_exctn_dt")]

# get daily median
trace[,median_price:=stats::median(rptd_pr,na.rm=T),by=c("cusip_id","trd_exctn_dt")]

# merge back median prices
trace=merge(trace,trace1,by=c("cusip_id","trd_exctn_dt"),all.x=T)

# remove absolute price deviation larger than 10% of median price of 9d windows
# or larger than 10% of median daily price 
# or an absolute price change deviating from the lead, lag and average lead/lag price change by at least 10%.
trace=trace[abs((rptd_pr/median_9)-1)<=cutoff
][abs((rptd_pr/median_price)-1)<=cutoff]
# house cleaning
gc()

trace[,.N]
save(trace,file="Data/temp/trace_clean_filtered.rdata")
# both filters removed 949787 trades

##
## Compute daily price, volume and transaction numbers  ----
##

# load("Data/temp/trace_clean_filtered.rdata")

# remove when issued and lock in and special trades and primary trades and missing prices
trace=trace[wis_fl!="Y"&spcl_trd_fl!="Y"&trdg_mkt_cd!="P1"&!is.na(rptd_pr)]

# clean price calculation x each bond-day
trace1=trace[,.(clean_price=sum(rptd_pr*entrd_vol_qt/sum(entrd_vol_qt)),
                volume=sum(entrd_vol_qt),
                transaction_number=.N)
             ,by=c("cusip_id","trd_exctn_dt")]

# set dates
fwrite(trace1,file="Data/temp/trace_daily_clean.csv")
rm(list=ls())
