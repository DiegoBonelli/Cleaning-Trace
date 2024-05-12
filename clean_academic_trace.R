
####
####
#  R Code to load and clean TRACE Academic
#   
# Author: Diego Bonelli & Katsiaryna Falkovich
# Date: 26/01/2024
# Preprocessing: 
#               
# Notes: 
#     1) 
#
####
####

rm(list=ls())

# set wd and load functions
# trace files are in Data/Academic Trace file path
setwd("")

# Load Libraries
suppressWarnings(suppressMessages({
  require(lubridate, quietly = T) # Useful package for dates
  require(zoo, quietly = T) # Useful package for dates (yearmon format)
  require(dplyr, quietly = T) # usefull for ntile function
  require(data.table, quietly = T) # data.table
  require(haven, quietly = T) # read sas files
  require(bizdays, quietly = T) # package for business days
  library(stringr) # needed for time in strings
  
}))

# cutoff for reversal and median price filter
cutoff=0.1
# load calendars
load_rmetrics_calendars(1970:2030)

##
## load trace  ----
##

# read trades pre 2012 change
read_pre=function(){
  x=seq(2002,2011,1)
  trace=NULL
  for (i in x) {
    files=list.files(paste0("Data/Academic Trace/",i))
    trades=grep("0044-corp-academic",files,  ignore.case=T, value=T)
    dt<- rbindlist(lapply(paste(paste0("Data/Academic Trace/",i),trades,sep="/"),  
                          function(x) fread(x, sep='|', header=T,
                                            stringsAsFactors=F,
                                            colClasses = list("character"=c("CUSIP_ID","TRD_RPT_TM","EXCTN_TM"))
                          )),fill=T ) 
    names(dt)=tolower(names(dt))
    trace=rbind(trace,dt)}
  # consider also days in 2012 before feb 06
  files=list.files(paste0("Data/Academic Trace/",2012))
  trades=data.table()
  trades$name=grep("0044-corp-academic",files,  ignore.case=T, value=T)
  trades$date=as.Date(gsub("0044-corp-academic-trace-data-|.txt","",trades$name))
  trades=trades[date<"2012-02-06"]
  dt<- rbindlist(lapply(paste(paste0("Data/Academic Trace/",2012),trades$name,sep="/"),  
                        function(x) cbind(fread(x, sep='|', header=T, stringsAsFactors=F,
                                                colClasses = list("character"=c("CUSIP_ID","TRD_RPT_TM","EXCTN_TM"))))),fill=T ) 
  names(dt)=tolower(names(dt))
  trace=rbind(trace,dt) 
  return(trace)}
# read trades post 2012 change
read_post=function(max_year){
  trace=NULL
  # consider also days in 2012 after feb 06
  files=list.files(paste0("Data/Academic Trace/",2012))
  trades=data.table()
  trades$name=grep("0044-corp-academic",files,  ignore.case=T, value=T)
  trades$date=as.Date(gsub("0044-corp-academic-trace-data-|.txt","",trades$name))
  trades=trades[date>="2012-02-06"]
  trace<- rbindlist(lapply(paste(paste0("Data/Academic Trace/",2012),trades$name,sep="/"),  
                           function(x) fread(x, sep='|', header=T,stringsAsFactors=F,
                                             colClasses = list("character"=c("CUSIP_ID","TRD_RPT_TM","EXCTN_TM"))
                           )),fill=T ) 
  names(trace)=tolower(names(trace))
  
  x=seq(2013,max_year,1)
  for (i in x) {
    files=list.files(paste0("Data/Academic Trace/",i))
    trades=grep("0044-corp-academic",files,  ignore.case=T, value=T)
    dt<- rbindlist(lapply(paste(paste0("Data/Academic Trace/",i),trades,sep="/"),
                          function(x) cbind(fread(x, sep='|', header=T,
                                                  stringsAsFactors=F,
                                                  colClasses = list("character"=c("CUSIP_ID","TRD_RPT_TM","EXCTN_TM"))
                          ))),fill=T )
    names(dt)=tolower(names(dt))
    trace=rbind(trace,dt)}
  return(trace)}

# read bond file and supplemental files
read_bond_file=function(year){
  files=list.files(paste0("Data/Academic Trace/",year))
  trades=data.table()
  trades$name=grep("0044-corp-bond",files,  ignore.case=T, value=T)
  trades$date=as.Date(gsub("0044-corp-bond-|.txt","",trades$name))
  
  data <- lapply(paste(paste0("Data/Academic Trace/",year),trades$name,sep="/"),  
                 function(x) fread(x, sep='|', header=T,
                                   stringsAsFactors=F))
  for (i in 1:dim(trades)[1]) {data[[i]]$date=trades$date[i]}
  dt <- rbindlist(data,fill=T)
  names(dt)=tolower(names(dt))
  return(dt)}

# read and save as csv
trace=read_pre()
fwrite(trace,file="Data/Academic Trace/trace_pre_2012.csv")
trace=read_post(max_year=2021)
fwrite(trace,file="Data/Academic Trace/trace_post_2012.csv")
for (i in 2002:2021) {
  print(i)
  trace_names=read_bond_file(i)
  fwrite(trace_names,file=paste0("Data/Academic Trace/names/trace_names_",i,".csv"))
}
rm(trace,trace_names)
##
## Clean Post 2012  ----
##

# load post 2012 file
trace_post=fread("Data/Academic Trace/trace_post_2012.csv")

dim(trace_post)
# unify contra party
trace_post[,rptg_party_id:=fifelse(rptg_party_gvp_id!="",rptg_party_gvp_id,rptg_party_id)
][,cntra_party_id:=fifelse(cntra_party_gvp_id!="",cntra_party_gvp_id,cntra_party_id)]

# filter after 6 feb 2012 and create dt for trades,cancellations and reversals
post_TR=trace_post[cusip_id!=""&trd_st_cd%in%c("T","R")&asof_cd!="R"]
post_XC=trace_post[cusip_id!=""&trd_st_cd%in% c("X", "C")]
post_Y=trace_post[cusip_id!=""&trd_exctn_dt>= "2012-02-06"&(trd_st_cd%in%c("Y")|asof_cd=="R")]

# reversals referring to other file
post_Y_pre=trace_post[cusip_id!=""&trd_exctn_dt< "2012-02-06"&(trd_st_cd%in%c("Y")|asof_cd=="R")]

# Take out all cancellations and corrections;
# These transactions should be deleted together with the original report;
# These transactions can be matched by message sequence number and date.

# First remove cancellations and corrections from reversals
post_Y=post_Y[!post_XC[,.(cusip_id,
                          trd_exctn_dt,
                          systm_cntrl_nb)], 
              on=list(cusip_id,
                      trd_exctn_dt,
                      systm_cntrl_nb)]

# also from reversals pre 2012
post_Y_pre=post_Y_pre[!post_XC[,.(cusip_id,
                                  trd_exctn_dt,
                                  systm_cntrl_nb)], 
                      on=list(cusip_id,
                              trd_exctn_dt,
                              systm_cntrl_nb)]

# Delete the cancellations and corrections found, i.e. remove all trades from the data.table,
# which are included in post_XC
post_TR=post_TR[!post_XC[,.(cusip_id,
                            trd_exctn_dt,
                            systm_cntrl_nb)], 
                on=list(cusip_id,
                        trd_exctn_dt,
                        systm_cntrl_nb)]

# Now also remove Reversals
post_TR <- post_TR[!post_Y[, .(cusip_id,
                               trd_exctn_dt,
                               systm_cntrl_nb)],,
                   on = list(cusip_id,
                             trd_exctn_dt,
                             systm_cntrl_nb)]

rm(post_Y,post_XC,trace_post)

# Before filtering the data 176752618                
# after filtering 168809697, which means 7942921 rows are removed

# unify column names
post_TR=post_TR[,-c("trd_mdfr_sro_cd")]
setnames(post_TR,"trd_st_cd","trc_st",skip_absent=T)
setnames(post_TR,"issue_sym_id","bond_sym_id",skip_absent=T)
setnames(post_TR,"wis_dstrd_cd","wis_cd",skip_absent=T)
setnames(post_TR,"no_rmnrn_cd","cmsn_trd_fl",skip_absent=T)
setnames(post_TR,"yld_drctn_cd","yld_sign_cd",skip_absent=T)
setnames(post_TR,"calcd_yld_pt","yld_pt",skip_absent=T)
setnames(post_TR,"exctn_tm","trd_exctn_tm",skip_absent=T)
setnames(post_TR,"trd_mdfr_late_cd","sale_cndtn_cd",skip_absent=T)
setnames(post_TR,"buyer_cmsn_amt","buy_cmsn_rt",skip_absent=T)
setnames(post_TR,"buyer_cpcty_cd","buy_cpcty_cd",skip_absent=T)
setnames(post_TR,"sllr_cmsn_amt","sell_cmsn_rt",skip_absent=T)
setnames(post_TR,"sllr_cpcty_cd","sell_cpcty_cd",skip_absent=T)
setnames(post_TR,"pblsh_fl","dissem_fl",skip_absent=T)

# house cleaning
gc()

# save temp file
# save.image(file="Data/Academic Trace/temp.rdata")

##
## Clean Pre 2012  ----
##
# 
# load("Data/Academic Trace/temp.rdata")
# load pre 2012 file
trace_pre=fread("Data/Academic Trace/trace_pre_2012.csv")

# unify columns and contra party
trace_pre=trace_pre[,sale_cndtn_cd:=fcoalesce(sale_cndtn_cd,sale_cndtn2_cd)
][,-c("sale_cndtn2_cd")
][,rptg_mkt_mp_id:=fifelse(rptg_side_gvp_mp_id!="",rptg_side_gvp_mp_id,rptg_mkt_mp_id)
][,cntra_mp_id:=fifelse(cntra_gvp_id!="",cntra_gvp_id,cntra_mp_id)]

# filter before 6 feb 2012 and create dt for trades,cancellations and reversals
dim(trace_pre)[1]
# 98711223

# keep only trades and trade corrections which are not reversals
# in this version of the file C and N are reports that are cancelled or reversed 
# hence they should be removed altogether
pre_T=trace_pre[cusip_id!=""&!trc_st%in%c("C","N")&asof_cd!="R"]

dim(pre_T)[1]
# 94598624

# Select reversals
# asof_cd==R (reversal)
pre_rev=trace_pre[!trc_st%in%c("C","N")&asof_cd=="R",list(rec_ct_nb,cusip_id,bond_sym_id,trd_exctn_dt,exctn_tm,
                                                          trd_rpt_dt,trd_rpt_tm,entrd_vol_qt,rptd_pr,
                                                          rpt_side_cd,cntra_mp_id,rptg_mkt_mp_id)]

# Include reversals referring to transactions before February 6th, 2012 that are reported after this date;
# adjust names
setnames(post_Y_pre,"issue_sym_id","bond_sym_id",skip_absent=T)
setnames(post_Y_pre,"trd_exctn_tm","exctn_tm",skip_absent=T)
setnames(post_Y_pre,"cntra_party_id","cntra_mp_id",skip_absent=T)
setnames(post_Y_pre,"rptg_party_id","rptg_mkt_mp_id",skip_absent=T)
# select columns and bind together
post_Y_pre=post_Y_pre[,list(rec_ct_nb,cusip_id,bond_sym_id,trd_exctn_dt,exctn_tm,
                            trd_rpt_dt,trd_rpt_tm,entrd_vol_qt,rptd_pr,
                            rpt_side_cd,cntra_mp_id,rptg_mkt_mp_id)]
pre_rev=rbind(pre_rev,post_Y_pre)

# remove duplicates
dim(pre_rev)[1]
# 1236280
pre_rev=unique(pre_rev,by=c('trd_exctn_dt','cusip_id','exctn_tm','rptd_pr',
                            'entrd_vol_qt','rpt_side_cd','cntra_mp_id',
                            'trd_rpt_dt','trd_rpt_tm','rec_ct_nb','rptg_mkt_mp_id'))

# get reporting time in seconds
pre_rev[,trd_rpt_tm:=str_pad(trd_rpt_tm, 6, pad = "0")
][,trd_rpt_ts:=paste(trd_rpt_dt, trd_rpt_tm)
][,trd_rpt_ts:=as.POSIXct(trd_rpt_ts, format="%Y%m%d%H%M%S")]

# create ordering (such that seq is the number of reversals referring to the same trade)
# in case one reversals refer to the original trade and the following refer to the new trades (with code asof_cd=="A")
pre_rev[order(cusip_id,trd_exctn_dt,rptd_pr,entrd_vol_qt,
              rpt_side_cd,cntra_mp_id,rptg_mkt_mp_id,trd_rpt_ts,exctn_tm),
        seq:=1:.N,by=list(cusip_id,trd_exctn_dt,rptd_pr,entrd_vol_qt,
                          rpt_side_cd,cntra_mp_id,rptg_mkt_mp_id)]


# Create the same ordering among the non-reversal records;
pre_T1=copy(pre_T)[,trd_rpt_tm:=str_pad(trd_rpt_tm, 6, pad = "0")
][,trd_rpt_ts:=paste(trd_rpt_dt, trd_rpt_tm)
][,trd_rpt_ts:=as.POSIXct(trd_rpt_ts, format="%Y%m%d%H%M%S")]

# Match by only 7 keys
pre_T1[order(cusip_id,trd_exctn_dt,rptd_pr,entrd_vol_qt,
             rpt_side_cd,cntra_mp_id,rptg_mkt_mp_id,trd_rpt_ts,exctn_tm),
       seq:=1:.N,by=list(cusip_id,trd_exctn_dt,rptd_pr,entrd_vol_qt,
                         rpt_side_cd,cntra_mp_id,rptg_mkt_mp_id)]

# Match by only 7 keys
pre_T1=pre_T1[pre_rev[,list(cusip_id,trd_exctn_dt,exctn_tm,rptd_pr,
                            entrd_vol_qt,rpt_side_cd,cntra_mp_id,seq,
                            trd_rpt_ts,rptg_mkt_mp_id)],
              on=list(cusip_id,trd_exctn_dt,rptd_pr,entrd_vol_qt,
                      rpt_side_cd,cntra_mp_id,seq),nomatch=NULL]

# reversals have to be reported after the trade
pre_T1=pre_T1[i.trd_rpt_ts>=trd_rpt_ts]
pre_T1=unique(pre_T1,by=c("rec_ct_nb","trd_exctn_dt","cusip_id"))

# reversal records matched (96%)
pre_T1[,.N]/pre_rev[,.N]

# reversed trades
dim(pre_T1)[1]
# 1187383
dim(pre_T)[1]
# 94598624
# anti_join (remove from full file the trades that are reversed)
pre_T=pre_T[!pre_T1[,list(cusip_id,trd_exctn_dt,entrd_vol_qt,rptd_pr,
                          rpt_side_cd,cntra_mp_id,rec_ct_nb,trd_rpt_dt)
],on=list(cusip_id,trd_exctn_dt,entrd_vol_qt,rptd_pr,
          rpt_side_cd,cntra_mp_id,rec_ct_nb,trd_rpt_dt)]

# unify some of the column names
setnames(pre_T,"rptg_mkt_mp_id","rptg_party_id",skip_absent=T)
setnames(pre_T,"rptg_side_gvp_mp_id","rptg_party_gvp_id",skip_absent=T)
setnames(pre_T,"scrty_type_cd","prdct_sbtp_cd",skip_absent=T)
setnames(pre_T,"cntra_mp_id","cntra_party_id",skip_absent=T)
setnames(pre_T,"cntra_gvp_id","cntra_party_gvp_id",skip_absent=T)
setnames(pre_T,"agu_trd_id","lckd_in_fl",skip_absent=T)
setnames(pre_T,"exctn_tm","trd_exctn_tm",skip_absent=T)


# remove columns
post_TR=post_TR[,-c("systm_cntrl_dt","systm_cntrl_nb","prev_trd_cntrl_dt","prev_trd_cntrl_nb","first_trd_cntrl_dt","first_trd_cntrl_nb")]
pre_T=pre_T[,-c("prev_rec_ct_nb")]

# merge all together
trace=rbind(post_TR,pre_T,fill=T)

# house clean
rm(pre_T1,pre_rev,post_TR,pre_T,post_Y_pre,trace_pre)

save.image(file="Data/Academic Trace/temp2.rdata")
gc()

##
## Clean Locked-In trades and remove wash trades ----
##

# remove wash trades (dealer trading with himself)

#look at some examples
# wash_trades=trace[rptg_party_id==cntra_party_id]
# remove them
trace=trace[rptg_party_id!=cntra_party_id]

# identify lock-in trades and create a duplicate with inverted parties and buy/sell indicator.
lckd_trades=trace[lckd_in_fl%in%c("Y","G","Q")] 
# invert ids
lckd_trades=lckd_trades[,":="(rptg_party_id2=cntra_party_id,
                              cntra_party_id=rptg_party_id,
                              rpt_side_cd=fifelse(rpt_side_cd=="B","S","B"))
][,rptg_party_id:=rptg_party_id2
][,-"rptg_party_id2"]
# bind together
trace=rbind(trace,lckd_trades)
rm(lckd_trades)

# save file
save(trace,file="Data/Academic Trace/trace_transactions.rdata")
gc()

##
## Clean Interdealer trades and Agency trades  ----
##

trace[,.N]
#267788061

# from here on, all the cleaning is only for prices not for transactions
# load("Data/Academic Trace/trace_transactions.rdata")

# create one file with only dealer buy trades
trace_DB=trace[rpt_side_cd == 'B' & !cntra_party_id%in%c('C','A')]
# order and create a seq number to avoid multiple matches
trace_DB[order(cusip_id,trd_exctn_dt,rptd_pr,entrd_vol_qt,
               cntra_party_id,rptg_party_id,trd_exctn_tm),
         seq:=1:.N,by=list(cusip_id,trd_exctn_dt,rptd_pr,entrd_vol_qt,
                           cntra_party_id,rptg_party_id)]

# create one file with only dealer sell trades
trace_DS=trace[rpt_side_cd == 'S' & !cntra_party_id%in%c('C','A')]
# order and create a seq number to avoid multiple matches
trace_DS[order(cusip_id,trd_exctn_dt,rptd_pr,entrd_vol_qt
               ,cntra_party_id,rptg_party_id,trd_exctn_tm),
         seq:=1:.N,by=list(cusip_id,trd_exctn_dt,rptd_pr,entrd_vol_qt,
                           cntra_party_id,rptg_party_id)]

# get only unmatched sell trades
trace_s_unmatched=trace_DS[!trace_DB,on=list(cusip_id,entrd_vol_qt,rptd_pr,
                                             trd_exctn_dt,seq,
                                             rptg_party_id=cntra_party_id,
                                             cntra_party_id=rptg_party_id)]
# bind together 
trace=rbind(trace[cntra_party_id%in%c("C","A")],
            trace_DB[,-"seq"],
            trace_s_unmatched[,-"seq"])

rm(trace_DB,trace_s_unmatched,trace_DS)

# house cleaning
gc()


# Remove agency trades without commission
# # create agency dummy
# trace[,agency:=fifelse(rpt_side_cd=='B',buy_cpcty_cd,sell_cpcty_cd)]
# 
# # delete agency without commission
# trace=trace[!trace[agency=="A"&cntra_party_id == "C"& cmsn_trd == "N"],
#          on = .(cusip_id,entrd_vol_qt,rptd_pr,trd_exctn_dt,
#                 trd_exctn_tm,rpt_side_cd,cntra_party_id,agency,cmsn_trd)]

# trace$trd_exctn_ts=paste(trace$trd_exctn_dt, trace$trd_exctn_tm)
# trace$trd_exctn_ts=as.POSIXct(trace$trd_exctn_ts, format="%Y-%m-%d %H:%M:%S")


##
## Median price and reversal filter  ----
##

trace[,.N]
#198674702
fwrite(trace,file="Data/Academic Trace/trace_clean_filtered.csv")

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
trace[,trd_exctn_dt:=as.Date(as.character(trd_exctn_dt),format="%Y%m%d")]
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


# # create a range of 9 trading days by creating start and end date of the 9days window for each trade
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
# 197666547
# median filter removed 714258 trades

##
## Compute daily price, volume and transaction numbers  ----
##

# create a daily summary file for different samples
# 1. complete file
# 2. remove trades lower than 10k and primary and special trades
# 3. remove trades lower than 100k and primary and special trades
# 4. remove trades lower than 10k and primary and special trades and keep only customer trades
# 5. remove trades lower than 10k and primary and special trades and keep only sell trades
# 6. remove trades lower than 10k and primary and special trades and keep only buys trades
# 7. remove trades lower than 10k and primary and special trades and keep only customer trades sell trades
# 8. remove trades lower than 10k and primary and special trades and keep only customer trades buys trades

# clean price calculation x each bond-day
trace1=trace[,.(clean_price=sum(rptd_pr*entrd_vol_qt/sum(entrd_vol_qt)),
                volume=sum(entrd_vol_qt),
                transaction_number=.N)
             ,by=c("cusip_id","trd_exctn_dt")]

# set dates
fwrite(trace1,file="Data/Academic Trace/trace_daily_complete.csv")

# remove when issued and special trades and primary trades and missing prices
trace=trace[wis_cd!="Y"&spcl_pr_fl!="Y"&trdg_mkt_cd!="P1"&!is.na(rptd_pr)]

# remove trades lower than 10k
trace=trace[(entrd_vol_qt*rptd_pr/100)>=10000]


# clean price calculation x each bond-day
trace1=trace[,.(clean_price=sum(rptd_pr*entrd_vol_qt/sum(entrd_vol_qt)),
                volume=sum(entrd_vol_qt),
                transaction_number=.N)
             ,by=c("cusip_id","trd_exctn_dt")]

# set dates
fwrite(trace1,file="Data/Academic Trace/trace_daily_clean.csv")


# remove trades smaller than 100.000$
# clean price calculation x each bond-day
trace1=trace[(entrd_vol_qt*rptd_pr/100)>=100000][!is.na(rptd_pr)][,.(clean_price=sum(rptd_pr*entrd_vol_qt/sum(entrd_vol_qt)),
                                                                     volume=sum(entrd_vol_qt),
                                                                     transaction_number=.N)
                                                                  ,by=c("cusip_id","trd_exctn_dt")]

# set dates
fwrite(trace1,file="Data/Academic Trace/trace_daily_clean_100k.csv")



# remove dealer trades
# clean price calculation x each bond-day
trace1=trace[cntra_party_id == 'C'
][!is.na(rptd_pr)][,.(clean_price=sum(rptd_pr*entrd_vol_qt/sum(entrd_vol_qt)),
                      volume=sum(entrd_vol_qt),
                      transaction_number=.N)
                   ,by=c("cusip_id","trd_exctn_dt")]


# set dates
fwrite(trace1,file="Data/Academic Trace/trace_daily_clean_customers.csv")

# clean price calculation x each bond-day
trace1=trace[rpt_side_cd == 'S'
][!is.na(rptd_pr)][,.(clean_price=sum(rptd_pr*entrd_vol_qt/sum(entrd_vol_qt)),
                      volume=sum(entrd_vol_qt),
                      transaction_number=.N)
                   ,by=c("cusip_id","trd_exctn_dt")]


# set dates
fwrite(trace1,file="Data/Academic Trace/trace_daily_clean_sell.csv")

# clean price calculation x each bond-day
trace1=trace[rpt_side_cd == 'B'
][!is.na(rptd_pr)][,.(clean_price=sum(rptd_pr*entrd_vol_qt/sum(entrd_vol_qt)),
                      volume=sum(entrd_vol_qt),
                      transaction_number=.N)
                   ,by=c("cusip_id","trd_exctn_dt")]

# set dates
fwrite(trace1,file="Data/Academic Trace/trace_daily_clean_buy.csv")


# remove dealer trades
# clean price calculation x each bond-day
trace1=trace[(entrd_vol_qt*rptd_pr/100)>=100000
][rpt_side_cd == 'B'
][cntra_party_id == 'C'
][!is.na(rptd_pr)][,.(clean_price=sum(rptd_pr*entrd_vol_qt/sum(entrd_vol_qt)),
                      volume=sum(entrd_vol_qt),
                      transaction_number=.N)
                   ,by=c("cusip_id","trd_exctn_dt")]


# set dates
fwrite(trace1,file="Data/Academic Trace/trace_daily_clean_sell_customer.csv")

# remove dealer trades
# clean price calculation x each bond-day
trace1=trace[(entrd_vol_qt*rptd_pr/100)>=100000
][rpt_side_cd == 'S'
][cntra_party_id == 'C'
][!is.na(rptd_pr)][,.(clean_price=sum(rptd_pr*entrd_vol_qt/sum(entrd_vol_qt)),
                      volume=sum(entrd_vol_qt),
                      transaction_number=.N)
                   ,by=c("cusip_id","trd_exctn_dt")]

# set dates
fwrite(trace1,file="Data/Academic Trace/trace_daily_clean_buy_customer.csv")

# house cleaning
gc()
