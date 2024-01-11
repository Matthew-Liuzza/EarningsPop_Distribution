import snowflake.connector

import pandas as pd
import datetime as dt
from datetime import timedelta

conn = snowflake.connector.connect(
    user='MATTLIUZZA',
    password='retSEC201066$',
    account='EJQXKSY-ERB08254',
    warehouse='COMPUTE_WH',
    database='SPGLOBALXPRESSCLOUD_SPGLOBALXPRESSCLOUD_AWS_US_WEST_2_XF_PANTILE',
    schema='XPRESSFEED',
    role='SYSADMIN'
)

cur = conn.cursor()

def dateToString(date):
    """Converts 'datetime' object to 'string'."""
    raw_date = str(date).split(' ')
    return raw_date[0]

def stringToDate(date):
    """Converts 'string' to 'datetime' object."""
    raw_date = date.split('-')
    raw_date = dt.datetime( int(raw_date[0]), int(raw_date[1]), int(raw_date[2]) )
    return raw_date

def pandaDateToString(date):
    """Converts 'Pandas datetime' object to 'string'."""
    raw_date = str(date)
    return raw_date.split(' ')[0]

# set market cap and share price to construct sample space
mcap = 2_000_000_000 # billion
pr = 5 # dollar/share

# sample generation key
generate = False

# SQL query -- select companies with set market cap and share price in 2022 from NYSE, NASDAQ, and AMEX
sqlquery = """select date(m.datadate) date,
            m.gvkey,
            tic ticker,
            round(p.prccm * m.cshom,0) mkt_cap
            from sec_mshare m
            join sec_mthprc p on m.gvkey = p.gvkey and m.datadate = p.datadate
            join security s on m.gvkey = s.gvkey and m.iid = s.iid
            where s.exchg in (11,12,14)
            and s.tpci = '0'
            and s.iid = '01'
            and mkt_cap > {}
            and date between '2022-01-01' and '2022-01-31'
            and m.gvkey in
            (select gvkey from sec_dprc where prccd > {})
            order by date""".format(mcap,pr)
            
cur.execute(sqlquery)
tic_and_mktcap_sql = cur.fetchall()
tic_and_mktcap_df = pd.DataFrame(tic_and_mktcap_sql)
tic_and_mktcap_df = tic_and_mktcap_df.drop_duplicates(subset=1).reset_index(drop=True)

# obtain GVKeys of sampled companies
gvkeys = tic_and_mktcap_df[1].drop_duplicates()
gvkeys = gvkeys.reset_index(drop=True)
N_key = len(gvkeys)
N_sample = 0

# construct sample space of returns
returns = []

if generate:
    
    output = """NUMBER OF COMPANIES: {} """.format(N_key)
    print(output)

    # loop through all selected GVKeys
    for i in range(N_key):
    
        key_i = gvkeys.at[i]
        key_i_int = int(key_i)
    
        # SQL query -- get earnings dates for ith GVKey
        query = """select gvkey,
                rdq as date
                from co_idesind
                where gvkey = {}
                and rdq is not null
                order by date""".format(key_i_int)
    
        cur.execute(query)
        e_dates_sql = cur.fetchall()
        e_dates_df = pd.DataFrame(e_dates_sql)
        e_dates = e_dates_df[e_dates_df.columns[-1]]
    
        N_dates = len(e_dates)
        
        # get first earnings date
        e_date0 = e_dates[0]
        str_e_date0 = pandaDateToString(e_date0)
    
        # get yesterday's date
        yest_date = dt.datetime.now() - timedelta(days=1)
        str_yest_date = dateToString(yest_date)
    
        # SQL query -- get daily prices from first earnings date until now
        query = """select datadate as date,
                prccd / ajexdi price
                from sec_dprc prc
                join security sec on prc.gvkey = sec.gvkey and prc.iid = sec.iid
                where tpci = '0'
                and sec.iid = '01'
                and sec.exchg in (11,12,14)
                and date between '{}' and '{}'
                and prc.gvkey = {}
                order by date
                """.format(str_e_date0,str_yest_date,key_i_int)

        cur.execute(query)
        dates_sql = cur.fetchall()
        dates_df = pd.DataFrame(dates_sql)
    
        # loop through all earnings dates of selected GVKey
        for j in range(N_dates):
        
            e_date_j_pd = e_dates[j]
            e_date_j = stringToDate(pandaDateToString(e_date_j_pd))
        
            # if earnings date has a recorded closing price
            if e_date_j_pd in dates_df[dates_df.columns[0]].values:
            
                # get earnings date index in dataframe
                index = dates_df.index[dates_df[0] == e_date_j].tolist()[0]
            
                # get closing price one business day before earnings
                p_i = dates_df.iat[index-1,1]
            
                # check if share price exists one business day after earnings
                try:
                    # get closing price one business day after earnings
                    p = dates_df.iat[index+1,1]
                except IndexError:
                    continue
        
                # calculate return and append to returns sample space
                ret = ((p - p_i) / p_i) * 100
                returns.append(ret)
            
                # augment sample size
                N_sample = N_sample + 1
            
                # output return % summary of earnings pop
                tick_ij = tic_and_mktcap_df.loc[tic_and_mktcap_df[1] == key_i].iat[0,2]
                output = """SAMPLE: {} | GVKEY: {} | TICKER: {} | RETURN %: {} | EARNINGS DATE: {}""".format(N_sample, key_i, tick_ij, ret,e_date_j)
                print(output)
        
        
cur.close()
conn.close()

# convert returns sample space to csv
#df = pd.DataFrame(returns)
#returns_csv = df.to_csv('returns_unconditioned.csv',index=False)