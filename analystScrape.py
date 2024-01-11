import requests
import pandas as pd
from bs4 import BeautifulSoup
import httplib2

import generate_returns_sample_unconditioned_py as returns

def analystCount_current(ticker):
    
    if type(ticker) is not str:
        raise TypeError("Only string inputs are allowed.")
    
    url = "https://finance.yahoo.com/quote/{}/analysis?p={}".format(ticker,ticker)

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
    }

    soup = BeautifulSoup(requests.get(url, headers=headers).content, "html.parser")
    title = str(soup.title)
    
    if title == "<title>Requested symbol wasn't found</title>":
        return None
    
    # Extract all Table Information from Analyst Page and store in dataframe
    analyst_df = pd.DataFrame(columns = ['Info','Current Qtr.','Next Qtr','Current Year','Next Year'])

    non_exist_key = '-'
    for h2 in soup.select("h2"):
        for span in h2.select("span"):
            non_exist_key = str(span.extract())
            non_exist_key = (non_exist_key.split('<')[1].split('>')[1])[:18]

    if non_exist_key == 'Symbols similar to':
        return None
            
    for t in soup.select("table"):
        for tr in t.select("tr:has(td)"):
            for span in tr.select("span"):
                view = str(span.extract())
                view = view.split('<')[1].split('>')[1]
            tds = [td.get_text(strip=True) for td in tr.select("td")]
                
            # Store all relevant info in dataframe
            if view is not None:
                analyst_df = pd.concat([analyst_df,
                                        pd.DataFrame([[view,tds[1],tds[2],tds[3],tds[4]]],
                                                    columns=['Info','Current Qtr.','Next Qtr','Current Year','Next Year'])],
                                                    ignore_index=True)
        
                                        
    if analyst_df.empty:
        return None
    
    return int(analyst_df.at[0,'Current Qtr.'])
