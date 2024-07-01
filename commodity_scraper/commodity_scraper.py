import pandas as pd
import numpy as np
import datetime as dt
import pdblp
from xbbg import blp 

con = pdblp.BCon(debug=False, port=8194, timeout=5000)
con.start()

basket = {
            "W": ["H", "K", "N", "U", "Z"], 
            "C": ["H", "K", "N", "U", "Z"], 
            "S": ["F", "H", "K", "N", "Q", "U", "X"], 
            "SM": ["F", "H", "K", "N", "Q", "U", "V", "Z"],
            "BO": ["F", "H", "K", "N", "Q", "U", "V", "Z"],
            "ODC": ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"], 
            "ODS": ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]
            }

class AgScraper(object):
    def __init__(self):
        pass

    def get_data(self, curr_month: str, curr_yr: str, target_yr: str, mode: str):
        x = int(curr_yr)
        y = int(target_yr)
        years = list()
        years = list()
        for i in range(y - x + 1):
            years.append(x + i)
        
        global basket
        for year in years:
            year = str(year)
            if (mode == "w"):
                temp = pd.DataFrame(columns=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"])
                for ticker in basket.keys():
                    if len(ticker) == 1:
                        for month in basket[ticker]:
                            df = blp.bds(ticker + " " + month + year[3:] + " " + "Comdty", "OPT_CHAIN")
                            if (df.shape[0] != 0):
                                for security in df.iloc[:, 0]:
                                    opt = blp.bdp(tickers=security, flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], VWAP_Dt='20240101')
                                    opt["commod_code"] = ticker
                                    opt["contract_month"] = opt.index.str[2:3] + year[3:]
                                    opt.set_index("px_settle_last_dt", drop=True, inplace=True)
                                    temp = pd.concat([temp, opt], axis=0, ignore_index=False)
                                    temp.to_csv("Ag.csv", mode=mode)
                            else:
                                opt = blp.bdh(tickers = ticker + " " + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date = curr_yr +"-01-01")
                                try:
                                    opt.columns = opt.columns.droplevel()
                                    opt["commod_code"] = ticker
                                    opt["contract_month"] = month + year[3:]
                                    temp = pd.concat([temp, opt], axis=0, ignore_index=False)
                                    temp.to_csv("Ag.csv", mode=mode)
                                except: 
                                    pass

                    else:
                        for month in basket[ticker]:
                            df = blp.bds(ticker + month + year[3:] + " " + "Comdty", "OPT_CHAIN")
                            if (df.shape[0] != 0):
                                for security in df.iloc[:, 0]:
                                    opt = blp.bdp(tickers=security, flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], VWAP_Dt='20240101')
                                    opt["commod_code"] = ticker
                                    opt["contract_month"] = opt.index.str[2:3] + year[3:]
                                    opt.set_index("px_settle_last_dt", drop=True, inplace=True)
                                    temp = pd.concat([temp, opt], axis=0, ignore_index=False)
                                    temp.to_csv("Ag.csv", mode=mode)
                            else:
                                opt = blp.bdh(tickers = ticker + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date = curr_yr + "-01-01")
                                try:
                                    opt.columns = opt.columns.droplevel()
                                    opt["commod_code"] = ticker
                                    opt["contract_month"] = month + year[3:]
                                    temp = pd.concat([temp, opt], axis=0, ignore_index=False)
                                    temp.to_csv("Ag.csv", mode=mode)
                                except: 
                                    pass
            
            
            
            else: 
                temp = pd.read_csv("Ag.csv", index_col=0)
                for ticker in basket.keys():
                    signal = 0
                    if len(ticker) == 1:
                        for month in basket[ticker]:
                            if month == curr_month:
                                signal = 1
                            
                            if signal == 1:
                                df = blp.bds(ticker + " " + month + year[3:] + " " + "Comdty", "OPT_CHAIN")
                                if (df.shape[0] != 0):
                                    for security in df.iloc[:, 0]:
                                        opt = blp.bdp(tickers=security, flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], VWAP_Dt='20240101')
                                        opt["commod_code"] = ticker
                                        opt["contract_month"] = opt.index.str[2:3] + year[3:]
                                        opt.set_index("px_settle_last_dt", drop=True, inplace=True)
                                        temp = pd.concat([temp, opt], axis=0, ignore_index=False)
                                        temp.to_csv("Ag.csv", mode="w")
                                else:
                                    opt = blp.bdh(tickers = ticker + " " + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date=dt.date.today())
                                    try:
                                        opt.columns = opt.columns.droplevel()
                                        opt["commod_code"] = ticker
                                        opt["contract_month"] = opt.index.str[2:3] + year[3:]
                                        temp = pd.concat([temp, opt], axis=0, ignore_index=False)
                                        temp.to_csv("Ag.csv", mode="w")
                                    except: 
                                        pass
                    else:
                        for month in basket[ticker]:
                            if month == curr_month:
                                signal = 1
                            
                            if signal == 1:
                                df = blp.bds(ticker + month + year[3:] + " " + "Comdty", "OPT_CHAIN")
                                if (df.shape[0] != 0):
                                    for security in df.iloc[:, 0]:
                                        opt = blp.bdp(tickers=security, flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], VWAP_Dt='20240101')
                                        opt["commod_code"] = ticker
                                        opt["contract_month"] = opt.index.str[2:3] + year[3:]
                                        opt.set_index("px_settle_last_dt", drop=True, inplace=True)
                                        temp = pd.concat([temp, opt], axis=0, ignore_index=False)
                                        temp.to_csv("Ag.csv", mode="w")
                                else:
                                    opt = blp.bdh(tickers = ticker + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date=dt.date.today())
                                    try:
                                        opt.columns = opt.columns.droplevel()
                                        opt["commod_code"] = ticker
                                        opt["contract_month"] = opt.index.str[2:3] + year[3:]
                                        temp = pd.concat([temp, opt], axis=0, ignore_index=False)
                                        temp.to_csv("Ag.csv", mode="w")
                                    except: 
                                        pass


AgScraper = AgScraper()
AgScraper.get_data(curr_month="U", curr_yr="2025", target_yr="2025", mode="w")
