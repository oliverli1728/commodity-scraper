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

    def get_data(self, curr_month: str, curr_yr: str, target_yr: str, mode: str, start_date: str):
        x = int(curr_yr)
        y = int(target_yr)
        years = list()
        for i in range(y - x + 1):
            years.append(x + i)
        
        temp = pd.DataFrame(columns=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"])
        global basket
        for year in years:
            year = str(year)
            if (mode == "w"):
                for ticker in basket.keys():
                    if len(ticker) == 1:
                        for month in basket[ticker]:
                            df = blp.bds(ticker + " " + month + year[3:] + " " + "Comdty", "OPT_CHAIN")
                            if (df.shape[0] != 0):
                                s = df.iloc[:, 0]
                                try:
                                    if (year == curr_yr):
                                        opt = blp.bdh(tickers=s, flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date = start_date)
                                    else: 
                                        opt = blp.bdh(tickers = ticker + " " + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date = start_date)
                                
                                    cols = opt.columns.to_frame()

                                    for i in range (opt.shape[1]):
                                        settlements = pd.DataFrame()
                                        settlements["px_settle"] = opt.iloc[:, i]
                                        settlements["commod_code"] = cols.iloc[i, 0][0:1]
                                        settlements["opt_put_call"] = cols.iloc[i, 0][4:5]
                                        if (year == curr_yr):
                                            settlements["opt_strike_px"] = cols.iloc[i, 0][-11:-7]
                                        settlements["contract_month"] = cols.iloc[i, 0][2:4]
                                        
                                        temp = pd.concat([temp, settlements], axis=0, ignore_index=False)
                                        temp.to_csv("Ag.csv", mode=mode)
                                except:
                                    pass
                            else:
                                strikes = blp.bds(tickers = ticker + " " + month + year[3:] + " " + "Comdty", flds=["opt_strike_px"])
                                try:
                                    opt = blp.bdh(tickers = ticker + " " + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date = start_date)
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
                                s = df.iloc[:, 0]
                                try:
                                    if (year == curr_yr):
                                        opt = blp.bdh(tickers=s, flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date = start_date)

                                    else: 
                                        opt = blp.bdh(tickers = ticker + " " + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date = start_date)
                                
                                    cols = opt.columns.to_frame()

                                    for i in range (opt.shape[1]):
                                        settlements = pd.DataFrame()
                                        settlements["px_settle"] = opt.iloc[:, i]
                                        if (len(ticker == 2)):
                                            settlements["commod_code"] = cols.iloc[i, 0][0:2]
                                            settlements["contract_month"] = cols.iloc[i, 0][2:4]
                                        elif (len(ticker) == 3):
                                            settlements["commod_code"] = cols.iloc[i, 0][0:3]
                                            settlements["contract_month"] = cols.iloc[i, 0][3:5]
                                        settlements["opt_put_call"] = cols.iloc[i, 0][4:5]
                                        if (year == curr_yr):
                                            settlements["opt_strike_px"] = cols.iloc[i, 0][-11:-7]

                                        temp = pd.concat([temp, settlements], axis=0, ignore_index=False)
                                        temp.to_csv("Ag.csv", mode=mode)

                                except:
                                    pass
                            else:
                                opt = blp.bdh(tickers = ticker + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date = start_date)
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
                                    s = df.iloc[:, 0]
                                    try:
                                        if (year == curr_yr):
                                            opt = blp.bdp(tickers=s, flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"])

                                        else: 
                                            opt = blp.bdp(tickers = ticker + " " + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"])
                                    
                                        cols = opt.columns.to_frame()

                                        for i in range (opt.shape[1]):
                                            settlements = pd.DataFrame()
                                            settlements["px_settle"] = opt.iloc[:, i]
                                            settlements["commod_code"] = cols.iloc[i, 0][0:1]
                                            settlements["opt_put_call"] = cols.iloc[i, 0][4:5]
                                            if (year == curr_yr):
                                                settlements["opt_strike_px"] = cols.iloc[i, 0][-11:-7]
                                            settlements["contract_month"] = cols.iloc[i, 0][2:4]

                                            temp = pd.concat([temp, settlements], axis=0, ignore_index=False)
                                            temp.to_csv("Ag.csv", mode="w")
                                    except:
                                        pass
                                else:
                                    opt = blp.bdp(tickers = ticker + " " + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date=dt.date.today())
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
                                    s = df.iloc[:, 0]
                                    try:
                                        if (year == curr_yr):
                                            opt = blp.bdp(tickers=s, flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"])

                                        else: 
                                            opt = blp.bdp(tickers = ticker + " " + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"])
                                    
                                        cols = opt.columns.to_frame()

                                        for i in range (opt.shape[1]):
                                            settlements = pd.DataFrame()
                                            settlements["px_settle"] = opt.iloc[:, i]
                                            if (len(ticker == 2)):
                                                settlements["commod_code"] = cols.iloc[i, 0][0:2]
                                                settlements["contract_month"] = cols.iloc[i, 0][2:4]
                                            elif (len(ticker) == 3):
                                                settlements["commod_code"] = cols.iloc[i, 0][0:3]
                                                settlements["contract_month"] = cols.iloc[i, 0][3:5]
                                            settlements["opt_put_call"] = cols.iloc[i, 0][4:5]
                                            if (year == curr_yr):
                                                settlements["opt_strike_px"] = cols.iloc[i, 0][-11:-7]

                                            temp = pd.concat([temp, settlements], axis=0, ignore_index=False)
                                            temp.to_csv("Ag.csv", mode="w")
                                    except:
                                        pass
                                else:
                                    opt = blp.bdp(tickers = ticker + month + year[3:] + " " + "Comdty", flds=["opt_strike_px", "opt_undl_px","opt_days_expire","opt_put_call", "px_settle", "px_settle_last_dt"], start_date=dt.date.today())
                                    try:
                                        opt.columns = opt.columns.droplevel()
                                        opt["commod_code"] = ticker
                                        opt["contract_month"] = opt.index.str[2:3] + year[3:]
                                        temp = pd.concat([temp, opt], axis=0, ignore_index=False)
                                        temp.to_csv("Ag.csv", mode="w")
                                    except: 
                                        pass


AgScraper = AgScraper()
AgScraper.get_data(curr_month="N", curr_yr="2024", target_yr="2025", mode="a", start_date="2024-01-01")
