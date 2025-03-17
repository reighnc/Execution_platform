import io
import pytz
import traceback
from db import DB
import pandas as pd
import requests as r
import datetime as dt
from time import sleep
from logger import Logger
import concurrent.futures as cf
from account import AccountManager
from strategy_runner import StrategyRunner


def get_master_contract(logger):
    for i in range(10):
        try:
            res = r.get("https://api.kite.trade/instruments")
            master_contract = pd.read_csv(io.StringIO(res.text))

            if "instrument_token" not in master_contract.columns:
                res = res.json()

                if res["message"] == "Too many requests":
                    print("Too many requests: master contract")
                    sleep(15)
                    continue

            break

        except Exception as e:
            logger.log(f"Error {e} came while fetching master contract from Zerodha API", "ERROR")
            traceback.print_exc()
            sleep(1)

    return master_contract


def main():
    db = DB()
    db.connect()

    logger = Logger(db)

    mc = get_master_contract(logger)
    mc["expiry"] = pd.to_datetime(mc["expiry"])
    mc = mc[mc["exchange"].isin(["NSE", "NFO", "MCX"])]

    account_manager = AccountManager(db, logger, mc)

    cur_t = dt.datetime.now(tz=pytz.timezone("Asia/Kolkata")).time()

    while cur_t < dt.time(hour=23, minute=35, second=0):
        strategies = []

        for i, strategy in db.get_strategies().iterrows():
            strategies.append(StrategyRunner(strategy, db, logger, account_manager))
            # logger.log(f"Started {strategy['name']}", "SUCCESS")

        with cf.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []

            for strategy in strategies:
                futures.append(executor.submit(strategy.run))

            for future in cf.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(e)
                    traceback.print_exc()

        cur_t = dt.datetime.now(tz=pytz.timezone("Asia/Kolkata")).time()

    for strategy in strategies:
        logger.log("Closing", "ERROR")


if __name__ == "__main__":
    while True:
        now = dt.datetime.now(tz=pytz.timezone("Asia/Kolkata")).time()

        if now > dt.time(hour=9, minute=14, second=30) and now < dt.time(hour=23, minute=35, second=0):
            main()

        else:
            print("Waiting for Time Limit")
            sleep(15)