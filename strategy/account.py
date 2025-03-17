import math
import json
import traceback
import pandas as pd
import datetime as dt
from time import sleep
import concurrent.futures as cf
from multitrade import Multitrade


class AccountManager:
    def __init__(self, db, logger, mc):
        self.db = db
        self.logger = logger
        self.mc = mc

        all_accounts = self.db.get_accounts()
        self.accounts_objs = {acc.id: None for i, acc in all_accounts.iterrows()}

        # self.scrip_map = {
        #     "NIFTY": "  ",
        #     "BANKNIFTY": "NIFTY BANK",
        #     "FINNIFTY": "NIFTY FIN SERVICE",
        #     # No underlying for CRUDEOIL & NATURALGAS
        #     "CRUDEOIL": "CRUDEOIL",
        #     "NATURALGAS": "NATURALGAS",
        # }

        self.pnl_mult = {
            "NIFTY": 1,
            "BANKNIFTY": 1,
            "FINNIFTY": 1,
            "CRUDEOIL": 100,
            "NATURALGAS": 1250,
        }

    def do_login(self, strategy):
        # str_accounts = self.db.get_accounts(strategy.id)

        # if isinstance(str_accounts, pd.Series):
        #     account = str_accounts
        #     if self.accounts_objs[account.id] is None:
        #         self.login(account)
        # else:
        #     for i, account in str_accounts.iterrows():
        #         if self.accounts_objs[account.id] is None:
        #             self.login(account)

        if self.accounts_objs[strategy.account_id] is None:
            self.login(strategy.account_id)

    def is_logged_in(self, strategy):
        # is_logged_in = True

        # str_accounts = self.db.get_accounts(strategy.id)

        # if isinstance(str_accounts, pd.Series):
        #     account = str_accounts
        #     if self.accounts_objs[account.id] is None or not self.accounts_objs[account.id].is_ws_connected:
        #         is_logged_in = False

        # else:
        #     for i, account in str_accounts.iterrows():
        #         if self.accounts_objs[account.id] is None or not self.accounts_objs[account.id].is_ws_connected:
        #             is_logged_in = False
        #             break

        # return is_logged_in

        return self.accounts_objs[strategy.account_id] is not None and self.accounts_objs[strategy.account_id].is_ws_connected

    def login(self, account_id):
        account = self.db.get_account(account_id)

        acc = Account(account, self.db, self.logger, self.mc)
        self.accounts_objs[account.id] = acc
        res = acc.login()

        if res == "error":
            self.accounts_objs[account.id] = None

    def get_orderbook(self, strategy):
        account_to_use = self.accounts_objs[strategy.account_id]
        return account_to_use.get_orderbook()

    def place_order(self, strategy, ins, token, lots, tt, ot, leg, port, qty_type="lots"):
        account = self.db.get_account(strategy.account_id)
        exch_data = self.get_exchange(port.scrip)

        ltp = self.get_ltp(strategy, exch_data[2], token)
        underlying_ltp = self.get_ltp(strategy, exch_data[0], self.get_underlying_token(port, strategy))

        if tt == "BUY":
            limit_price = ltp * (1 + leg.limit_pct / 100)
        elif tt == "SELL":
            limit_price = ltp * (1 - leg.limit_pct / 100)

        limit_price = round(round(limit_price / 0.05) * 0.05, 2)

        # for i, account in accounts.iterrows():

        if qty_type == "lots":
            qty = int(math.floor(lots * account.lots_multiplier) * self.get_lot_size(port.scrip))
        elif qty_type == "qty":
            qty = lots

        order_id, msg = self.accounts_objs[account.id].place_order(exch_data[2], ins, token, tt, ot, qty, limit_price, port)
        return order_id, msg, qty, ltp, underlying_ltp

    def cancel_order(self, order_id, strategy):
        account_to_use = self.accounts_objs[strategy.account_id]
        return account_to_use.cancel_order(order_id)

    # -------------------------------------
    # Utilities
    # -------------------------------------

    def get_ltp(self, strategy, exch, token):
        # accounts = self.db.get_accounts(strategy.id)
        # account_to_use = self.accounts_objs[accounts.iloc[0].id]

        account_to_use = self.accounts_objs[strategy.account_id]
        return account_to_use.get_ltp(exch, token)

    def get_exchange(self, scrip):
        if scrip in ("NIFTY", "BANKNIFTY", "FINNIFTY"):
            return ["NSECM", "NFO", "NSEFO"]
        elif scrip in ("CRUDEOIL", "NATURALGAS"):
            return ["MCX", "MCX", "MCX"]

    def get_instrument(self, scrip, str_distance, expiry, ins_type, strategy, port):
        # accounts = self.db.get_accounts(strategy.id)
        # account_to_use = self.accounts_objs[accounts.iloc[0].id]

        account_to_use = self.accounts_objs[strategy.account_id]

        exch_data = self.get_exchange(scrip)

        if ins_type == "FUT":
            row = account_to_use.master_contract[
                (account_to_use.master_contract["symbol"] == scrip) &
                (account_to_use.master_contract["exchange"] == exch_data[2]) &
                (account_to_use.master_contract["option_type"] == ins_type) &
                (account_to_use.master_contract["expiry_date"] == dt.datetime(expiry.year, expiry.month, expiry.day, 0, 0, 0))
            ].iloc[0]

            # row = self.mc[
            #     (self.mc["symbol"] == scrip) &
            #     (self.mc["exchange"] == exch_data[1]) &
            #     (self.mc["option_type"] == ins_type) &
            #     (self.mc["expiry_date"] == expiry.strftime("%Y-%m-%d"))
            # ].iloc[0]

            return row.sec_description, 0, row.sec_id

        else:
            str_dis_map = {
                "NIFTY": 50,
                "BANKNIFTY": 100,
                "FINNIFTY": 50,
                "CRUDEOIL": 50,
                "NATURALGAS": 5
            }

            underlying_ltp = self.get_ltp(strategy, exch_data[0], self.get_underlying_token(port, strategy))
            atm = round(underlying_ltp / str_dis_map[scrip]) * str_dis_map[scrip]

            if ins_type == "CE":
                strike = atm + (str_distance * str_dis_map[scrip])
            elif ins_type == "PE":
                strike = atm - (str_distance * str_dis_map[scrip])

            row = account_to_use.master_contract[
                (account_to_use.master_contract["symbol"] == scrip) &
                (account_to_use.master_contract["exchange"] == exch_data[2]) &
                (account_to_use.master_contract["option_type"] == ins_type) &
                (account_to_use.master_contract["expiry_date"] == dt.datetime(expiry.year, expiry.month, expiry.day, 0, 0, 0)) &
                (account_to_use.master_contract["strike_price"] == strike)
            ].iloc[0]

            # row = self.mc[
            #     (self.mc["name"] == scrip) &
            #     (self.mc["exchange"] == exch_data[1]) &
            #     (self.mc["instrument_type"] == ins_type) &
            #     (self.mc["expiry"] == expiry.strftime("%Y-%m-%d")) &
            #     (self.mc["strike"] == strike)
            # ].iloc[0]

            return row.sec_description, strike, row.sec_id

    def get_lot_size(self, scrip):
        return self.mc[(self.mc["name"] == scrip) & (self.mc["instrument_type"] == "CE")].iloc[0].lot_size # Lot Size is same for CE & PE

    def get_underlying_token(self, port, strategy):
        # accounts = self.db.get_accounts(strategy.id)
        # account_to_use = self.accounts_objs[accounts.iloc[0].id]

        account_to_use = self.accounts_objs[strategy.account_id]

        if port.scrip_type == "INDEX":
            index_name_map = {
                "NIFTY": "Nifty 50",
                "BANKNIFTY": "Nifty Bank",
                "FINNIFTY": "Nifty Fin Service",
                # No index for MCX
                # "CRUDEOIL": "Crude Oil",
                # "NATURALGAS": "Natural Gas"
            }

            return account_to_use.master_contract[
                (account_to_use.master_contract["symbol"] == index_name_map[port.scrip]) &
                (account_to_use.master_contract["instrument_type"] == "INDEX") &
                (account_to_use.master_contract["exchange"] == self.get_exchange(port.scrip)[0])
            ].iloc[0].sec_id

        elif port.scrip_type == "FUT":
            expiry = self.mc[(self.mc["name"] == port.scrip) & (self.mc["instrument_type"] == "FUT")].expiry.min().to_pydatetime()

            return account_to_use.master_contract[
                (account_to_use.master_contract["symbol"] == port.scrip) &
                (account_to_use.master_contract["option_type"] == "FUT") &
                (account_to_use.master_contract["exchange"] == self.get_exchange(port.scrip)[2]) &
                (account_to_use.master_contract["expiry_date"] == dt.datetime(expiry.year, expiry.month, expiry.day, 0, 0, 0))
            ].iloc[0].sec_id


class Account:
    def __init__(self, account, db, logger, master_contract):
        self.account = account
        self.db = db
        self.logger = logger
        self.kite_master_contract = master_contract
        self.master_contract = None

        self.is_ws_connected = False

        # if self.account.type == "MULTITRADE":
        self.broker = Multitrade( # For now, multitrade is the only broker
            self.account.api_key,
            self.account.api_secret,
            self.account.root_url,
            self.account.ws_root_url
        )

        self.ticks_dict = {}
        self.tokens_dict = {}

    def login(self):
        try:
            req_token = self.broker.login()
            self.broker.generate_session_token(req_token)
            self.connect_broker_ws()

            self.store_master_contract()

        except Exception as e:
            self.logger.log(f"Error {e} came while trying to login to account: {self.account['name']}", "ERROR")
            sleep(5)

            return "error"

    def connect_broker_ws(self):
        self.broker.connect_ws(self.ws_on_connect, self.ws_on_message, self.ws_on_close, self.ws_on_error)

    def store_master_contract(self):
        for i in range(10):
            try:
                self.master_contract = self.broker.get_master_contract()
                self.master_contract = self.master_contract[self.master_contract.exchange.isin(["NSEFO", "NSECM", "MCX"])]
                self.master_contract = self.master_contract[self.master_contract.instrument_type.isin(["INDEX", "FUTIDX", "FUTCOM", "OPTIDX", "OPTFUT"])]

                break

            except Exception as e:
                self.logger.log(f"Error {e} came while fetching master contract from Account#{self.account['name']}", "ERROR")
                traceback.print_exc()

    def ws_on_connect(self, ws):
        self.logger.log(f"Connected to Multitrade Websocket successfully: {self.account['name']}", "SUCCESS")

        self.ticks_dict = {}
        self.tokens_dict = {}

    def ws_on_message(self, ws, msg):
        try:
            msg = json.loads(msg)

            if msg["Message"] == "HandShake":
                # Not needed for Multitrade Market Data WS V1
                # self.broker.login_ws(self.broker.req_token) # To login to WS using tokens

                self.is_ws_connected = True

            # elif msg["Message"] == "LoginResponse" and msg["Status"] == "success":
            if msg["Message"] == "Broadcast":
                self.ticks_dict[f"{msg['EXC']}:{msg['SECID']}"] = float(msg["LTP"])

        except Exception as e:
            self.logger.log(f"Error {e} came in WS on_message", "ERROR")

    def ws_on_close(self, ws, code, reason):
        self.logger.log(f"Multitrade Websocket connection closed @ {code} {reason}, retrying in 1 sec: {self.account['name']}", "ERROR")

        sleep(1)

        self.connect_broker_ws()

    def ws_on_error(self, ws, err):
        self.logger.log(f"Multitrade Websocket error @ {err}: {self.account['name']}", "ERROR")

    def get_ltp(self, exch, token):
        token_str = f"{exch}:{token}"

        if token_str not in list(self.ticks_dict.keys()):
            self.broker.subscribe(exch, token)
            self.ticks_dict[token_str] = 0.0

            self.logger.log(f"Subscribed to {token_str}", "SUCCESS")

        for i in range(50):
            ltp = self.ticks_dict[token_str]

            if ltp != 0.0:
                return ltp
            else:
                sleep(0.1)

    def get_orderbook(self):
        try:
            orderbook = self.broker.orderbook()

        except Exception as e:
            self.logger.log(f"Error {e} came while fetching orderbook from Account#{self.account['name']}", "ERROR")
            sleep(1)

        return orderbook

    def place_order(self, exch, ins, token, tt, ot, qty, price, port):
        order_id = "0"
        msg = ""

        if port.trading_mode == "Live":
            try:
                order_id, msg = self.broker.place_order(
                    exch,
                    token,
                    tt,
                    ot,
                    qty,
                    price
                )

            except Exception as e:
                self.logger.log(f"Error {e} came while placing order in Account#{self.account['name']}", "ERROR", port.id)
                sleep(1)

        elif port.trading_mode == "Paper":
            msg = "sucess"

        self.db.add_order(ins, tt, qty, ot, price, port.id, self.account.id)
        self.logger.log(f"Order Placed: {ins} | Trade: {tt} | Qty: {qty} | Type: {ot} | Price: {price} | Account: {self.account['name']}", "SUCCESS", port.id)

        return order_id, msg

    def cancel_order(self, order_id):
        try:
            return self.broker.cancel_order(order_id)

        except Exception as e:
            self.logger.log(f"Error {e} came while cancelling order: {order_id} from Account#{self.account['name']}", "ERROR")
            sleep(1)