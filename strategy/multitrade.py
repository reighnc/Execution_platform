import io
import ssl
import json
import urllib3
import traceback
import pandas as pd
import requests as r
import datetime as dt
from time import sleep
from threading import Thread
from websocket import WebSocketApp

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



class Multitrade:
    def __init__(self, api_key, api_secret, root_url, ws_root_url, access_token=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.root = root_url
        self.ws_root = ws_root_url

        self.req_token = None
        self.access_token = access_token

        self.ws = None

        self.to_verify = False # For SSL Errors, set this to False

        self.paths = {
            "login": "connect/login",
            "session_token": "session/token",
            "profile": "user/profile",
            "orders": "orders",
            "master_contract": "instruments"
        }

    def login(self):
        response = r.post(
            self.root + self.paths["login"],
            data={
                "api_key": self.api_key,
                "api_secrets": self.api_secret
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Api-Version": "3"
            },
            verify=self.to_verify
        )

        data = response.json()
        return data["data"]["request_token"]

    def generate_session_token(self, req_token):
        response = r.get(
            self.root + self.paths["session_token"],
            headers={
                "Api-Version": "3",
                "Authorization": f"{self.api_key}:{req_token}"
            },
            verify=self.to_verify
        )

        data = response.json()
        print(data)

        self.req_token = req_token
        self.access_token = data["data"]["acess_token"]

    def profile(self):
        response = r.get(
            self.root + self.paths["profile"],
            headers={
                "Api-Version": "3",
                "Authorization": f"{self.api_key}:{self.access_token}"
            },
            verify=self.to_verify
        )

        return response.json()

    def orderbook(self):
        response = r.get(
            self.root + self.paths["orders"],
            headers={
                "Api-Version": "3",
                "Authorization": f"{self.api_key}:{self.access_token}"
            },
            verify=self.to_verify
        )

        return response.json()["data"]

    def place_order(self, exch, token, tt, ot, qty, price):
        response = r.post(
            self.root + self.paths["orders"] + "/regular",
            headers={
                "Api-Version": "3",
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"{self.api_key}:{self.access_token}"
            },
            data={
                "tradingsecurity": token,
                "exchange": exch,
                "transaction_type": tt,
                "order_type": ot,
                "quantity": str(qty),
                "validity": "DAY",
                "price": str(price) if ot == "LIMIT" else "0",
                "product": "CNC",
                "userid": "OWN"
            },
            verify=self.to_verify
        )

        res = response.json()

        try:
            order_id = str(res["data"]["orderid"])
        except Exception:
            order_id = "0"

        if res["status"] in ("sucess", "successful"):
            return order_id, res["status"]
        else:
            return order_id, str(res)

    def cancel_order(self, order_id):
        response = r.delete(
            self.root + self.paths["orders"] + "/regular/" + str(order_id),
            headers={
                "Api-Version": "3",
                "Authorization": f"{self.api_key}:{self.access_token}"
            },
            verify=self.to_verify
        )

        return response.json()

    def get_master_contract(self):
        res = r.get(
            self.root + self.paths["master_contract"],
            headers={
                "Api-Version": "3",
                "Content-Type": "application/x-www-form-urlencoded"
            },
            verify=self.to_verify
        )

        df = pd.read_csv(io.StringIO(res.text), names=["sec_id", "sec_id_2", "symbol", "sec_description", "prev_close", "expiry_date", "strike_price", "tick_size", "quantity", "option_type", "instrument_type", "exchange"], header=None)
        df["expiry_date"] = pd.to_datetime(df["expiry_date"], format="mixed", errors="coerce")
        print(df)

        return df

    # ---------------------------------------------
    # WS Functions
    # ---------------------------------------------

    def connect_ws(self, on_open, on_message, on_close, on_error):
        self.ws = WebSocketApp(
            self.ws_root,
            on_open=on_open,
            on_message=on_message,
            on_close=on_close,
            on_error=on_error
        )

        self.ws_thread = Thread(target=self.ws.run_forever, daemon=True, kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}} if not self.to_verify else None)
        # self.ws_thread = Thread(target=self.ws.run_forever, daemon=True)
        self.ws_thread.start()

    # Not needed for Multitrade Market Data WS V1
    # def login_ws(self, access_token): # To be run after connection opens
    #     self.ws.send(json.dumps({
    #         "Message": "Login",
    #         "API_KEY": self.api_key,
    #         "REQUEST_TOKEN": access_token
    #     }))

    def subscribe(self, exch, token):
        self.ws.send(json.dumps({
            "Message": "Broadcast",
            "EXC": exch,
            "SECID": token
        }))