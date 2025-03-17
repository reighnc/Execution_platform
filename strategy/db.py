import os
import pytz
import warnings
import psycopg2
import threading
import pandas as pd
import datetime as dt
from configparser import ConfigParser

warnings.simplefilter(action="ignore", category=UserWarning)


class DB:
    def __init__(self):
        self.CFG = ConfigParser()
        env = os.environ.get("ALGO_ENV")

        if env == "prod":
            self.CFG.read("prod_config.ini")
        else:
            self.CFG.read("dev_config.ini")

        self.host = self.CFG["DB"]["HOST"]
        self.port = self.CFG["DB"]["PORT"]
        self.database = self.CFG["DB"]["DATABASE"]
        self.username = self.CFG["DB"]["USER"]
        self.password = self.CFG["DB"]["PASSWORD"]

        self.engine = None
        self.engine_lock = threading.Lock()

    def connect(self):
        # self.engine = create_engine(f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}")
        self.engine = psycopg2.connect(
            database=self.database,
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port
        )

    # Users

    def get_users(self):
        with self.engine_lock:
            return pd.read_sql("SELECT * FROM backend_user", con=self.engine)

    def get_user(self, id):
        with self.engine_lock:
            return pd.read_sql("SELECT * FROM backend_user WHERE id=%s", con=self.engine, params=(int(id),)).iloc[0]

    def update_user(self, column, value, user_id):
        with self.engine_lock:
            cur = self.engine.cursor()
            cur.execute(f"UPDATE backend_user SET {column}=%s WHERE id=%s", (value, int(user_id)))
            self.engine.commit()
            cur.close()

    # Strategies

    def get_strategies(self):
        with self.engine_lock:
            return pd.read_sql("SELECT * FROM backend_strategy", con=self.engine)

    def get_strategy(self, id):
        with self.engine_lock:
            return pd.read_sql("SELECT * FROM backend_strategy WHERE id=%s", con=self.engine, params=(int(id),)).iloc[0]

    # Accounts

    # def get_accounts(self, str_id=None):
    def get_accounts(self):
        with self.engine_lock:
            # if str_id is None:
            return pd.read_sql("SELECT * FROM backend_account", con=self.engine)
            # else:
            #     return pd.read_sql(
            #         "SELECT * FROM backend_account WHERE id IN (SELECT account_id FROM backend_strategy_accounts WHERE strategy_id=%s)",
            #         con=self.engine,
            #         params=(int(str_id),)
            #     )
            #     # return pd.read_sql("SELECT * FROM backend_account WHERE id=%s", con=self.engine, params=(int(str_id),)).iloc[0]

    def get_account(self, id):
        with self.engine_lock:
            return pd.read_sql("SELECT * FROM backend_account WHERE id=%s", con=self.engine, params=(int(id),)).iloc[0]

    # Ports

    def get_ports(self, strategy_id):
        with self.engine_lock:
            # if strategy_id is not None:
            return pd.read_sql("SELECT * FROM backend_port WHERE strategy_id=%s", con=self.engine, params=(int(strategy_id),))
            # else:
            #     return pd.read_sql("SELECT * FROM backend_port", con=self.engine)

    # def get_port_account(self, port_id):
    #     with self.engine_lock:
    #         acc_id = pd.read_sql("SELECT account_id FROM backend_port WHERE id=%s", con=self.engine, params=(int(port_id),)).iloc[0].account_id
    #         return pd.read_sql("SELECT * FROM backend_account WHERE id=%s", con=self.engine, params=(int(acc_id),)).iloc[0]

    # def get_port_strategy(self, port_id):
    #     with self.engine_lock:
    #         str_id = pd.read_sql("SELECT strategy_id FROM backend_port WHERE id=%s", con=self.engine, params=(int(port_id),)).iloc[0].strategy_id
    #         return pd.read_sql("SELECT * FROM backend_strategy WHERE id=%s", con=self.engine, params=(int(str_id),)).iloc[0]

    def update_port(self, column, value, port_id):
        with self.engine_lock:
            cur = self.engine.cursor()
            cur.execute(f"UPDATE backend_port SET {column}=%s WHERE id=%s", (value, int(port_id)))
            self.engine.commit()
            cur.close()

    def clone_port(self, name, port, strategy_id):
        legs = self.get_legs(port.id)

        with self.engine_lock:
            cur = self.engine.cursor()

            cur.execute(
                """INSERT INTO backend_port
                (name, strategy_id, scrip, scrip_type,
                start_time, stop_time, squareoff_time, combined_sl,
                combined_target, to_re_execute, trading_mode, lots_multiplier_set,
                is_re_executed_port, execute_button, execute_button_lots,
                squareoff_button, stop_button, combined_exit_done)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (name, int(strategy_id), port.scrip, port.scrip_type,
                port.start_time, port.stop_time, port.squareoff_time,
                float(port.combined_sl), float(port.combined_target),
                port.to_re_execute, port.trading_mode, port.lots_multiplier_set,
                True, False, 0, False, False, False)
            )

            # new_id = id_df.iloc[0].id
            new_id = cur.fetchone()[0]
            print(new_id)

            for i, leg in legs.iterrows():
                cur.execute(
                    """INSERT INTO backend_leg
                    (name, port_id, lots, ins_type, strike_distance,
                    expiry, trade_type, order_type, limit_pct,
                    num_modifications, modification_wait_time,
                    sl_on, sl, target, status, entered_ins, entered_token,
                    entered_strike, entered_underlying_price, ltp,
                    running_pnl, booked_pnl, entry_order_id, exit_order_id,
                    entry_order_type, exit_order_type, entry_order_message,
                    exit_order_message, entry_order_status, exit_order_status,
                    entry_num_modifications_done, exit_num_modifications_done,
                    entry_filled_qty, exit_filled_qty, entry_executed_price,
                    exit_executed_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (leg['name'], int(new_id), int(leg.lots), leg.ins_type, int(leg.strike_distance),
                    leg.expiry, leg.trade_type, leg.order_type, float(leg.limit_pct),
                    int(leg.num_modifications), float(leg.modification_wait_time),
                    leg.sl_on, leg.sl, leg.target, "no_position", "", "",
                    0, 0, 0, 0, 0, "", "", "MARKET", "MARKET",
                    "", "", "", "", 0, 0, 0, 0, 0, 0)
                )

            self.engine.commit()
            cur.close()

    # Legs

    def get_legs(self, port_id):
        with self.engine_lock:
            return pd.read_sql("SELECT * FROM backend_leg WHERE port_id=%s", con=self.engine, params=(int(port_id),))

    def update_leg(self, column, value, leg_id):
        with self.engine_lock:
            cur = self.engine.cursor()
            cur.execute(f"UPDATE backend_leg SET {column}=%s WHERE id=%s", (value, int(leg_id)))
            self.engine.commit()
            cur.close()

    # Orders

    def add_order(self, ins, trade, qty, order_type, price, port_id, account_id):
        with self.engine_lock:
            now = dt.datetime.now(tz=pytz.timezone("Asia/Kolkata")).replace(tzinfo=pytz.timezone("UTC"))

            cur = self.engine.cursor()
            cur.execute("INSERT INTO backend_order (timestamp, instrument, trade, qty, order_type, price, port_id, account_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (now, ins, trade, int(qty), order_type, float(price), int(port_id), int(account_id)))
            self.engine.commit()
            cur.close()

    # Logs

    def add_log(self, timestamp, text, level, port_id):
        with self.engine_lock:
            cur = self.engine.cursor()
            cur.execute("INSERT INTO backend_log (timestamp, text, level, port_id) VALUES (%s, %s, %s, %s)", (timestamp, text, level, int(port_id) if port_id is not None else None))
            self.engine.commit()
            cur.close()

    # Alerts

    def get_pending_tv_alerts(self, port_id):
        with self.engine_lock:
            return pd.read_sql("SELECT * FROM backend_tradingviewalert WHERE port_id=%s AND status=%s", con=self.engine, params=(int(port_id),"pending"))

    def update_alert(self, column, value, alert_id):
        with self.engine_lock:
            cur = self.engine.cursor()
            cur.execute(f"UPDATE backend_tradingviewalert SET {column}=%s WHERE id=%s", (value, int(alert_id)))
            self.engine.commit()
            cur.close()