import pytz
import datetime as dt


class OrderTracker:
    def __init__(self, db, logger, account_manager, enter_leg_func, exit_leg_func):
        self.db = db
        self.logger = logger
        self.account_manager = account_manager

        self.enter_leg_func = enter_leg_func
        self.exit_leg_func = exit_leg_func

    def check_leg_order(self, orderbook, leg, port, strategy):
        exchange = self.account_manager.get_exchange(port.scrip)[2]

        if port.trading_mode == "Paper":
            return "continue"

        elif port.trading_mode == "Live":
            if leg.status == "no_position":
                return "continue"

            elif leg.status == "entered":
                if leg.entry_order_message not in ("sucess", "successful"):
                    self.db.update_leg("status", "no_position", leg.id)
                    return "return"

                else:
                    if leg.entry_order_status == "Execute":
                        return "continue"

                    elif leg.entry_order_status == "Reject":
                        self.db.update_leg("status", "no_position", leg.id)
                        return "return"

                    elif leg.entry_order_status in ("Pending", "PARTIALLY_FILLED"):
                        order = self.get_order(exchange, orderbook, leg.entry_order_id)

                        if leg.entry_order_type == "MARKET":
                            self.db.update_leg("entry_order_status", order["status"], leg.id)
                            self.db.update_leg("entry_filled_qty", int(order["filled_quantity"]), leg.id)
                            self.db.update_leg("entry_executed_price", float(order["average_price"]), leg.id)
                            return "return"

                        elif leg.entry_order_type == "LIMIT":
                            if order["status"] != "Pending":
                                self.db.update_leg("entry_order_status", order["status"], leg.id)
                                self.db.update_leg("entry_filled_qty", int(order["filled_quantity"]), leg.id)
                                self.db.update_leg("entry_executed_price", float(order["average_price"]), leg.id)
                                return "return"

                            else:
                                cur_time = dt.datetime.now(tz=pytz.timezone("Asia/Kolkata")).replace(tzinfo=None)
                                order_time = dt.datetime.strptime(order["order_timestamp"], "%d/%m/%Y%H:%M:%S")

                                time_diff = cur_time - order_time
                                max_time_diff = dt.timedelta(seconds=float(leg.modification_wait_time))

                                if time_diff >= max_time_diff:
                                    res = self.account_manager.cancel_order(leg.entry_order_id, strategy)
                                    self.logger.log(f"Time elapsed: {time_diff.seconds} > Max allowed time diff.: {max_time_diff.seconds}, retrying, entry order cancellation message: {str(res)}", "ERROR", port.id)

                                    if res["status"] == "success":
                                        res2 = self.replace_order(order["quantity"], leg, port, "entry")

                                        if res2 == "close":
                                            self.logger.log(f"No. of modifications already done: {leg.entry_num_modifications_done} exceeds Max allowed No. of modifications: {leg.num_modifications}, cancelling entry", "ERROR", port.id)
                                            self.db.update_leg("status", "no_position", leg.id)
                                            return "return"

            elif leg.status == "exited":
                if leg.exit_order_message not in ("sucess", "successful"):
                    self.db.update_leg("status", "entered", leg.id)
                    return "return"
                else:
                    if leg.exit_order_status == "Execute":
                        self.reset_leg_for_exit(leg, port)
                        return "continue"

                    elif leg.exit_order_status == "Reject":
                        self.db.update_leg("status", "entered", leg.id)
                        return "return"

                    elif leg.exit_order_status in ("Pending", "PARTIALLY_FILLED"):
                        order = self.get_order(exchange, orderbook, leg.exit_order_id)

                        if leg.exit_order_type == "MARKET":
                            self.db.update_leg("exit_order_status", order["status"], leg.id)
                            self.db.update_leg("exit_filled_qty", int(order["filled_quantity"]), leg.id)
                            self.db.update_leg("exit_executed_price", float(order["average_price"]), leg.id)
                            return "return"

                        elif leg.exit_order_type == "LIMIT":
                            if order["status"] != "Pending":
                                self.db.update_leg("exit_order_status", order["status"], leg.id)
                                self.db.update_leg("exit_filled_qty", int(order["filled_quantity"]), leg.id)
                                self.db.update_leg("exit_executed_price", float(order["average_price"]), leg.id)
                                return "return"

                            else:
                                cur_time = dt.datetime.now(tz=pytz.timezone("Asia/Kolkata"))
                                order_time = dt.datetime.strptime(order["exchange_timestamp"], "%d/%m/%Y%H:%M:%S")

                                time_diff = cur_time - order_time
                                max_time_diff = dt.timedelta(seconds=float(leg.modification_wait_time))

                                if time_diff >= max_time_diff:
                                    res = self.account_manager.cancel_order(leg.exit_order_id, strategy)
                                    self.logger.log(f"Time elapsed: {time_diff.seconds} > Max allowed time diff.: {max_time_diff.seconds}, retrying, exit order cancellation message: {str(res)}", "ERROR", port.id)

                                    if res["status"] == "successful":
                                        res2 = self.replace_order(order["quantity"], leg, port, "exit")

                                        if res2 == "close":
                                            self.logger.log(f"No. of modifications already done: {leg.exit_num_modifications_done} exceeds Max allowed No. of modifications: {leg.num_modifications}, cancelling exit", "ERROR", port.id)
                                            self.db.update_leg("status", "entered", leg.id)
                                            return "return"

    def get_order(self, exchange, orderbook, order_id):
        order = None

        for _order in orderbook:
            if _order["exchange"] == exchange and _order["order_id"] == order_id:
                order = _order
                break

        return order

    def replace_order(self, qty, leg, port, action_type):
        if action_type == "entry":
            if leg.entry_num_modifications_done >= leg.num_modifications:
                # self.enter_leg_func(leg, port, 1, "MARKET", qty)  # For final to be MARKET
                return "close" # For final to be close

            else:
                self.enter_leg_func(leg, port, 1, order_type="LIMIT", qty=qty, modification=True)
                return "success"

        elif action_type == "exit":
            if leg.exit_num_modifications_done >= leg.num_modifications:
                # self.enter_leg_func(leg, port, 1, "MARKET", qty)  # For final to be MARKET
                return "close" # For final to be close

            else:
                self.exit_leg_func(leg, port, 1, order_type="LIMIT", qty=qty, modification=True)
                return "success"

    def reset_leg_for_exit(self, leg, port):
        if leg.trade_type == "BUY":
            _pnl = leg.exit_executed_price - leg.entry_executed_price
        elif leg.trade_type == "SELL":
            _pnl = leg.entry_executed_price - leg.exit_executed_price

        pnl = round(_pnl * leg.exit_filled_qty * self.account_manager.pnl_mult[port.scrip], 2)
        booked_pnl = round(leg.booked_pnl + pnl, 2)

        self.logger.log(f"Booked P&L: {pnl} for Leg#{leg['name']}", "PNL", port.id)

        self.db.update_leg("status", "no_position", leg.id)
        self.db.update_leg("entered_ins", "", leg.id)
        self.db.update_leg("entered_token", "", leg.id)
        self.db.update_leg("entered_strike", 0, leg.id)
        self.db.update_leg("entered_underlying_price", 0, leg.id)
        self.db.update_leg("ltp", 0, leg.id)
        self.db.update_leg("running_pnl", 0, leg.id)
        self.db.update_leg("booked_pnl", float(booked_pnl), leg.id)

    def is_entered(self, leg):
        return leg.status == "entered" and leg.entry_order_message in ("sucess", "successful") and leg.entry_order_status == "Execute"

    def is_exited(self, leg):
        return leg.status == "no_position" and leg.exit_order_message in ("sucess", "successful") and leg.exit_order_status == "Execute"