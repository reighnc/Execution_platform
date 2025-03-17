import pytz
import traceback
import datetime as dt
import concurrent.futures as cf
from order_tracker import OrderTracker


class StrategyRunner:
    def __init__(self, strategy, db, logger, account_manager):
        self.strategy = strategy
        self.db = db
        self.logger = logger
        self.user = self.db.get_user(self.strategy.user_id)

        self.account_manager = account_manager
        self.order_tracker = OrderTracker(self.db, self.logger, self.account_manager, self.enter_leg, self.exit_leg)

    # Algo Functions

    def run(self):
        self.update_data()
        ports = self.db.get_ports(self.strategy.id)

        self.account_manager.do_login(self.strategy)
        if self.account_manager.is_logged_in(self.strategy):
            orderbook = self.account_manager.get_orderbook(self.strategy)
            # position_book = self.account_manager.get_positions()

            with cf.ThreadPoolExecutor(max_workers=10) as executor:
                futures = []

                for i, port in ports.iterrows():
                    futures.append(executor.submit(self.run_port, port, orderbook))

                for future in cf.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(e)
                        traceback.print_exc()

    def run_port(self, port, orderbook):
        now = dt.datetime.now(tz=pytz.timezone("Asia/Kolkata"))
        current_time = now.time()

        tv_alerts = self.db.get_pending_tv_alerts(port.id).sort_values(by=["id"], ascending=False)
        legs = self.db.get_legs(port.id)

        if len(tv_alerts) > 0:
            latest_alert = tv_alerts.iloc[0]
            self.db.update_alert("status", "complete", latest_alert.id)
        else:
            latest_alert = None

        if port.execute_button:
            # Reset execute button for next cycle, for this cycle, it will be considered
            self.db.update_port("execute_button", False, port.id)
            self.db.update_port("execute_button_lots", 0, port.id)

        if not port.combined_exit_done:
            combined_pnl = 0

            for k, leg in legs.iterrows():
                combined_pnl += leg.running_pnl + leg.booked_pnl

            combined_pnl = round(combined_pnl, 2)
            combined_exit = False

            if port.combined_sl != 0 and combined_pnl <= -port.combined_sl:
                self.logger.log(f"Combined P&L: {combined_pnl} went below Combined SL: {-port.combined_sl}, re-executing port", "INFO", port.id)
                combined_exit = True

            elif port.combined_target != 0 and combined_pnl >= port.combined_target:
                self.logger.log(f"Combined P&L: {combined_pnl} went above Combined Target: {port.combined_target}, re-executing port", "INFO", port.id)
                combined_exit = True

            if combined_exit:
                for k, leg in legs.iterrows():
                    if self.order_tracker.is_entered(leg):
                        self.exit_leg(leg, port)

                self.db.update_port("combined_exit_done", True, port.id)

                if port.to_re_execute:
                    self.re_execute_port(port)

        for j, leg in legs.iterrows():
            res = self.order_tracker.check_leg_order(orderbook, leg, port, self.strategy)

            if res != "continue":
                continue # This continue means continue in loop, i.e., go check next cycle

            if port.squareoff_button:
                # legs = self.db.get_legs(port.id)
                # for i, leg in legs.iterrows():
                if leg.status == "entered":
                    self.logger.log(f"Exiting Leg#{leg['name']} because Squareoff button was clicked", "INFO", port.id)
                    self.exit_leg(leg, port)

                self.db.update_port("squareoff_button", False, port.id)

            elif not port.stop_button:
                if current_time >= port.squareoff_time:
                    if leg.status == "entered":
                        self.logger.log(f"Exiting Leg#{leg['name']} as Squareoff time is reached", "INFO", port.id)
                        self.exit_leg(leg, port)

                elif current_time >= port.start_time and current_time <= port.squareoff_time: # After squareoff time no conditions are checked
                    if leg.status == "no_position" and current_time <= port.stop_time and not port.combined_exit_done: # Check Entry Conditions till Stop Time
                        to_take_entry = False
                        lots_to_enter = 0

                        if latest_alert is not None and latest_alert.type == "ENTRY":
                            strike = latest_alert.get('STRIKE')
                            expiry = latest_alert.get('EXPIRY')

                            if strike is not None and expiry is not None:
                                # Construct the expected Leg name
                                expected_leg_name = f"{port.name}-{latest_alert['TYPE']}-{strike}-{expiry}"

                                # Check if the leg name exist.
                                if leg['name'] == expected_leg_name:
                                    self.logger.log(f"Taking entry in Leg#{leg['name']} as Latest TV Entry Alert: {latest_alert.id} has come", "INFO", port.id)
                                    to_take_entry = True
                                    lots_to_enter = latest_alert.lots
                                else:
                                    continue # if the leg name does not match, then this is not the leg for this alert.
                            else:
                                # Fallback: Process the alert without strike/expiry
                                self.logger.log(f"Taking entry in Leg#{leg['name']} as Latest TV Entry Alert: {latest_alert.id} has come (strike/expiry not provided)", "INFO", port.id)
                                to_take_entry = True
                                lots_to_enter = latest_alert.lots
                            # expected_leg_name = f"{port.name}-{latest_alert['TYPE']}-{latest_alert.get('STRIKE')}-{latest_alert.get('EXPIRY')}"

                            # if leg['name'] == expected_leg_name:
                            #     self.logger.log(f"Taking entry in Leg#{leg['name']} as Latest TV Entry Alert: {latest_alert.id} has come", "INFO", port.id)
                            #     to_take_entry = True
                            #     lots_to_enter = latest_alert.lots
                            # else:
                            #     continue 
                            # self.logger.log(f"Taking entry in Leg#{leg['name']} as Latest TV Entry Alert: {latest_alert.id} has come", "INFO", port.id)
                            # to_take_entry = True
                            # lots_to_enter = latest_alert.lots

                        elif port.execute_button:
                            self.logger.log(f"Taking entry in Leg#{leg['name']} as Manual Execute Button was clicked", "INFO", port.id)
                            to_take_entry = True
                            lots_to_enter = port.execute_button_lots

                        elif port.is_re_executed_port:
                            self.logger.log(f"Taking entry in Leg#{leg['name']} as this port is re-executed", "INFO", port.id)
                            to_take_entry = True
                            lots_to_enter = port.lots_multiplier_set

                        if to_take_entry:
                            self.enter_leg(leg, port, lots_to_enter)
                            self.db.update_port("is_re_executed_port", False, port.id)

                    elif leg.status == "entered":
                        base_price_for_sl, sl = self.get_sl(leg)
                        base_price_for_tp, target = self.get_tp(leg)

                        ltp = self.account_manager.get_ltp(
                            self.strategy,
                            self.account_manager.get_exchange(port.scrip)[2],
                            leg.entered_token
                        )

                        underlying_ltp = self.account_manager.get_ltp(
                            self.strategy,
                            self.account_manager.get_exchange(port.scrip)[0],
                            self.account_manager.get_underlying_token(port, self.strategy)
                        )

                        to_exit = False

                        if latest_alert is not None and latest_alert.type == "EXIT":
                            # self.logger.log(f"Exiting Leg#{leg['name']} as Latest TV Exit Alert: {latest_alert.id} has come", "INFO", port.id)
                            # to_exit = True
                            # expected_leg_name = f"{port.name}-{latest_alert['TYPE']}-{latest_alert.get('STRIKE')}-{latest_alert.get('EXPIRY')}"

                            # if leg['name'] == expected_leg_name:
                            #     self.logger.log(f"Exiting Leg#{leg['name']} as Latest TV Exit Alert: {latest_alert.id} has come", "INFO", port.id)
                            #     to_exit = True
                            # else:
                            #     continue
                            strike = latest_alert.get('STRIKE')
                            expiry = latest_alert.get('EXPIRY')

                            if strike is not None and expiry is not None:
                                # Construct the expected Leg name
                                expected_leg_name = f"{port.name}-{latest_alert['TYPE']}-{strike}-{expiry}"

                                if leg['name'] == expected_leg_name:
                                    self.logger.log(f"Exiting Leg#{leg['name']} as Latest TV Exit Alert: {latest_alert.id} has come", "INFO", port.id)
                                    to_exit = True
                                else:
                                    continue
                            else:
                                # Fallback: Process the alert without strike/expiry
                                self.logger.log(f"Exiting Leg#{leg['name']} as Latest TV Exit Alert: {latest_alert.id} has come (strike/expiry not provided)", "INFO", port.id)
                                to_exit = True
                        else:
                            if leg.trade_type == "BUY":
                                if sl != 0 and leg.sl_on == "PREMIUM" and ltp <= base_price_for_sl - sl:
                                    self.logger.log(f"Premium: {ltp} went below SL: {base_price_for_sl - sl}, exiting Leg#{leg['name']}", "INFO", port.id)
                                    to_exit = True
                                elif target != 0 and ltp >= base_price_for_tp + target:
                                    self.logger.log(f"Premium: {ltp} went above Target: {base_price_for_tp + target}, exiting Leg#{leg['name']}", "INFO", port.id)
                                    to_exit = True

                            elif leg.trade_type == "SELL":
                                if sl != 0 and leg.sl_on == "PREMIUM" and ltp >= base_price_for_sl + sl:
                                    self.logger.log(f"Premium: {ltp} went above SL: {base_price_for_sl + sl}, exiting Leg#{leg['name']}", "INFO", port.id)
                                    to_exit = True
                                elif target != 0 and ltp <= base_price_for_tp - target:
                                    self.logger.log(f"Premium: {ltp} went below Target: {base_price_for_tp - target}, exiting Leg#{leg['name']}", "INFO", port.id)
                                    to_exit = True

                            if leg.sl_on == "UNDERLYING":
                                position_type = self.get_position_type(leg)

                                if sl != 0 and position_type == "BULLISH" and underlying_ltp <= base_price_for_sl - sl:
                                    self.logger.log(f"Underlying LTP: {underlying_ltp} went below SL: {base_price_for_sl - sl}, exiting Leg#{leg['name']}", "INFO", port.id)
                                    to_exit = True
                                elif sl != 0 and position_type == "BEARISH" and underlying_ltp >= base_price_for_sl + sl:
                                    self.logger.log(f"Underlying LTP: {underlying_ltp} went above SL: {base_price_for_sl + sl}, exiting Leg#{leg['name']}", "INFO", port.id)
                                    to_exit = True

                        if to_exit:
                            self.exit_leg(leg, port)
                        else:
                            if leg.trade_type == "BUY":
                                running_pnl = ltp - leg.entry_executed_price
                            elif leg.trade_type == "SELL":
                                running_pnl = leg.entry_executed_price - ltp

                            running_pnl = round(running_pnl * leg.entry_filled_qty * self.account_manager.pnl_mult[port.scrip], 2)

                            self.db.update_leg("ltp", float(ltp), leg.id)
                            self.db.update_leg("running_pnl", float(running_pnl), leg.id)

    def update_data(self):
        try:
            self.user = self.db.get_user(self.user.id)
            self.strategy = self.db.get_strategy(self.strategy.id)

        except IndexError:
            # Strategy might be deleted
            return "deleted"

    # Entry/Exit Functions

    def enter_leg(self, leg, port, alert_lots_multiplier, order_type=None, qty=0, modification=False):
        if qty == 0:
            lots = leg.lots * alert_lots_multiplier * self.strategy.lots_multiplier
        else:
            lots = qty

        if modification:
            ins = leg.entered_ins
            strike = leg.entered_strike
            token = leg.entered_token

        else:
            ins, strike, token = self.account_manager.get_instrument(
                port.scrip,
                leg.strike_distance,
                leg.expiry,
                leg.ins_type,
                self.strategy,
                port
            )

        entry_order_id, order_msg, order_qty, entry_price, underlying_entry_price = self.account_manager.place_order(
            self.strategy,
            ins,
            token,
            lots,
            leg.trade_type,
            leg.order_type if order_type is None else order_type,
            leg,
            port,
            qty_type="lots" if qty == 0 else "qty"
        )

        self.db.update_leg("status", "entered", leg.id)
        self.db.update_leg("entered_ins", ins, leg.id)
        self.db.update_leg("entered_token", token, leg.id)
        self.db.update_leg("entered_strike", int(strike), leg.id)
        self.db.update_leg("entered_underlying_price", float(underlying_entry_price), leg.id)
        self.db.update_leg("ltp", 0, leg.id)
        self.db.update_leg("running_pnl", 0, leg.id)

        self.db.update_leg("entry_order_id", entry_order_id, leg.id)
        self.db.update_leg("entry_order_type", leg.order_type if order_type is None else order_type, leg.id)
        self.db.update_leg("entry_order_message", order_msg, leg.id)

        if not modification:
            self.db.update_leg("entry_num_modifications_done", 0, leg.id)
            self.db.update_leg("lots_multiplier_set", alert_lots_multiplier, leg.id)
        else:
            self.db.update_leg("entry_num_modifications_done", leg.entry_num_modifications_done+1, leg.id)

        if port.trading_mode == "Live":
            self.db.update_leg("entry_order_status", "Pending", leg.id)
            self.db.update_leg("entry_filled_qty", 0, leg.id)
            self.db.update_leg("entry_executed_price", 0, leg.id)

        elif port.trading_mode == "Paper":
            self.db.update_leg("entry_order_status", "Execute", leg.id)
            self.db.update_leg("entry_filled_qty", int(order_qty), leg.id)
            self.db.update_leg("entry_executed_price", float(entry_price), leg.id)

    def exit_leg(self, leg, port, order_type=None, modification=False):
        # position = None

        # for _pos in position_book:
        #     if _pos["token"] == leg.entered_token:

        exit_order_id, order_msg, order_qty, exit_price, underlying_exit_price = self.account_manager.place_order(
            self.strategy,
            leg.entered_ins,
            leg.entered_token,
            # qty_to_exit,
            leg.entry_filled_qty,
            "SELL" if leg.trade_type == "BUY" else "BUY",
            leg.order_type if order_type is None else order_type,
            leg,
            port,
            qty_type="qty"
        )

        self.db.update_leg("status", "exited", leg.id)
        self.db.update_leg("exit_order_id", exit_order_id, leg.id)
        self.db.update_leg("exit_order_type", leg.order_type if order_type is None else order_type, leg.id)
        self.db.update_leg("exit_order_message", order_msg, leg.id)

        if not modification:
            self.db.update_leg("exit_num_modifications_done", 0, leg.id)
        else:
            self.db.update_leg("exit_num_modifications_done", leg.exit_num_modifications_done+1, leg.id)

        if port.trading_mode == "Live":
            self.db.update_leg("exit_order_status", "Pending", leg.id)
            self.db.update_leg("exit_filled_qty", 0, leg.id)
            self.db.update_leg("exit_executed_price", 0, leg.id)

        elif port.trading_mode == "Paper":
            self.db.update_leg("exit_order_status", "Execute", leg.id)
            self.db.update_leg("exit_filled_qty", int(order_qty), leg.id)
            self.db.update_leg("exit_executed_price", float(exit_price), leg.id)

    # Utility Functions

    def get_sl(self, leg):
        if leg.sl_on == "PREMIUM":
            base_price = leg.entry_executed_price
        elif leg.sl_on == "UNDERLYING":
            base_price = leg.entered_underlying_price

        try:
            sl_pts = round(float(leg.sl), 2)

        except ValueError:
            sl_pct = round(float(leg.sl.replace("%", "").strip()) / 100, 2)
            sl_pts = round(base_price * sl_pct, 2)

        return base_price, sl_pts

    def get_tp(self, leg):
        base_price = leg.entry_executed_price

        try:
            tp_pts = round(float(leg.target), 2)

        except ValueError:
            tp_pct = round(float(leg.target.replace("%", "").strip()) / 100, 2)
            tp_pts = round(base_price * tp_pct, 2)

        return base_price, tp_pts

    def get_position_type(self, leg):
        if leg.ins_type == "FUT" and leg.trade_type == "BUY":
            return "BULLISH"
        elif leg.ins_type == "FUT" and leg.trade_type == "SELL":
            return "BEARISH"
        elif leg.ins_type == "CE" and leg.trade_type == "BUY":
            return "BULLISH"
        elif leg.ins_type == "CE" and leg.trade_type == "SELL":
            return "BEARISH"
        elif leg.ins_type == "PE" and leg.trade_type == "BUY":
            return "BEARISH"
        elif leg.ins_type == "PE" and leg.trade_type == "SELL":
            return "BULLISH"

    def re_execute_port(self, port):
        cur_name = port['name']

        prefix = cur_name
        new_num = 1

        if "_REX" in cur_name:
            prefix = cur_name.split("_REX")[0]
            num = int(cur_name.split("_REX")[1])
            new_num = num + 1

        new_name = prefix + "_REX" + str(new_num)
        self.db.clone_port(new_name, port, self.strategy.id)