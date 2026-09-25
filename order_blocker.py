import json
import math
import time
import signal
import subprocess
from pathlib import Path
from datetime import datetime, time as clock_time
from threading import Event, Thread
from zoneinfo import ZoneInfo

from ibapi.client import EClient
from ibapi.order import Order
from ibapi.wrapper import EWrapper


HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 0

TIMEZONE = ZoneInfo("America/Toronto")

MARKET_OPEN_ORDER_BLOCK_START = clock_time(9, 30)
MARKET_OPEN_ORDER_BLOCK_END = clock_time(9, 40)

BUY_CANCEL_BLOCK_START = clock_time(9, 0)
BUY_CANCEL_BLOCK_END = clock_time(10, 00)

STOP_BUY_REMINDER_TIME = clock_time(9, 20)
STOP_BUY_REMINDER_END = clock_time(9, 30)

WHOLE_DOLLAR_SELL_PRICE_START = clock_time(9, 30)
WHOLE_DOLLAR_SELL_PRICE_END = clock_time(15, 0)
WHOLE_DOLLAR_SELL_COOLDOWN_SECONDS = 10 * 60

TUESDAY_SELL_BLOCK_START = clock_time(9, 30)
TUESDAY_SELL_BLOCK_END = clock_time(9, 55)

OVERNIGHT_START = clock_time(20, 0)
OVERNIGHT_END = clock_time(4, 0)
MAX_OVERNIGHT_BUYS = 2
MAX_OVERNIGHT_SELLS = 2
OVERNIGHT_HISTORY_FILE = Path(__file__).with_name("overnight_order_history.json")

MAX_POSITION_VALUE = 12000
EXISTING_ORDER_BLOCK_START = clock_time(9, 30)
EXISTING_ORDER_BLOCK_END = clock_time(10, 00)
MAX_NEW_ORDER_VALUE_WITH_EXISTING_ORDER = 5000
PROFIT_POPUP_POSITION_VALUE_MAX = 5000
PROFIT_POPUP_UNREALIZED_MIN = 100
PROFIT_POPUP_INTERVAL_SECONDS = 30 * 60

MAX_BUY_FILLS_PER_HOUR = 2
MAX_SELL_FILLS_PER_HOUR = 2
FILL_WINDOW_SECONDS = 60 * 60
FILLED_HISTORY_FILE = Path(__file__).with_name("hourly_filled_history.json")

# The cooldown starts only after an order is completely filled.
SAME_SIDE_FILLED_COOLDOWN_SECONDS = 10 * 60


def show_position_increase_popup(symbol, action, current_position, quantity):
    """Show a non-blocking native macOS warning when adding to an existing position."""
    def popup():
        try:
            message = (
                f"{symbol} {action} {quantity:g} shares\n\n"
                f"Current position: {current_position:g}"
            )

            apple_script = """
            on run argv
                display alert "POSITION INCREASE" ¬
                    message (item 1 of argv) ¬
                    as warning ¬
                    buttons {"OK"} ¬
                    default button "OK"
            end run
            """

            subprocess.run(
                ["osascript", "-e", apple_script, message],
                check=False,
                stdout=subprocess.DEVNULL,
            )
        except Exception as exc:
            print(f"Could not show position-increase popup: {exc}")

    Thread(target=popup, daemon=True).start()


def show_buy_cancel_popup():
    """Show a non-blocking native macOS warning when a BUY order is cancelled during the protected window."""
    def popup():
        try:
            apple_script = """
            display alert "DO NOT CANCEL YOUR BUY ORDERS." ¬
                as warning ¬
                buttons {"OK"} ¬
                default button "OK"
            """

            subprocess.run(
                ["osascript", "-e", apple_script],
                check=False,
                stdout=subprocess.DEVNULL,
            )
        except Exception as exc:
            print(f"Could not show BUY-cancel popup: {exc}")

    Thread(target=popup, daemon=True).start()


def show_stop_buy_reminder_popup():
    """Show a non-blocking macOS reminder to place stop BUY orders before market open."""
    def popup():
        try:
            apple_script = """
            display alert "PLACE STOP BUY ORDERS" ¬
                as warning ¬
                buttons {"OK"} ¬
                default button "OK"
            """

            subprocess.run(
                ["osascript", "-e", apple_script],
                check=False,
                stdout=subprocess.DEVNULL,
            )
        except Exception as exc:
            print(f"Could not show stop-BUY reminder popup: {exc}")

    Thread(target=popup, daemon=True).start()


def show_take_profit_popup(symbol, position_value, unrealized_pnl):
    """Show a non-blocking native macOS warning for a profitable position."""
    def popup():
        try:
            message = (
                f"{symbol}\n\n"
                f"Position value: ${abs(position_value):,.0f}\n"
                f"Unrealized profit: ${unrealized_pnl:,.0f}"
            )

            apple_script = """
            on run argv
                display alert "LOCK IN $100 PROFITS" ¬
                    message (item 1 of argv) ¬
                    as warning ¬
                    buttons {"OK"} ¬
                    default button "OK"
            end run
            """

            subprocess.run(
                ["osascript", "-e", apple_script, message],
                check=False,
                stdout=subprocess.DEVNULL,
            )
        except Exception as exc:
            print(f"Could not show take-profit popup: {exc}")

    Thread(target=popup, daemon=True).start()


class OrderBlocker(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

        self.ready = Event()
        self.stop_buy_reminder_date = None
        self.initial_orders_loaded = False
        self.positions_loaded = False

        self.initial_order_ids = set()
        self.processed_orders = set()
        self.cancel_requested = set()
        self.logged_orders = set()

        self.positions = {}
        self.position_contracts = {}
        self.position_accounts = {}

        self.active_sell_orders = {}
        self.active_stock_orders = {}

        self.order_details = {}

        self.last_completed_order_times = {}
        self.whole_dollar_sell_blocked_times = {}
        self.pnl_request_symbols = {}
        self.next_pnl_request_id = 900000
        self.last_profit_popup_times = {}
        self.position_values = {}
        self.unrealized_pnls = {}

        self.hourly_filled_history = {}
        self.load_hourly_filled_history()

        self.overnight_order_history = {}
        self.load_overnight_order_history()

    def prune_hourly_filled_history(self):
        cutoff = datetime.now(TIMEZONE).timestamp() - FILL_WINDOW_SECONDS

        for key in list(self.hourly_filled_history):
            recent = [
                ts for ts in self.hourly_filled_history[key]
                if ts >= cutoff
            ]

            if recent:
                self.hourly_filled_history[key] = recent
            else:
                self.hourly_filled_history.pop(key, None)

    def load_hourly_filled_history(self):
        try:
            if not FILLED_HISTORY_FILE.exists():
                return

            data = json.loads(FILLED_HISTORY_FILE.read_text())

            for item in data.get("fills", []):
                con_id = int(item["conId"])
                action = str(item["action"]).upper()
                timestamps = [float(ts) for ts in item.get("timestamps", [])]
                self.hourly_filled_history[(con_id, action)] = timestamps

            self.prune_hourly_filled_history()

            if self.hourly_filled_history:
                print("Restored completed BUY/SELL fills from the last hour.")
        except Exception as exc:
            print(f"Could not load hourly filled-history file: {exc}")

    def save_hourly_filled_history(self):
        try:
            self.prune_hourly_filled_history()
            fills = [
                {
                    "conId": con_id,
                    "action": action,
                    "timestamps": timestamps,
                }
                for (con_id, action), timestamps
                in self.hourly_filled_history.items()
            ]
            FILLED_HISTORY_FILE.write_text(
                json.dumps({"fills": fills}, indent=2)
            )
        except Exception as exc:
            print(f"Could not save hourly filled-history file: {exc}")

    def hourly_filled_limit_reached(self, con_id, symbol, action):
        if action not in {"BUY", "SELL"}:
            return False

        self.prune_hourly_filled_history()

        key = (con_id, action)
        current_count = len(self.hourly_filled_history.get(key, []))
        maximum = (
            MAX_BUY_FILLS_PER_HOUR
            if action == "BUY"
            else MAX_SELL_FILLS_PER_HOUR
        )

        if current_count >= maximum:
            oldest = min(self.hourly_filled_history[key])
            seconds_until_available = max(0, int(oldest + FILL_WINDOW_SECONDS - datetime.now(TIMEZONE).timestamp()))
            minutes = seconds_until_available // 60
            seconds = seconds_until_available % 60

            print(
                f"Hourly completed-fill limit reached: "
                f"{symbol} already has {current_count}/{maximum} "
                f"filled {action} orders in the last 60 minutes. "
                f"Cancelling the new order. Next slot in about "
                f"{minutes}m {seconds}s."
            )
            return True

        return False

    def record_hourly_completed_fill(self, con_id, symbol, action):
        if action not in {"BUY", "SELL"}:
            return

        self.prune_hourly_filled_history()

        key = (con_id, action)
        self.hourly_filled_history.setdefault(key, []).append(
            datetime.now(TIMEZONE).timestamp()
        )

        maximum = (
            MAX_BUY_FILLS_PER_HOUR
            if action == "BUY"
            else MAX_SELL_FILLS_PER_HOUR
        )
        current_count = len(self.hourly_filled_history[key])

        self.save_hourly_filled_history()

        print(
            f"Hourly completed-fill count: {symbol} {action} "
            f"{current_count}/{maximum} in the last 60 minutes."
        )

        if current_count >= maximum:
            for pending_order_id, pending in list(self.active_stock_orders.items()):
                if (
                    pending["conId"] == con_id
                    and pending["action"] == action
                ):
                    print(
                        f"Hourly {action} fill limit is now reached for {symbol}. "
                        f"Cancelling pending order {pending_order_id}."
                    )
                    self.request_cancel(pending_order_id)

    def is_overnight(self, now=None):
        if now is None:
            now = datetime.now(TIMEZONE)

        current_time = now.time()
        return current_time >= OVERNIGHT_START or current_time < OVERNIGHT_END

    def overnight_session_key(self, now=None):
        if now is None:
            now = datetime.now(TIMEZONE)

        # The session is named by the calendar date on which it begins at 8:00 PM.
        if now.time() < OVERNIGHT_END:
            session_date = now.date().fromordinal(now.date().toordinal() - 1)
        else:
            session_date = now.date()

        return session_date.isoformat()

    def load_overnight_order_history(self):
        try:
            if not OVERNIGHT_HISTORY_FILE.exists():
                return

            data = json.loads(OVERNIGHT_HISTORY_FILE.read_text())
            session_key = self.overnight_session_key()

            if data.get("session") == session_key:
                self.overnight_order_history = {
                    "BUY": int(data.get("BUY", 0)),
                    "SELL": int(data.get("SELL", 0)),
                }
                print(
                    "Restored overnight order counts: "
                    f"BUY={self.overnight_order_history['BUY']}/"
                    f"{MAX_OVERNIGHT_BUYS}, "
                    f"SELL={self.overnight_order_history['SELL']}/"
                    f"{MAX_OVERNIGHT_SELLS}."
                )
            else:
                self.overnight_order_history = {"BUY": 0, "SELL": 0}
        except Exception as exc:
            print(f"Could not load overnight order-history file: {exc}")
            self.overnight_order_history = {"BUY": 0, "SELL": 0}

    def save_overnight_order_history(self):
        try:
            session_key = self.overnight_session_key()
            data = {
                "session": session_key,
                "BUY": int(self.overnight_order_history.get("BUY", 0)),
                "SELL": int(self.overnight_order_history.get("SELL", 0)),
            }
            OVERNIGHT_HISTORY_FILE.write_text(json.dumps(data, indent=2))
        except Exception as exc:
            print(f"Could not save overnight order-history file: {exc}")

    def refresh_overnight_session(self, now=None):
        if now is None:
            now = datetime.now(TIMEZONE)

        session_key = self.overnight_session_key(now)

        try:
            if OVERNIGHT_HISTORY_FILE.exists():
                data = json.loads(OVERNIGHT_HISTORY_FILE.read_text())
                stored_session = data.get("session")
            else:
                stored_session = None
        except Exception:
            stored_session = None

        if stored_session != session_key:
            self.overnight_order_history = {"BUY": 0, "SELL": 0}
            self.save_overnight_order_history()

    def overnight_limit_reached(self, action, now=None):
        if action not in {"BUY", "SELL"}:
            return False

        if now is None:
            now = datetime.now(TIMEZONE)

        if not self.is_overnight(now):
            return False

        self.refresh_overnight_session(now)

        maximum = (
            MAX_OVERNIGHT_BUYS
            if action == "BUY"
            else MAX_OVERNIGHT_SELLS
        )
        current_count = int(self.overnight_order_history.get(action, 0))

        return current_count >= maximum

    def record_overnight_order(self, action, symbol, order_id, now=None):
        if action not in {"BUY", "SELL"}:
            return

        if now is None:
            now = datetime.now(TIMEZONE)

        if not self.is_overnight(now):
            return

        self.refresh_overnight_session(now)
        self.overnight_order_history[action] = (
            int(self.overnight_order_history.get(action, 0)) + 1
        )
        self.save_overnight_order_history()

        maximum = (
            MAX_OVERNIGHT_BUYS
            if action == "BUY"
            else MAX_OVERNIGHT_SELLS
        )

        print(
            f"Overnight {action} slot used by order {order_id} for {symbol}: "
            f"{self.overnight_order_history[action]}/{maximum}."
        )

    def nextValidId(self, order_id: int):
        print("Connected to TWS.")
        print(f"Next valid API order ID: {order_id}")
        self.reqPositions()
        self.reqAutoOpenOrders(True)
        self.reqOpenOrders()
        self.ready.set()


    def has_active_stop_buy_order(self):
        return any(
            pending.get("action") == "BUY"
            and pending.get("orderType") in {"STP", "STP LMT"}
            for pending in self.active_stock_orders.values()
        )

    def stop_buy_reminder_loop(self):
        while self.isConnected():
            now = datetime.now(TIMEZONE)
            today = now.date()

            if STOP_BUY_REMINDER_TIME <= now.time() < STOP_BUY_REMINDER_END:
                if self.stop_buy_reminder_date != today:
                    self.stop_buy_reminder_date = today

                    if not self.has_active_stop_buy_order():
                        show_stop_buy_reminder_popup()

            time.sleep(1)

    def position(self, account, contract, position, avg_cost):
        con_id = int(contract.conId)
        quantity = float(position)

        self.positions[con_id] = quantity
        self.position_contracts[con_id] = contract
        self.position_accounts[con_id] = account

    def positionEnd(self):
        self.positions_loaded = True

        for request_id in list(self.pnl_request_symbols):
            self.cancelPnLSingle(request_id)

        self.pnl_request_symbols.clear()

        for con_id, quantity in self.positions.items():
            if abs(quantity) <= 1e-9:
                continue

            account = self.position_accounts.get(con_id)
            contract = self.position_contracts.get(con_id)

            if not account or contract is None:
                continue

            symbol = str(contract.symbol).upper()
            request_id = self.next_pnl_request_id
            self.next_pnl_request_id += 1

            self.pnl_request_symbols[request_id] = {
                "conId": con_id,
                "symbol": symbol,
            }
            self.reqPnLSingle(request_id, account, "", con_id)

    def openOrder(self, order_id, contract, order, order_state):
        now = datetime.now(TIMEZONE)

        con_id = int(contract.conId)
        security_type = str(contract.secType).upper()
        symbol = str(contract.symbol).upper()
        action = str(order.action).upper()
        order_type = str(order.orderType).upper()
        status = str(order_state.status)
        quantity = float(order.totalQuantity)


        details = self.order_details.setdefault(
            order_id,
            {
                "conId": con_id,
                "symbol": symbol,
                "action": action,
                "securityType": security_type,
                "orderType": order_type,
                "orderRef": str(getattr(order, "orderRef", "")),
                "completedRecorded": False,
            },
        )


        details["conId"] = con_id
        details["symbol"] = symbol
        details["action"] = action
        details["securityType"] = security_type
        details["orderType"] = order_type
        details["orderRef"] = str(getattr(order, "orderRef", ""))

        log_key = (
            order_id,
            symbol,
            action,
            order_type,
            quantity,
            str(getattr(order, "lmtPrice", "")),
            status,
        )

        if log_key not in self.logged_orders:
            self.logged_orders.add(log_key)

            print(
                f"Order detected: "
                f"ID={order_id}, "
                f"symbol={symbol}, "
                f"action={action}, "
                f"type={order_type}, "
                f"quantity={quantity}, "
                f"status={status}"
            )

        if (
            security_type == "STK"
            and action == "SELL"
            and order_id != 0
            and status in {"PreSubmitted", "Submitted"}
        ):
            self.active_sell_orders[order_id] = {
                "conId": con_id,
                "quantity": quantity,
            }

        if (
            security_type == "STK"
            and action in {"BUY", "SELL"}
            and order_id != 0
            and status in {"PreSubmitted", "Submitted"}
        ):
            self.active_stock_orders[order_id] = {
                "conId": con_id,
                "symbol": symbol,
                "action": action,
                "quantity": quantity,
                "orderType": order_type,
                "orderRef": str(getattr(order, "orderRef", "")),
            }

        if not self.initial_orders_loaded:
            if order_id != 0:
                self.initial_order_ids.add(order_id)
            return

        if order_id == 0:
            return

        if order_id in self.initial_order_ids:
            return

        if order_id in self.processed_orders:
            return

        if status not in {"PreSubmitted", "Submitted"}:
            return

        self.processed_orders.add(order_id)

        # Global exception: always allow an order that only reduces or fully closes
        # a currently losing stock or option position. It must not reverse past flat.
        current_position = self.positions.get(con_id, 0.0)
        unrealized_pnl = self.unrealized_pnls.get(con_id)
        is_losing_position_close = (
            security_type in {"STK", "OPT"}
            and unrealized_pnl is not None
            and unrealized_pnl < 0
            and (
                (current_position > 1e-9 and action == "SELL" and quantity <= current_position + 1e-9)
                or (current_position < -1e-9 and action == "BUY" and quantity <= abs(current_position) + 1e-9)
            )
        )

        if is_losing_position_close:
            print(
                f"Loss-closing exception: allowing {symbol} {action} order {order_id} "
                f"to reduce/close a losing position."
            )
            return

        if (
            security_type in {"STK", "OPT"}
            and action == "SELL"
            and now.weekday() == 1
            and TUESDAY_SELL_BLOCK_START <= now.time() < TUESDAY_SELL_BLOCK_END
        ):
            print(
                f"Tuesday SELL restriction triggered. "
                f"Cancelling SELL order {order_id} for {symbol}. "
                f"Stock and option SELL orders are blocked from "
                f"{TUESDAY_SELL_BLOCK_START.strftime('%H:%M')} to {TUESDAY_SELL_BLOCK_END.strftime('%H:%M')}."
            )
            self.request_cancel(order_id)
            return

        if (
            security_type == "STK"
            and action in {"BUY", "SELL"}
            and self.is_overnight(now)
        ):
            if self.overnight_limit_reached(action, now):
                maximum = (
                    MAX_OVERNIGHT_BUYS
                    if action == "BUY"
                    else MAX_OVERNIGHT_SELLS
                )

                print(
                    f"Overnight {action} limit reached. "
                    f"Cancelling order {order_id} for {symbol}. "
                    f"Only {maximum} {action} order is allowed per "
                    f"overnight session "
                    f"({OVERNIGHT_START.strftime('%H:%M')}–"
                    f"{OVERNIGHT_END.strftime('%H:%M')})."
                )

                self.request_cancel(order_id)
                return

            # Reserve the overnight slot immediately when the order is accepted,
            # so a second order cannot slip through before the first one fills.
            self.record_overnight_order(action, symbol, order_id, now)

        # From 09:30 to 09:40, block every NEW stock/option BUY and SELL order.
        # Existing orders loaded when the script starts are left untouched.
        # Losing-position closes already returned above under the global exception.
        if (
            security_type in {"STK", "OPT"}
            and action in {"BUY", "SELL"}
            and MARKET_OPEN_ORDER_BLOCK_START <= now.time() < MARKET_OPEN_ORDER_BLOCK_END
        ):
            print(
                f"Market-open order block triggered. "
                f"Cancelling {symbol} {action} order {order_id}. "
                f"New stock and option BUY/SELL orders are blocked from "
                f"{MARKET_OPEN_ORDER_BLOCK_START.strftime('%H:%M')} to "
                f"{MARKET_OPEN_ORDER_BLOCK_END.strftime('%H:%M')}, "
                f"except orders that reduce/close a losing position."
            )
            self.request_cancel(order_id)
            return

        rule_key = (con_id, action)
        completed_at = self.last_completed_order_times.get(rule_key)
        if completed_at is not None:
            elapsed = time.monotonic() - completed_at

            if elapsed < SAME_SIDE_FILLED_COOLDOWN_SECONDS:
                remaining = max(
                    0,
                    int(SAME_SIDE_FILLED_COOLDOWN_SECONDS - elapsed),
                )

                minutes = remaining // 60
                seconds = remaining % 60

                print(
                    f"{symbol} {action} order {order_id} rejected by "
                    f"the completed-order 10-minute cooldown. "
                    f"Wait {minutes}m {seconds}s."
                )

                self.request_cancel(order_id)
                return

        if security_type in {"STK", "OPT"} and action in {"BUY", "SELL"}:
            if self.hourly_filled_limit_reached(con_id, symbol, action):
                self.request_cancel(order_id)
                return

        if security_type == "STK" and action in {"BUY", "SELL"}:
            # While holding a winning long position, the first non-whole-dollar SELL limit order
            # during the protected window is cancelled and starts a 10-minute cooldown.
            # Losing or breakeven long positions are not subject to this restriction.
            # After 10 minutes, non-whole-dollar SELL prices are allowed again.
            unrealized_pnl = self.unrealized_pnls.get(con_id)

            if (
                current_position > 1e-9
                and unrealized_pnl is not None
                and unrealized_pnl > 0
                and action == "SELL"
                and WHOLE_DOLLAR_SELL_PRICE_START <= now.time() < WHOLE_DOLLAR_SELL_PRICE_END
                and order_type == "LMT"
            ):
                sell_price = float(order.lmtPrice)

                if sell_price > 0 and not math.isclose(
                    sell_price,
                    round(sell_price),
                    abs_tol=1e-9,
                ):
                    blocked_at = self.whole_dollar_sell_blocked_times.get(con_id)

                    if blocked_at is None:
                        self.whole_dollar_sell_blocked_times[con_id] = time.monotonic()
                        print(
                            f"Whole-dollar SELL-price restriction triggered: "
                            f"{symbol} SELL order {order_id} price=${sell_price:.0f}. "
                            f"Cancelling this order. Non-whole-dollar SELL prices "
                            f"will be allowed after 10 minutes."
                        )
                        self.request_cancel(order_id)
                        return

                    elapsed = time.monotonic() - blocked_at

                    if elapsed < WHOLE_DOLLAR_SELL_COOLDOWN_SECONDS:
                        remaining = max(
                            0,
                            int(WHOLE_DOLLAR_SELL_COOLDOWN_SECONDS - elapsed),
                        )
                        minutes = remaining // 60
                        seconds = remaining % 60

                        print(
                            f"Whole-dollar SELL-price cooldown active: "
                            f"{symbol} SELL order {order_id} price=${sell_price:.2f}. "
                            f"Cancelling order. Wait {minutes}m {seconds}s."
                        )
                        self.request_cancel(order_id)
                        return

            # If this symbol already has a position AND another active order
            # on the SAME side as this new order, apply the existing-order restriction.
            # Existing BUY only restricts a new BUY.
            # Existing SELL only restricts a new SELL.
            has_other_active_same_side_order = any(
                pending_order_id != order_id
                and pending_order.get("conId") == con_id
                and pending_order.get("action") == action
                for pending_order_id, pending_order in self.active_stock_orders.items()
            )

            if abs(current_position) > 1e-9 and has_other_active_same_side_order:
                if EXISTING_ORDER_BLOCK_START <= now.time() < EXISTING_ORDER_BLOCK_END:
                    print(
                        f"Existing-position/order restriction triggered: "
                        f"{symbol} already has a position and another active {action} order. "
                        f"Cancelling order {order_id}; additional same-side orders are blocked from "
                        f"{EXISTING_ORDER_BLOCK_START.strftime('%H:%M')} to "
                        f"{EXISTING_ORDER_BLOCK_END.strftime('%H:%M')}."
                    )
                    self.request_cancel(order_id)
                    return

                if order_type in {"LMT", "STP LMT"}:
                    new_order_price = float(order.lmtPrice)

                    if new_order_price > 0:
                        new_order_value = abs(quantity * new_order_price)

                        if new_order_value > MAX_NEW_ORDER_VALUE_WITH_EXISTING_ORDER:
                            print(
                                f"New-order value limit exceeded: {symbol} {action} order {order_id}, "
                                f"quantity={quantity:.0f}, price=${new_order_price:.0f}, "
                                f"order value=${new_order_value:.0f}, "
                                f"maximum=${MAX_NEW_ORDER_VALUE_WITH_EXISTING_ORDER:.0f}."
                            )
                            self.request_cancel(order_id)
                            return

            # Warn whenever this order adds to an EXISTING position.
            # Long + BUY  -> increasing a long position.
            # Short + SELL -> increasing a short position.
            # Reducing/closing or opening from flat does not trigger the popup.
            is_increasing_position = (
                (current_position > 1e-9 and action == "BUY")
                or (current_position < -1e-9 and action == "SELL")
            )

            if is_increasing_position:
                show_position_increase_popup(
                    symbol,
                    action,
                    current_position,
                    quantity,
                )

            other_pending_buy_quantity = sum(
                pending_order.get("quantity", 0.0)
                for pending_order_id, pending_order
                in self.active_stock_orders.items()
                if pending_order_id != order_id
                and pending_order["conId"] == con_id
                and pending_order["action"] == "BUY"
            )

            other_pending_sell_quantity = sum(
                pending_order.get("quantity", 0.0)
                for pending_order_id, pending_order
                in self.active_stock_orders.items()
                if pending_order_id != order_id
                and pending_order["conId"] == con_id
                and pending_order["action"] == "SELL"
            )

            projected_position = (
                current_position
                + other_pending_buy_quantity
                - other_pending_sell_quantity
                + (quantity if action == "BUY" else -quantity)
            )


        if security_type == "STK" and action in {"BUY", "SELL"}:
            # Apply MAX_POSITION_VALUE only when THIS order increases absolute exposure.
            # Reducing or closing a position is always allowed by the position-value rule,
            # even when the remaining position is still above MAX_POSITION_VALUE.
            position_before_this_order = (
                current_position
                + other_pending_buy_quantity
                - other_pending_sell_quantity
            )
            is_increasing_absolute_position = (
                abs(projected_position) > abs(position_before_this_order) + 1e-9
            )

            if is_increasing_absolute_position and abs(projected_position) > 1e-9:
                # A limit price is required only for orders that increase exposure,
                # so the projected dollar value can be verified.
                if order_type not in {"LMT", "STP LMT"}:
                    print(
                        f"Position-value check could not be verified: "
                        f"{symbol} {action} order {order_id} is {order_type}. "
                        "Use LMT or STP LMT when increasing a position so the "
                        "projected position value can be checked."
                    )

                    self.request_cancel(order_id)
                    return

                limit_price = float(order.lmtPrice)

                if limit_price <= 0:
                    print(
                        f"Invalid limit price. "
                        f"Cancelling order {order_id}."
                    )

                    self.request_cancel(order_id)
                    return

                projected_position_value = abs(projected_position) * limit_price
                projected_side = "LONG" if projected_position > 0 else "SHORT"

                if projected_position_value > MAX_POSITION_VALUE:
                    print(
                        f"Position-value limit exceeded while increasing position: "
                        f"{symbol}, "
                        f"projected {projected_side.lower()} shares="
                        f"{abs(projected_position):.0f}, "
                        f"price=${limit_price:.0f}, "
                        f"projected value=${projected_position_value:.0f}, "
                        f"maximum=${MAX_POSITION_VALUE:.0f}."
                    )

                    self.request_cancel(order_id)
                    return

        print(
            f"{symbol} {action} order {order_id} allowed. "
        )

    def request_cancel(self, order_id):
        if order_id == 0:
            return

        if order_id in self.cancel_requested:
            return

        self.cancel_requested.add(order_id)
        self.cancelOrder(order_id)

    def openOrderEnd(self):
        if self.initial_orders_loaded:
            return

        self.initial_orders_loaded = True

        print(
            "Existing orders loaded. "
            "Order restrictions are active."
        )

    def orderStatus(
        self,
        order_id,
        status,
        filled,
        remaining,
        avg_fill_price,
        perm_id,
        parent_id,
        last_fill_price,
        client_id,
        why_held,
        mkt_cap_price=0,
    ):

        normalized_status = str(status).strip().lower()
        remaining_quantity = float(remaining)

        now = datetime.now(TIMEZONE)
        details = self.order_details.get(order_id)

        if (
            normalized_status in {"cancelled", "apicancelled"}
            and details is not None
            and details.get("action") == "BUY"
            and BUY_CANCEL_BLOCK_START <= now.time() < BUY_CANCEL_BLOCK_END
        ):
            show_buy_cancel_popup()

        if normalized_status == "filled" and remaining_quantity <= 1e-9:
            details = self.order_details.get(order_id)

            if details is None:
                print(
                    f"Filled order {order_id} was not found in "
                    "the local order-details cache, so no cooldown "
                    "could be recorded."
                )
            elif not details["completedRecorded"]:
                con_id = details["conId"]
                symbol = details["symbol"]
                action = details["action"]
                security_type = details["securityType"]
                rule_key = (con_id, action)

                self.last_completed_order_times[rule_key] = time.monotonic()
                details["completedRecorded"] = True

                if security_type in {"STK", "OPT"} and action in {"BUY", "SELL"}:
                    self.record_hourly_completed_fill(con_id, symbol, action)

                print(
                    f"Cooldown started: {symbol} {action} order "
                    f"{order_id} was fully filled. "
                    "The same-side orders are blocked for 10 minutes."
                )

        if normalized_status in {
            "cancelled",
            "apicancelled",
            "filled",
        }:
            self.active_sell_orders.pop(order_id, None)
            self.active_stock_orders.pop(order_id, None)
            self.cancel_requested.discard(order_id)

    def pnlSingle(self, req_id, pos, daily_pnl, unrealized_pnl, realized_pnl, value):
        details = self.pnl_request_symbols.get(req_id)

        if details is None:
            return

        symbol = details["symbol"]
        con_id = details["conId"]
        position_value = float(value)
        unrealized = float(unrealized_pnl)

        if math.isfinite(position_value) and abs(position_value) < 1e100:
            self.position_values[con_id] = position_value

        if math.isfinite(unrealized) and abs(unrealized) < 1e100:
            self.unrealized_pnls[con_id] = unrealized

        if abs(float(pos)) <= 1e-9:
            return

        if not math.isfinite(position_value) or not math.isfinite(unrealized):
            return

        if abs(position_value) <= 0.01:
            return

        if abs(unrealized) > 1e100:
            return

        if abs(position_value) <= 1:
            return

        if unrealized <= PROFIT_POPUP_UNREALIZED_MIN:
            return

        if unrealized <= abs(position_value) * 0.02:
            return

        now = time.monotonic()
        last_popup = self.last_profit_popup_times.get(symbol, 0.0)

        if now - last_popup < PROFIT_POPUP_INTERVAL_SECONDS:
            return

        self.last_profit_popup_times[symbol] = now
        print(
            f"Take Profit Reminder: {symbol}, "
            f"Position Value=${abs(position_value):.0f}, "
            f"Unrealized Profit=${unrealized:.0f}."
        )
        show_take_profit_popup(symbol, position_value, unrealized)

    def connectionClosed(self):
        print("TWS API connection closed.")

    def error(
        self,
        req_id,
        error_code,
        error_string,
        advanced_order_reject_json="",
    ):
        informational_codes = {
            2104,
            2106,
            2108,
            2150,
            2158,
        }

        if error_code in informational_codes:
            return

        print(
            f"IBKR message: "
            f"request={req_id}, "
            f"code={error_code}, "
            f"message={error_string}"
        )

def confirm_exit(signum, frame):
    print()
    print("Ctrl+C detected.")
    confirmation = input('Type "DO NOT TRADE WITH EMOTIONS" to stop the order blocker: ')

    if confirmation.strip() == "DO NOT TRADE WITH EMOTIONS":
        raise KeyboardInterrupt

    print("Incorrect phrase. Order blocker will continue running.")


def api_loop(app: OrderBlocker):
    app.run()


def main():
    app = OrderBlocker()

    print(f"Connecting to TWS at {HOST}:{PORT}...")

    app.connect(
        HOST,
        PORT,
        clientId=CLIENT_ID,
    )

    api_thread = Thread(
        target=api_loop,
        args=(app,),
        daemon=True,
    )

    api_thread.start()

    if not app.ready.wait(timeout=15):
        print("Could not establish a ready connection to TWS.")
        print("Check the TWS API setting and Socket Port.")

        app.disconnect()
        return

    stop_buy_reminder_thread = Thread(
        target=app.stop_buy_reminder_loop,
        daemon=True,
    )
    stop_buy_reminder_thread.start()

    print()
    print("Order blocker is running.")
    print(
        f"Maximum single position value: "
        f"${MAX_POSITION_VALUE:.0f}."
    )
    print(
        f"Tuesday SELL orders are blocked from "
        f"{TUESDAY_SELL_BLOCK_START.strftime('%H:%M')} to {TUESDAY_SELL_BLOCK_END.strftime('%H:%M')}."
    )
    print(
        f"New BUY/SELL orders are blocked from "
        f"{MARKET_OPEN_ORDER_BLOCK_START.strftime('%H:%M')} to "
        f"{MARKET_OPEN_ORDER_BLOCK_END.strftime('%H:%M')}, "
        "except stop-loss orders."
    )
    print(
        f"If an existing position has an active order, new orders are blocked from "
        f"{EXISTING_ORDER_BLOCK_START.strftime('%H:%M')} to "
        f"{EXISTING_ORDER_BLOCK_END.strftime('%H:%M')} "
        f"or above ${MAX_NEW_ORDER_VALUE_WITH_EXISTING_ORDER:.0f}."
    )
    print(
        f"While holding a winning long position, non-whole-dollar SELL orders after "
        f"{WHOLE_DOLLAR_SELL_PRICE_START.strftime('%H:%M')} "
        f"are blocked for 10 minutes."
    )
    print(
        "After an order is filled, same-side orders for the same ticker are blocked for 10 minutes."
    )
    print(
        f"Maximum completed fills per stock/option in 60-minute window: "
        f"{MAX_BUY_FILLS_PER_HOUR} BUY fills and "
        f"{MAX_SELL_FILLS_PER_HOUR} SELL fills."
    )
    print("Press Control+C to stop.")
    print()

    signal.signal(signal.SIGINT, confirm_exit)

    try:
        while app.isConnected():
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping the order blocker...")

    finally:
        if app.isConnected():
            app.disconnect()

        print("Order blocker stopped.")


if __name__ == "__main__":
    main()