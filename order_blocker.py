import json
import time
import signal
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

BLOCKED_WEEKDAY = 1
BLOCK_END_TIME = clock_time(10, 0)

# Block all BUY and SELL orders during IBKR overnight trading hours.
OVERNIGHT_BLOCK_START = clock_time(20, 0)
OVERNIGHT_BLOCK_END = clock_time(4, 0)

ALL_ORDERS_BLOCK_WINDOWS = (
    (clock_time(9, 29), clock_time(9, 39)),
    (clock_time(10, 50), clock_time(10, 59)),
    (clock_time(11, 30), clock_time(11, 59)),
)

MAX_SHORT_POSITION_VALUE = 6000
MAX_SINGLE_ORDER_VALUE = 6000
MAX_BUY_FILLS_PER_DAY = 3
MAX_SELL_FILLS_PER_DAY = 3
FILLED_COUNT_FILE = Path(__file__).with_name("daily_filled_counts.json")

# The cooldown starts only after an order is completely filled.
SAME_SIDE_FILLED_COOLDOWN_SECONDS = 10 * 60


class OrderBlocker(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

        self.ready = Event()
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

        self.daily_filled_counts = {}
        self.daily_filled_count_date = datetime.now(TIMEZONE).date()
        self.load_daily_filled_counts()

    def load_daily_filled_counts(self):
        try:
            if not FILLED_COUNT_FILE.exists():
                return

            data = json.loads(FILLED_COUNT_FILE.read_text())
            saved_date = data.get("date")
            today = str(datetime.now(TIMEZONE).date())

            if saved_date != today:
                return

            for item in data.get("counts", []):
                con_id = int(item["conId"])
                action = str(item["action"]).upper()
                count = int(item["count"])
                self.daily_filled_counts[(con_id, action)] = count

            if self.daily_filled_counts:
                print("Restored today's completed BUY/SELL fill counts.")
        except Exception as exc:
            print(f"Could not load filled-count file: {exc}")

    def save_daily_filled_counts(self):
        try:
            counts = [
                {"conId": con_id, "action": action, "count": count}
                for (con_id, action), count in self.daily_filled_counts.items()
            ]
            data = {
                "date": str(self.daily_filled_count_date),
                "counts": counts,
            }
            FILLED_COUNT_FILE.write_text(json.dumps(data, indent=2))
        except Exception as exc:
            print(f"Could not save filled-count file: {exc}")

    def reset_daily_filled_counts_if_needed(self):
        today = datetime.now(TIMEZONE).date()

        if today != self.daily_filled_count_date:
            self.daily_filled_counts.clear()
            self.daily_filled_count_date = today
            self.save_daily_filled_counts()
            print(f"Daily completed BUY/SELL fill counts reset: {today}")

    def daily_filled_limit_reached(self, con_id, symbol, action):
        if action not in {"BUY", "SELL"}:
            return False

        self.reset_daily_filled_counts_if_needed()

        key = (con_id, action)
        current_count = self.daily_filled_counts.get(key, 0)
        maximum = (
            MAX_BUY_FILLS_PER_DAY
            if action == "BUY"
            else MAX_SELL_FILLS_PER_DAY
        )

        if current_count >= maximum:
            print(
                f"Daily completed-fill limit reached: "
                f"{symbol} already has {current_count}/{maximum} "
                f"filled {action} orders today. Cancelling the new order."
            )
            return True

        return False

    def record_daily_completed_fill(self, con_id, symbol, action):
        if action not in {"BUY", "SELL"}:
            return

        self.reset_daily_filled_counts_if_needed()

        key = (con_id, action)
        self.daily_filled_counts[key] = self.daily_filled_counts.get(key, 0) + 1

        maximum = (
            MAX_BUY_FILLS_PER_DAY
            if action == "BUY"
            else MAX_SELL_FILLS_PER_DAY
        )

        self.save_daily_filled_counts()

        print(
            f"Daily completed-fill count: {symbol} {action} "
            f"{self.daily_filled_counts[key]}/{maximum}."
        )

        if self.daily_filled_counts[key] >= maximum:
            for pending_order_id, pending in list(self.active_stock_orders.items()):
                if (
                    pending["conId"] == con_id
                    and pending["action"] == action
                ):
                    print(
                        f"Daily {action} fill limit is now reached for {symbol}. "
                        f"Cancelling pending order {pending_order_id}."
                    )
                    self.request_cancel(pending_order_id)

    def nextValidId(self, order_id: int):
        print("Connected to TWS.")
        print(f"Next valid API order ID: {order_id}")

        self.reqPositions()
        self.reqAutoOpenOrders(True)
        self.reqOpenOrders()
        self.ready.set()

    def position(self, account, contract, position, avg_cost):
        con_id = int(contract.conId)
        quantity = float(position)

        self.positions[con_id] = quantity
        self.position_contracts[con_id] = contract
        self.position_accounts[con_id] = account

    def positionEnd(self):
        self.positions_loaded = True

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
                "completedRecorded": False,
            },
        )


        details["conId"] = con_id
        details["symbol"] = symbol
        details["action"] = action
        details["securityType"] = security_type

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

        should_cancel_all_orders = any(
            start_time <= now.time() < end_time
            for start_time, end_time in ALL_ORDERS_BLOCK_WINDOWS
        )

        if should_cancel_all_orders:
            print(
                f"Time restriction triggered. "
                f"Cancelling new {action} order {order_id} "
                f"for {symbol}."
            )

            self.request_cancel(order_id)
            return

        should_cancel_overnight_order = (
            security_type == "STK"
            and action in {"BUY", "SELL"}
            and (
                now.time() >= OVERNIGHT_BLOCK_START
                or now.time() < OVERNIGHT_BLOCK_END
            )
        )

        if should_cancel_overnight_order:
            print(
                f"Overnight trading restriction triggered. "
                f"Cancelling {action} order {order_id} for {symbol}. "
                f"Stock BUY and SELL orders are blocked from "
                f"{OVERNIGHT_BLOCK_START.strftime('%H:%M')} to "
                f"{OVERNIGHT_BLOCK_END.strftime('%H:%M')} "
            )

            self.request_cancel(order_id)
            return

        should_cancel_tuesday_sell = (
            now.weekday() == BLOCKED_WEEKDAY
            and now.time() < BLOCK_END_TIME
            and action == "SELL"
        )

        if should_cancel_tuesday_sell:
            print(
                f"Tuesday restriction triggered. "
                f"Cancelling SELL order {order_id} for {symbol}."
            )

            self.request_cancel(order_id)
            return

        if security_type == "STK":
            if order_type not in {"LMT", "STP LMT"}:
                print(
                    f"Single-order value could not be verified: "
                    f"{symbol} {action} order {order_id} is {order_type}. "
                    "Only LMT and STP LMT stock orders are allowed."
                )

                self.request_cancel(order_id)
                return

            order_price = float(order.lmtPrice)

            if order_price <= 0:
                print(
                    f"Invalid limit price. "
                    f"Cancelling order {order_id}."
                )

                self.request_cancel(order_id)
                return

            single_order_value = quantity * order_price

            if single_order_value > MAX_SINGLE_ORDER_VALUE:
                print(
                    f"Single-order limit exceeded: "
                    f"{symbol} {action} order {order_id}, "
                    f"quantity={quantity:.2f}, "
                    f"price=${order_price:.2f}, "
                    f"value=${single_order_value:.2f}, "
                    f"maximum=${MAX_SINGLE_ORDER_VALUE:.2f}."
                )

                self.request_cancel(order_id)
                return

            print(
                f"Single-order value check passed: "
                f"{symbol} {action} order {order_id}, "
                f"value=${single_order_value:.2f}."
            )

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

        if security_type == "STK" and action in {"BUY", "SELL"}:
            if self.daily_filled_limit_reached(con_id, symbol, action):
                self.request_cancel(order_id)
                return

        if security_type == "STK" and action == "SELL":
            if not self.positions_loaded:
                print(
                    f"Position information is not ready. "
                    f"Cancelling SELL order {order_id} for safety."
                )

                self.request_cancel(order_id)
                return

            current_position = self.positions.get(con_id, 0.0)

            other_pending_sell_quantity = sum(
                pending_order["quantity"]
                for pending_order_id, pending_order
                in self.active_sell_orders.items()
                if pending_order_id != order_id
                and pending_order["conId"] == con_id
            )

            projected_position = (
                current_position
                - other_pending_sell_quantity
                - quantity
            )

            if projected_position < 0:
                if order_type != "LMT":
                    print(
                        f"Short market order blocked: "
                        f"{symbol} order {order_id}. "
                        "Use a limit order so the short value can be checked."
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

                projected_short_shares = abs(projected_position)
                projected_short_value = (
                    projected_short_shares * limit_price
                )

                if projected_short_value > MAX_SHORT_POSITION_VALUE:
                    print(
                        f"Short-position limit exceeded: "
                        f"{symbol}, "
                        f"projected short shares="
                        f"{projected_short_shares:.2f}, "
                        f"price=${limit_price:.2f}, "
                        f"projected value="
                        f"${projected_short_value:.2f}, "
                        f"maximum=${MAX_SHORT_POSITION_VALUE:.2f}."
                    )

                    self.request_cancel(order_id)
                    return

                print(
                    f"Short-position check passed: "
                    f"{symbol}, "
                    f"projected short value="
                    f"${projected_short_value:.2f}."
                )

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

                if security_type == "STK" and action in {"BUY", "SELL"}:
                    self.record_daily_completed_fill(con_id, symbol, action)

                print(
                    f"Cooldown started: {symbol} {action} order "
                    f"{order_id} was fully filled. "
                    "The same ticker and side are blocked for 10 minutes."
                )

        if normalized_status in {
            "cancelled",
            "apicancelled",
            "filled",
        }:
            self.active_sell_orders.pop(order_id, None)
            self.active_stock_orders.pop(order_id, None)
            self.cancel_requested.discard(order_id)

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
    confirmation = input('Type "FOLLOW YOUR RULES" to stop the order blocker: ')

    if confirmation.strip() == "FOLLOW YOUR RULES":
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

    print()
    print("Order blocker is running.")
    print("Tuesday SELL orders are blocked before 10:00 AM.")
    print(
        "Stock BUY and SELL orders are blocked overnight from "
        "8:00 PM to 4:00 AM Toronto time."
    )
    print(
        f"Maximum single stock order value: "
        f"${MAX_SINGLE_ORDER_VALUE:.2f}."
    )
    print(
        f"Maximum projected stock short value: "
        f"${MAX_SHORT_POSITION_VALUE:.2f}."
    )
    print(
        "After an order is fully filled, another order for the "
        "same ticker and same side is blocked for 10 minutes."
    )
    print(
        f"Maximum completed fills per ticker per day: "
        f"{MAX_BUY_FILLS_PER_DAY} BUY fills and "
        f"{MAX_SELL_FILLS_PER_DAY} SELL fills."
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