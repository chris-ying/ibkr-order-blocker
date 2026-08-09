import time
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
BLOCK_END_TIME = clock_time(14, 0)

ALL_ORDERS_BLOCK_WINDOWS = (
    (clock_time(9, 20), clock_time(9, 39)),
    (clock_time(10, 50), clock_time(10, 59)),
    (clock_time(11, 30), clock_time(11, 59)),
    (clock_time(22, 30), clock_time(23, 40)),
)

MAX_SHORT_POSITION_VALUE = 6000
MAX_SINGLE_ORDER_VALUE = 6000

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

        self.order_details = {}


        self.last_completed_order_times = {}

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
                f"for {symbol}. All new orders are blocked "
                f"from 10:50 AM to 10:59 AM & from 11:50 AM to 11:59 AM."
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
            "The cooldown will start only after the order is fully filled."
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
                rule_key = (con_id, action)

                self.last_completed_order_times[rule_key] = time.monotonic()
                details["completedRecorded"] = True

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
    print("Tuesday SELL orders are blocked before 2:00 PM.")
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
    print("Press Control+C to stop.")
    print()

    try:
        while app.isConnected():
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping the order controller...")

    finally:
        if app.isConnected():
            app.disconnect()

        print("Order controller stopped.")


if __name__ == "__main__":
    main()