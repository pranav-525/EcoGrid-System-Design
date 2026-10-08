"""EcoGrid Energy Member 3 resilience prototype (Python standard library only).

Run: python EcoGrid_Member3_Implementation.py
Test: python EcoGrid_Member3_Implementation.py --test

Uses in-memory SQLite, a simulated payment provider and an outbox relay.
It demonstrates patterns, not production Kafka, IoT or payment integration.
Data resets on exit. Verified delivery is supplied as a simulated event.
"""

import json
import sqlite3
import sys
import unittest


class PaymentProvider:
    """Idempotent simulated provider, including an ambiguous timeout."""

    def __init__(self):
        self.payments = {}
        self.calls = 0

    def capture(self, key, cents, mode):
        self.calls += 1
        if key in self.payments:
            return self.payments[key]
        if mode == "declined":
            self.payments[key] = "DECLINED"
        else:
            self.payments[key] = "PAID"
        if mode == "timeout_after_payment":
            raise TimeoutError("Provider accepted payment but response was lost")
        return self.payments[key]

    def lookup(self, key):
        return self.payments.get(key, "UNKNOWN")


class EcoGrid:
    """Marketplace owns reservations and trades in this compact prototype.

    The local ledger represents Settlement for demonstration. Production
    contexts need their own storage and a durable broker between services.
    """

    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.provider = PaymentProvider()
        self.broker = []
        self.dead_letters = []
        self.metrics = {"duplicates": 0, "timeouts": 0, "broker_failures": 0}
        self.db.executescript("""
            CREATE TABLE inventory (seller TEXT PRIMARY KEY, available INTEGER);
            INSERT INTO inventory VALUES ('seller1', 10);
            CREATE TABLE trades (
                id TEXT PRIMARY KEY, seller TEXT, quantity INTEGER,
                cents INTEGER, state TEXT, mode TEXT);
            CREATE TABLE processed (event_id TEXT PRIMARY KEY);
            CREATE TABLE ledger (trade_id TEXT PRIMARY KEY, cents INTEGER);
            CREATE TABLE outbox (
                id INTEGER PRIMARY KEY, payload TEXT, published INTEGER DEFAULT 0);
        """)

    def emit(self, event):
        self.db.execute("INSERT INTO outbox(payload) VALUES (?)", (json.dumps(event),))

    def reserve(self, trade_id, quantity, cents, mode="success"):
        if quantity <= 0 or cents <= 0:
            raise ValueError("Quantity and payment must be positive")
        if mode not in {"success", "declined", "timeout_after_payment"}:
            raise ValueError("Unknown simulation mode")
        with self.db:
            changed = self.db.execute(
                "UPDATE inventory SET available=available-? "
                "WHERE seller='seller1' AND available>=?", (quantity, quantity)
            ).rowcount
            if not changed:
                raise ValueError("Insufficient energy available")
            self.db.execute("INSERT INTO trades VALUES (?, 'seller1', ?, ?, 'RESERVED', ?)",
                            (trade_id, quantity, cents, mode))
            self.emit({"event_id": trade_id + ':reserved', "type": 'EnergyReserved',
                       "trade_id": trade_id})
        self.log(trade_id, "Energy reserved atomically")

    def relay(self, broker_available=True, crash_after_publish=False):
        """An interrupted relay may deliver twice; consumer must deduplicate."""
        rows = self.db.execute("SELECT * FROM outbox WHERE published=0 ORDER BY id").fetchall()
        for row in rows:
            if not broker_available:
                self.metrics['broker_failures'] += 1
                return  # persisted outbox stays pending for a later attempt
            self.broker.append(json.loads(row['payload']))
            if crash_after_publish:
                return  # deliberately omit marking published to simulate a crash
            with self.db:
                self.db.execute("UPDATE outbox SET published=1 WHERE id=?", (row['id'],))

    def consume(self, event):
        required = {'event_id', 'type', 'trade_id'}
        if not isinstance(event, dict) or not required.issubset(event):
            self.dead_letters.append({'reason': 'Invalid event schema', 'event': event})
            return
        if not all(isinstance(event[k], str) and event[k] for k in required):
            self.dead_letters.append({'reason': 'Invalid event fields', 'event': event})
            return
        if self.db.execute("SELECT 1 FROM processed WHERE event_id=?", (event['event_id'],)).fetchone():
            self.metrics['duplicates'] += 1
            return
        trade = self.db.execute("SELECT * FROM trades WHERE id=?", (event['trade_id'],)).fetchone()
        if not trade or event['type'] not in {'EnergyReserved', 'DeliveryVerified', 'SettlementCompleted', 'SettlementFailed'}:
            self.dead_letters.append({'reason': 'Unknown trade or event type', 'event': event})
            return
        # Only a simulated DeliveryVerified event can trigger capture.
        if event['type'] == 'DeliveryVerified' and trade['state'] == 'RESERVED':
            try:
                outcome = self.provider.capture(trade['id'], trade['cents'], trade['mode'])
            except TimeoutError:
                self.metrics['timeouts'] += 1
                outcome = 'UNKNOWN'
            with self.db:
                self.apply_outcome(trade, outcome)
                self.db.execute("INSERT INTO processed VALUES (?)", (event['event_id'],))
        else:
            with self.db:
                self.db.execute("INSERT INTO processed VALUES (?)", (event['event_id'],))

    def apply_outcome(self, trade, outcome):
        if outcome == 'PAID':
            self.db.execute("INSERT OR IGNORE INTO ledger VALUES (?, ?)", (trade['id'], trade['cents']))
            state = 'COMPLETED'
            self.emit({'event_id': trade['id'] + ':settled', 'type': 'SettlementCompleted', 'trade_id': trade['id']})
        elif outcome == 'DECLINED':
            state = 'CANCELLED'
            self.db.execute("UPDATE inventory SET available=available+? WHERE seller=?", (trade['quantity'], trade['seller']))
            self.emit({'event_id': trade['id'] + ':failed', 'type': 'SettlementFailed', 'trade_id': trade['id']})
        else:
            state = 'SETTLEMENT_PENDING'
        self.db.execute("UPDATE trades SET state=? WHERE id=?", (state, trade['id']))
        self.log(trade['id'], state)

    def reconcile(self):
        for trade in self.db.execute("SELECT * FROM trades WHERE state='SETTLEMENT_PENDING'").fetchall():
            outcome = self.provider.lookup(trade['id'])
            if outcome != 'UNKNOWN':
                with self.db:
                    self.apply_outcome(trade, outcome)

    def drain(self):
        while self.broker:
            self.consume(self.broker.pop(0))

    @staticmethod
    def log(trade_id, message):
        print(json.dumps({'service': 'EcoGridPrototype', 'trade_id': trade_id, 'message': message}))


class ResilienceTests(unittest.TestCase):
    def test_duplicate_and_relay_crash(self):
        app = EcoGrid()
        app.reserve('T1', 2, 100)
        app.relay(crash_after_publish=True)
        app.relay()
        app.drain()
        event = {'event_id': 'D1', 'type': 'DeliveryVerified', 'trade_id': 'T1'}
        app.consume(event)
        app.consume(event)
        self.assertEqual(app.db.execute('SELECT COUNT(*) FROM ledger').fetchone()[0], 1)
        self.assertEqual(app.provider.calls, 1)
        self.assertEqual(app.metrics['duplicates'], 2)

    def test_timeout_reconciliation(self):
        app = EcoGrid()
        app.reserve('T2', 3, 150, 'timeout_after_payment')
        app.consume({'event_id': 'D2', 'type': 'DeliveryVerified', 'trade_id': 'T2'})
        self.assertEqual(app.db.execute('SELECT state FROM trades').fetchone()[0], 'SETTLEMENT_PENDING')
        self.assertEqual(app.db.execute('SELECT available FROM inventory').fetchone()[0], 7)
        app.reconcile()
        app.reconcile()
        self.assertEqual(app.db.execute('SELECT state FROM trades').fetchone()[0], 'COMPLETED')
        self.assertEqual(app.provider.calls, 1)
        self.assertEqual(app.db.execute('SELECT COUNT(*) FROM ledger').fetchone()[0], 1)

    def test_compensation(self):
        app = EcoGrid()
        app.reserve('T3', 4, 200, 'declined')
        event = {'event_id': 'D3', 'type': 'DeliveryVerified', 'trade_id': 'T3'}
        app.consume(event)
        app.consume(event)
        self.assertEqual(app.db.execute('SELECT available FROM inventory').fetchone()[0], 10)
        self.assertEqual(app.db.execute('SELECT state FROM trades').fetchone()[0], 'CANCELLED')

    def test_outage_invalid_event_and_overselling(self):
        app = EcoGrid()
        app.reserve('T4', 8, 400)
        with self.assertRaises(ValueError):
            app.reserve('T5', 3, 150)
        app.relay(broker_available=False)
        self.assertEqual(len(app.broker), 0)
        app.relay()
        app.drain()
        app.consume({'bad': 'message'})
        self.assertEqual(len(app.dead_letters), 1)
        self.assertEqual(app.db.execute('SELECT available FROM inventory').fetchone()[0], 2)


def demo():
    print('EcoGrid Member 3 demonstration: proposed resilience patterns')
    app = EcoGrid()
    app.reserve('TRADE-1', 2, 100)
    app.reserve('TRADE-2', 3, 150, 'timeout_after_payment')
    app.reserve('TRADE-3', 1, 50, 'declined')
    print('\nSimulated broker outage: events remain in the outbox.')
    app.relay(broker_available=False)
    app.relay(crash_after_publish=True)
    app.relay()
    app.drain()
    for i in range(1, 4):
        event = {'event_id': f'DELIVERY-{i}', 'type': 'DeliveryVerified', 'trade_id': f'TRADE-{i}'}
        app.consume(event)
        app.consume(event)
    print('\nReconcile the ambiguous payment without charging again.')
    app.reconcile()
    app.consume({'invalid': 'meter event'})
    app.relay()
    app.drain()
    print('\nFINAL TRADES')
    for row in app.db.execute('SELECT id, quantity, cents, state FROM trades'):
        print(dict(row))
    print('Ledger postings:', app.db.execute('SELECT COUNT(*) FROM ledger').fetchone()[0])
    print('Available energy units:', app.db.execute('SELECT available FROM inventory').fetchone()[0])
    print('Metrics:', app.metrics)
    print('Dead-letter messages:', len(app.dead_letters))
    print('\nScope: simulation only. Kafka, real meters, distributed databases,')
    print('circuit breakers, production telemetry and real payments are not implemented.')


if __name__ == '__main__':
    if '--test' in sys.argv:
        unittest.main(argv=[sys.argv[0]])
    else:
        demo()
