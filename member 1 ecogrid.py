 class SmartMeterIntegration:
    def __init__(self):
        self.readings = {}

    def add_reading(self, meter_id, energy_amount):
        self.readings[meter_id] = energy_amount
        print(f"Meter {meter_id} reported {energy_amount} kWh.")
        return energy_amount


class Marketplace:
    def __init__(self):
        self.offers = []

    def create_offer(self, seller, energy_amount, price_per_kwh):
        offer = {
            "seller": seller,
            "energy": energy_amount,
            "price": price_per_kwh
        }

        self.offers.append(offer)

        print(
            f"{seller} added {energy_amount} kWh "
            f"for ${price_per_kwh} per kWh."
        )

        return offer

    def buy_energy(self, buyer, offer):
        print(
            f"{buyer} purchased {offer['energy']} kWh "
            f"from {offer['seller']}."
        )

        return True


class FinancialSettlement:
    def make_payment(self, buyer, seller, energy_amount, price_per_kwh):
        total = energy_amount * price_per_kwh

        print("Payment completed.")
        print(f"Buyer: {buyer}")
        print(f"Seller: {seller}")
        print(f"Amount: ${total:.2f}")

        return total


smart_meter = SmartMeterIntegration()
marketplace = Marketplace()
settlement = FinancialSettlement()

energy = smart_meter.add_reading("Meter-101", 12)

offer = marketplace.create_offer(
    "Seller A",
    energy,
    0.30
)

trade_completed = marketplace.buy_energy(
    "Buyer B",
    offer
)

if trade_completed:
    settlement.make_payment(
        "Buyer B",
        "Seller A",
        offer["energy"],
        offer["price"]
    )
