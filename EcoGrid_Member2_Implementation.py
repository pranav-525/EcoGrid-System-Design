# Maisha Ferdoushi
# Student_ID: 67117
# EcoGrid Energy - Member 2
# Smart Meter Integration and Event Simulation

import random

def create_energy_event(house_id):
    generation = round(random.uniform(2, 10), 2)
    consumption = round(random.uniform(1, 8), 2)
    surplus = round(generation - consumption, 2)

    event = {
        "event": "EnergyAvailability",
        "house_id": house_id,
        "generation_kwh": generation,
        "consumption_kwh": consumption,
        "surplus_kwh": max(surplus, 0)
    }

    return event


print("EcoGrid Smart Meter Integration")
print("-------------------------------")

for number in range(1, 6):
    event = create_energy_event("House-" + str(number))
    print(event)
