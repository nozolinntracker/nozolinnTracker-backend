def compare_prices(myhotels, nozolinn):
    alerts = []
    for room in myhotels:
        match = next((r for r in nozolinn if r["hotel"] == room["hotel"] and r["room"] == room["room"]), None)
        if match:
            if match["price"] > room["price"]:
                alerts.append({
                    "hotel": room["hotel"],
                    "room": room["room"],
                    "myhotels_price": room["price"],
                    "nozolinn_price": match["price"]
                })
    return alerts
