from decimal import Decimal, ROUND_HALF_UP


def totals(rows):
    orders = sum(row["order_count"] for row in rows)
    units = sum(row["units_sold"] for row in rows)
    revenue = sum((row["revenue_eur"] for row in rows), Decimal("0.00"))
    average = (revenue / orders).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) if orders else Decimal("0.00")
    return orders, units, revenue, average


def rollup(rows, keys):
    groups = {}
    for row in rows:
        key = tuple(row[k] for k in keys)
        group = groups.setdefault(key, {**dict(zip(keys, key)), "order_count": 0, "units_sold": 0, "revenue_eur": Decimal("0.00")})
        for metric in ("order_count", "units_sold", "revenue_eur"):
            group[metric] += row[metric]
    for group in groups.values():
        group["average_order_value_eur"] = totals([group])[3]
    return sorted(groups.values(), key=lambda row: row["revenue_eur"], reverse=True)
