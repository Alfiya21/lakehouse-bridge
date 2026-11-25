"""Synthetic source data with deliberate defects so the quality gates have work to do.
All data is synthetic. No real customer data is used."""
import json
import numpy as np
import pandas as pd
from . import config as C

FIRST = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Sana", "Vihaan", "Zoya", "Isha", "Arjun"]
LAST = ["Sharma", "Khan", "Patil", "Iyer", "Singh", "Mulla", "Nair", "Das", "Gupta", "Reddy"]
REGIONS = ["APAC", "EMEA", "AMER"]


def generate(scale: float = C.SCALE, seed: int = 42) -> dict:
    C.ensure_dirs()
    rng = np.random.default_rng(seed)
    n_c, n_o = max(int(2000 * scale), 50), max(int(20000 * scale), 500)

    cust = pd.DataFrame({
        "customer_id": np.arange(1, n_c + 1),
        "full_name": [f"{rng.choice(FIRST)} {rng.choice(LAST)}" for _ in range(n_c)],
        "email": [f"user{i}@example.com" for i in range(1, n_c + 1)],
        "phone": [f"+91-98{rng.integers(10**7, 10**8)}" for _ in range(n_c)],
        "region": rng.choice(REGIONS, n_c, p=[0.5, 0.25, 0.25]),
        "segment": rng.choice(["RETAIL", "SMB", "ENTERPRISE"], n_c, p=[0.7, 0.2, 0.1]),
        "signup_date": pd.to_datetime("2024-01-01") + pd.to_timedelta(rng.integers(0, 700, n_c), unit="D"),
    })
    cust.loc[rng.choice(n_c, max(n_c // 100, 1), replace=False), "email"] = None   # 1% missing email

    start = pd.Timestamp("2025-01-01")
    orders = pd.DataFrame({
        "order_id": np.arange(1, n_o + 1),
        "customer_id": rng.integers(1, n_c + 1, n_o),
        "order_ts": start + pd.to_timedelta(rng.integers(0, 540 * 24 * 3600, n_o), unit="s"),
        "amount": np.round(rng.lognormal(6.0, 0.8, n_o), 2),
        "status": rng.choice(["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"], n_o,
                             p=[0.1, 0.2, 0.55, 0.1, 0.05]),
        "currency": "INR",
    })
    bad = rng.choice(n_o, max(n_o // 100, 1), replace=False)
    orders.loc[bad, "amount"] = -orders.loc[bad, "amount"]                          # 1% negative amounts
    orphan = rng.choice(n_o, max(n_o // 100, 1), replace=False)
    orders.loc[orphan, "customer_id"] = n_c + 999                                    # 1% orphan customers
    dups = orders.sample(max(n_o // 200, 1), random_state=seed)
    orders = pd.concat([orders, dups], ignore_index=True)                            # 0.5% duplicates

    batch = C.LANDING / "batch"
    stream = C.LANDING / "stream"
    batch.mkdir(parents=True, exist_ok=True)
    stream.mkdir(parents=True, exist_ok=True)
    for f in stream.glob("*.json"):
        f.unlink()
    cust.to_csv(batch / "customers.csv", index=False)
    orders.to_csv(batch / "orders.csv", index=False)

    n_e = n_o // 2
    ev = pd.DataFrame({
        "event_id": [f"e{i}" for i in range(n_e)],
        "order_id": rng.integers(1, n_o + 1, n_e),
        "event_ts": (start + pd.to_timedelta(rng.integers(0, 540 * 24 * 3600, n_e), unit="s")).astype(str),
        "method": rng.choice(["UPI", "CARD", "NETBANKING"], n_e, p=[0.6, 0.3, 0.1]),
        "amount": np.round(rng.lognormal(6.0, 0.8, n_e), 2),
        "status": rng.choice(["SUCCESS", "FAILED"], n_e, p=[0.93, 0.07]),
    })
    ev = pd.concat([ev, ev.sample(max(n_e // 100, 1), random_state=seed)], ignore_index=True)  # replayed events
    ev = ev.sort_values("event_ts", kind="stable").reset_index(drop=True)             # arrive in event-time order
    for i, idx in enumerate(np.array_split(np.arange(len(ev)), 5)):
        part = ev.iloc[idx]
        with open(stream / f"events_{i}.json", "w") as fh:
            for rec in part.to_dict("records"):
                fh.write(json.dumps(rec) + "\n")
    return {"customers": len(cust), "orders": len(orders), "events": len(ev)}


if __name__ == "__main__":
    print(generate())
