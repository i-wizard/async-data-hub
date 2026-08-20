import asyncio
from collections import Counter
from typing import List

import httpx

# A small stock with more buyers than units -> a real fight for the last items.
INITIAL_STOCK = 3
CONCURRENCY = 10

BASE_URL = "http://localhost:8000/api/v1/inventory"


async def reserve_concurrently(
    client: httpx.AsyncClient,
    product_id: str,
    strategy: str,
    concurrency_number: int,
    quantity: int = 1,
) -> List[httpx.Response]:
    tasks = [
        client.post(
            f"/products/{product_id}/reserve/{strategy}", json={"quantity": quantity}
        )
        for _ in range(concurrency_number)
    ]
    result = await asyncio.gather(*tasks)
    return result


async def create_product(client: httpx.AsyncClient, name: str, stock: int) -> dict:
    """Create a product and return its JSON (including the generated id)."""
    response = await client.post("/products", json={"name": name, "stock": stock})
    response.raise_for_status()
    return response.json()


async def product_stock(client: httpx.AsyncClient, product_id: str) -> int:
    """Return the product's current stock."""
    response = await client.get(f"/products/{product_id}")
    response.raise_for_status()
    return response.json()["stock"]


async def reservation_count(client: httpx.AsyncClient, product_id: str) -> int:
    """Return how many reservations were recorded for the product."""
    response = await client.get(f"/products/{product_id}/reservations/count")
    response.raise_for_status()
    return response.json()


def status_breakdown(responses: List[httpx.Response]):
    return dict(sorted(Counter(r.status_code for r in responses).items()))


async def naive() -> None:
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        product = await create_product(client, name="widget", stock=INITIAL_STOCK)
        pid = product["id"]

        responses = await reserve_concurrently(
            client, pid, "naive", concurrency_number=CONCURRENCY
        )

        final_stock = await product_stock(client, pid)
        count = await reservation_count(client, pid)
        print(f"initial stock       : {INITIAL_STOCK}")
        print(f"concurrent buyers   : {CONCURRENCY}")
        print(f"status codes        : {status_breakdown(responses)}")
        print(f"reservations created: {count}")
        print(f"final stock         : {final_stock}")

        # Oversold: more reservations than units ever existed.
        assert count > INITIAL_STOCK
        print(
            f"\n❌ BUG: sold {count} units but only {INITIAL_STOCK} existed (lost update)."
        )
        print("   -> demos 02-05 fix this with real concurrency control.")


async def optimistic() -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        product = await create_product(client, name="widget", stock=INITIAL_STOCK)
        pid = product["id"]

        responses = await reserve_concurrently(client, pid, "optimistic", concurrency_number=CONCURRENCY)
        successes = sum(1 for r in responses if r.status_code == 200)

        final_stock = await product_stock(client, pid)
        count = await reservation_count(client, pid)
        print(f"initial stock       : {INITIAL_STOCK}")
        print(f"concurrent buyers   : {CONCURRENCY}")
        print(f"status codes        : {status_breakdown(responses)}")
        print(f"successful reserves : {successes}")
        print(f"reservations created: {count}")
        print(f"final stock         : {final_stock}")

        assert successes == INITIAL_STOCK
        assert count == INITIAL_STOCK      # no oversell
        assert final_stock == 0
        print(f"\n✅ Exactly {INITIAL_STOCK} reserved (CAS on version); no oversell.")

async def pessimistic() -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        product = await create_product(client, name="widget", stock=INITIAL_STOCK)
        pid = product["id"]

        responses = await reserve_concurrently(client, pid, "pessimistic", concurrency_number=CONCURRENCY)
        successes = sum(1 for r in responses if r.status_code == 200)

        final_stock = await product_stock(client, pid)
        count = await reservation_count(client, pid)
        print(f"initial stock       : {INITIAL_STOCK}")
        print(f"concurrent buyers   : {CONCURRENCY}")
        print(f"status codes        : {status_breakdown(responses)}")
        print(f"successful reserves : {successes}")
        print(f"reservations created: {count}")
        print(f"final stock         : {final_stock}")

        assert successes == INITIAL_STOCK
        assert count == INITIAL_STOCK      # no oversell
        assert final_stock == 0
        print(f"\n✅ Exactly {INITIAL_STOCK} reserved (pessimistic lock); no oversell.")

async def atomic() -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        product = await create_product(client, name="widget", stock=INITIAL_STOCK)
        pid = product["id"]

        responses = await reserve_concurrently(client, pid, "atomic", concurrency_number=CONCURRENCY)
        successes = sum(1 for r in responses if r.status_code == 200)

        final_stock = await product_stock(client, pid)
        count = await reservation_count(client, pid)
        print(f"initial stock       : {INITIAL_STOCK}")
        print(f"concurrent buyers   : {CONCURRENCY}")
        print(f"status codes        : {status_breakdown(responses)}")
        print(f"successful reserves : {successes}")
        print(f"reservations created: {count}")
        print(f"final stock         : {final_stock}")

        assert successes == INITIAL_STOCK
        assert count == INITIAL_STOCK      # no oversell
        assert final_stock == 0
        print(f"\n✅ Exactly {INITIAL_STOCK} reserved (single atomic UPDATE); no oversell.")

if __name__ == "__main__":
    asyncio.run(pessimistic())