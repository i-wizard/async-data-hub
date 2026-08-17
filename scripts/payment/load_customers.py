from sqlalchemy import text

from app.config.settings import get_settings
from app.db.base import create_database_engine, create_session_factory
from app.db.models import CustomerAccount

settings = get_settings()


def generate_customer_data():
    names = ["Alice", "Bob", "Charlie", "David", "Eve"]
    data = []
    for name in names:
        data.append({"name": name, "balance": 500})
    return data


async def seed_db():
    db_engine = create_database_engine(settings.database_url)
    session_factory = create_session_factory(engine=db_engine)
    async with session_factory() as session:
        customer_data = generate_customer_data()
        for customer in customer_data:
            new_customer = CustomerAccount(name=customer["name"], balance=customer["balance"])
            session.add(new_customer)
        await session.commit()

if __name__ == "__main__":
    import asyncio

    asyncio.run(seed_db())
