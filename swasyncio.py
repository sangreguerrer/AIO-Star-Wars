import asyncio
import aiohttp
from more_itertools import chunked

from models import StarPerson, init_db, Session


MAX_CHUNK = 1


async def get_person(person_id, session):
    response = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    json = await response.json()
    return json


async def insert_person(records):
    persons = []
    for record in records:
        person_data = {}
        for attr in StarPerson.__table__.columns:
            if attr.name in record:
                person_data[attr.name] = record[attr.name]
        person = StarPerson(**person_data)
        persons.append(person)
    async with Session() as session:
        session.add_all(persons)
        await session.commit()


async def main():
    await init_db()
    session = aiohttp.ClientSession()
    for person_id_chunk in chunked(range(1, 90), MAX_CHUNK):
        coros = [get_person(person_id, session) for person_id in person_id_chunk]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_person(result))

    await session.close()
    all_tasks_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*all_tasks_set)
asyncio.run(main())
