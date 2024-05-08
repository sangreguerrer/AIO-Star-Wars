import asyncio
import aiohttp
from more_itertools import chunked


from models import StarPerson, init_db, Session

MAX_CHUNK = 1


async def get_person(person_id, session):
    async with session.get(f"https://swapi.py4e.com/api/people/{person_id}/") as response:
        return await response.json()


async def insert_person(records, session):
    persons = []
    for record in records:
        person_data = {}
        for attr in StarPerson.__table__.columns:
            if attr.name in record:
                if isinstance(record[attr.name], list) and len(record[attr.name]) > 0:
                    person_data[attr.name] = []
                    for item in record[attr.name]:
                        response = await session.get(item)
                        json = await response.json()
                        if 'title' in json:
                            person_data[attr.name].append(json['title'])
                        elif 'name' in json:
                            person_data[attr.name].append(json['name'])
                elif attr.name == 'homeworld':
                    response = await session.get(record[attr.name])
                    json = await response.json()
                    person_data[attr.name] = json['name']
                else:
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
        asyncio.create_task(insert_person(result, session))

    await session.close()
    all_tasks_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*all_tasks_set)
asyncio.run(main())
