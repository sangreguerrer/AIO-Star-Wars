import asyncio
import datetime

from aiohttp import ClientSession
from asyncache import cached
import aiohttp
from cachetools import LRUCache
from more_itertools import chunked


from models import StarPerson, init_db, Session

MAX_CHUNK = 1


async def get_person(person_id, session):
    async with session.get(f"https://swapi.py4e.com/api/people/{person_id}/") as response:
        return await response.json()


async def get_url(url: str, session: ClientSession) -> dict:
    response = await session.get(url)
    data = await response.json()
    return data


@cached(key=lambda url, session: url, cache=LRUCache(maxsize=10000))
async def extract_name_from_url(url: str, session: ClientSession) -> str:
    response_dict = await get_url(url, session)
    return response_dict.get("name") or response_dict.get("title")


async def insert_person(records):
    async with aiohttp.ClientSession() as session:
        persons = []
        for record in records:
            person_data = {}
            for attr in StarPerson.__table__.columns:
                if attr.name in record:
                    films_coro = asyncio.gather(
                        *[extract_name_from_url(url, session) for url in record['films']]
                    )
                    species_coro = asyncio.gather(
                        *[extract_name_from_url(url, session) for url in record["species"]]
                    )
                    starships_coro = asyncio.gather(
                        *[extract_name_from_url(url, session) for url in record["starships"]]
                    )
                    vehicles_coro = asyncio.gather(
                        *[extract_name_from_url(url, session) for url in record["vehicles"]]
                    )
                    films, species, starships, vehicles = await asyncio.gather(
                        films_coro, species_coro, starships_coro, vehicles_coro
                    )
                    person_data = {
                        **record,
                        "films": films,
                        "species": species,
                        "starships": starships,
                        "vehicles": vehicles
                    }
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


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start_time)

# 0:00:05.072299

