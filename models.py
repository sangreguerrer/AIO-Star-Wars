import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, String, ARRAY


POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secret')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'app')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'swapi')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5431')

PG_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass


class StarPerson(Base):
    __tablename__ = "star_person"

    id = Column(Integer, primary_key=True)
    birth_year = Column(String, nullable=True)
    eye_color = Column(String)
    films = Column(ARRAY(String))
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String, unique=True)
    skin_color = Column(String)
    species = Column(ARRAY(String))
    starships = Column(ARRAY(String))
    created = Column(String)
    edited = Column(String)
    vehicles = Column(ARRAY(String))
    url = Column(String)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
