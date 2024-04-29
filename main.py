import decimal
import json
import os
import time
from datetime import datetime, date

import jsonlines
import opyls
import psycopg2
from tqdm import tqdm
from dotenv import load_dotenv
from malparser import MAL

CHUNK_SIZE = 100


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if 'malparser' in str(type(o)):
            return o.__dict__
        # TypeError: Object of type Decimal is not JSON serializable
        if isinstance(o, decimal.Decimal):
            return str(o)
        if isinstance(o, set):
            return list(o)
        # TypeError: Object of type date is not JSON serializable
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)


def insert_into_db(conn, records_chunk: list[dict[str, str]]):
    sql = """
    INSERT INTO public.mal_backlog("id", "entity", payload)
        VALUES(%s,%s,%s);
    """

    try:
        with conn.cursor() as cursor:
            for record in records_chunk:
                cursor.execute(sql, (record['id'], record['entity'], record['payload']))

            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return


def build_query(entity_type: str, is_prod: bool = False) -> str:
    if entity_type not in ('anime', 'manga'):
        raise ValueError(f'Entity type {entity_type} is not supported')

    return f"""
        select distinct
            id::integer
        from public.{entity_type}
        where id not in (
            select distinct id from public.mal_backlog where entity = '{entity_type}'
        )
        {'limit 10' if not is_prod else ''}
    """.strip()


def parse_anime(filename: str):
    mal = MAL()
    base_dir = opyls.basedir('tmp')

    ids = opyls.load_json(base_dir / filename)

    with jsonlines.open(base_dir / (filename + 'l'), mode='w') as writer:
        for aid in tqdm(ids, desc="Parsing anime"):
            try:
                anime = mal.get_anime(aid)
                anime.fetch()

                writer.write(
                    {
                        'id': aid,
                        'entity': 'anime',
                        'payload': json.dumps(anime, cls=EnhancedJSONEncoder)
                    }
                )
            except Exception as e:
                print(f'Error parsing anime {aid}: {e}')
                time.sleep(4)
                continue


def parse_manga(filename: str):
    mal = MAL()
    base_dir = opyls.basedir('tmp')

    ids = opyls.load_json(base_dir / filename)

    with jsonlines.open(base_dir / (filename + 'l'), mode='w') as writer:
        for mid in tqdm(ids, desc="Parsing manga"):
            try:
                manga = mal.get_manga(mid)
                manga.fetch()

                writer.write(
                    {
                        'id': mid,
                        'entity': 'manga',
                        'payload': json.dumps(manga, cls=EnhancedJSONEncoder)
                    }
                )
            except Exception as e:
                print(f'Error parsing manga {mid}: {e}')
                time.sleep(4)
                continue


def parse():
    load_dotenv()
    base_dir = opyls.basedir('tmp', mkdir=True)
    config_dir = opyls.basedir('config')

    prod_env = os.environ.get('PROD')

    if os.environ.get('CHUNK_SIZE'):
        chunk_size = int(os.environ.get('CHUNK_SIZE'), 10)
    else:
        chunk_size = CHUNK_SIZE

    prod = prod_env == 'true'

    db_config = opyls.load_ini(config_dir / 'database.ini', 'postgresql')

    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**db_config) as conn:
            print('Connected to the PostgreSQL server.')
            with conn.cursor() as cur:
                cur.execute(build_query('anime', prod))
                print("The number of anime: ", cur.rowcount)

                anime_ids = [a[0] for a in cur.fetchall()]
                opyls.json_dump(base_dir / 'anime.json', anime_ids)

                cur.execute(build_query('manga', prod))
                print("The number of manga: ", cur.rowcount)

                manga_ids = [m[0] for m in cur.fetchall()]
                opyls.json_dump(base_dir / 'manga.json', manga_ids)
    except (psycopg2.DatabaseError, Exception) as error:
        print("PostgreSQL: ", error)
        raise error

    parse_anime('anime.json')
    # parse_manga('manga.json') # Manga needs to be fixed on a library level, skip for now

    chunk = []

    with psycopg2.connect(**db_config) as conn:
        with jsonlines.open(base_dir / 'anime.jsonl') as reader:
            for anime in tqdm(reader, desc="Inserting anime"):
                chunk.append(anime)

                if len(chunk) >= chunk_size:
                    insert_into_db(conn, chunk)
                    chunk = []

            if len(chunk) > 0:
                insert_into_db(conn, chunk)


if __name__ == "__main__":
    parse()
