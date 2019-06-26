import asyncio
import logging
import random
from collections import defaultdict

from easydb import EasydbClient, MultipleElementFields, TransactionOperation, FilterQuery

logger = logging.getLogger('traffic_gen')
client = EasydbClient('http://localhost:8000')

word_file = "/usr/share/dict/words"
WORDS = open(word_file).read().splitlines()
BUCKETS = WORDS[:100]

CREATE_SPACE_MILLIS_INTERVAL = 200
DELETE_SPACE_MILLIS_INTERVAL = 1000 * 10
GET_SPACE_MILLIS_INTERVAL = 50

CREATE_BUCKET_MILLIS_INTERVAL = 200
DELETE_BUCKET_MILLIS_INTERVAL = 1000 * 100

ADD_ELEMENT_MILLIS_INTERVAL = 50
DELETE_ELEMENT_MILLIS_INTERVAL = 2000
GET_ELEMENT_MILLIS_INTERVAL = 50

PERFORM_TRANSACTION_MILLIS_INTERVAL = 200

FILTER_ELEMENT_MILLIS_INTERVAL = 200

spaces = {}


async def create_space():
    while True:
        global spaces
        try:
            space_name = await client.create_space()
            spaces[space_name] = defaultdict(dict)
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(CREATE_SPACE_MILLIS_INTERVAL / 1000)


async def remove_space():
    while True:
        global spaces
        try:
            if spaces:
                space_name = spaces.popitem()[0]
                await client.delete_space(space_name)
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(DELETE_SPACE_MILLIS_INTERVAL / 1000)


async def get_space():
    while True:
        global spaces
        try:
            if spaces:
                space_name = random.choice(list(spaces))
                await client.get_space(space_name)
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(GET_SPACE_MILLIS_INTERVAL / 1000)


async def create_bucket():
    while True:
        global spaces
        try:
            if spaces:
                space_name = random.choice(list(spaces))
                bucket_name = random.choice(list(BUCKETS))
                if bucket_name not in spaces[space_name].keys():
                    spaces[space_name][bucket_name] = {}
                    await client.create_bucket(space_name, bucket_name)
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(CREATE_BUCKET_MILLIS_INTERVAL / 1000)


async def delete_bucket():
    while True:
        global spaces
        try:
            if spaces:
                space_name = random.choice(list(spaces))
                if spaces[space_name]:
                    bucket_name = spaces[space_name].popitem()[0]
                    await client.delete_bucket(space_name, bucket_name)
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(DELETE_BUCKET_MILLIS_INTERVAL / 1000)


async def add_element():
    while True:
        global spaces
        try:
            if spaces:
                space_name = random.choice(list(spaces))
                if spaces[space_name]:
                    bucket_name = random.choice(list(spaces[space_name]))

                    fields = MultipleElementFields() \
                        .add_field("name", random.choice("Jurek")) \
                        .add_field("age", random.choice("22")) \
                        .add_field("city", random.choice("Warsaw")) \
                        .add_field("country", random.choice("Poland"))
                    element = await client.add_element(space_name, bucket_name, fields)

                    spaces[space_name][bucket_name][element.identifier] = element

        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(ADD_ELEMENT_MILLIS_INTERVAL / 1000)


async def delete_element():
    while True:
        global spaces
        try:
            if spaces:
                space_name = random.choice(list(spaces))
                if spaces[space_name]:
                    bucket_name = random.choice(list(spaces[space_name]))
                    if spaces[space_name][bucket_name]:
                        element_id = spaces[space_name][bucket_name].popitem()[0]
                        await client.delete_element(space_name, bucket_name, element_id)
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(DELETE_ELEMENT_MILLIS_INTERVAL / 1000)


async def get_element():
    while True:
        global spaces
        try:
            if spaces:
                space_name = random.choice(list(spaces))
                if spaces[space_name]:
                    bucket_name = random.choice(list(spaces[space_name]))
                    if spaces[space_name][bucket_name]:
                        element_id = random.choice(list(spaces[space_name][bucket_name]))
                        await client.get_element(space_name, bucket_name, element_id)

        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(GET_ELEMENT_MILLIS_INTERVAL / 1000)


async def update_element():
    while True:
        global spaces
        try:
            if spaces:
                space_name = random.choice(list(spaces))
                if spaces[space_name]:
                    bucket_name = random.choice(list(spaces[space_name]))
                    if spaces[space_name][bucket_name]:
                        element_id = random.choice(list(spaces[space_name][bucket_name]))

                        fields = MultipleElementFields() \
                            .add_field("name", random.choice("Andrzej")) \
                            .add_field("age", random.choice("25")) \
                            .add_field("city", random.choice("Wrocław")) \
                            .add_field("country", random.choice("Poland"))

                        await client.update_element(space_name, bucket_name, element_id, fields)

        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(ADD_ELEMENT_MILLIS_INTERVAL / 1000)


async def perform_transaction():
    while True:
        global spaces
        try:
            if spaces:
                space_name = random.choice(list(spaces))
                if spaces[space_name]:
                    bucket_name = random.choice(list(spaces[space_name]))
                    if spaces[space_name][bucket_name]:
                        transaction = await client.begin_transaction(space_name)

                        read_element_id = random.choice(list(spaces[space_name][bucket_name]))
                        update_element_id = random.choice(list(spaces[space_name][bucket_name]))
                        delete_element_id = random.choice(list(spaces[space_name][bucket_name]))
                        read_operation = TransactionOperation('READ', bucket_name, read_element_id)
                        create_operation = TransactionOperation('CREATE', bucket_name,
                                                                fields=MultipleElementFields()
                                                                .add_field("transactionCreateField1", "value1") \
                                                                .add_field("transactionCreateField2", "value2") \
                                                                .add_field("transactionCreateField3", "value3") \
                                                                .add_field("transactionCreateField4", "value4"))
                        update_operation = TransactionOperation('UPDATE', bucket_name, update_element_id,
                                                                fields=MultipleElementFields()
                                                                .add_field("transactionUpdateField1", "value1") \
                                                                .add_field("transactionUpdateField2", "value2") \
                                                                .add_field("transactionUpdateField3", "value3") \
                                                                .add_field("transactionUpdateField4", "value4"))
                        delete_operation = TransactionOperation('DELETE', bucket_name, delete_element_id)

                        await client.add_operation(space_name, transaction.transaction_id, read_operation)
                        await client.add_operation(space_name, transaction.transaction_id, create_operation)
                        await client.add_operation(space_name, transaction.transaction_id, update_operation)
                        await client.add_operation(space_name, transaction.transaction_id, delete_operation)
                        await client.commit_transaction(space_name, transaction.transaction_id)
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(PERFORM_TRANSACTION_MILLIS_INTERVAL / 1000)


async def filter_elements():
    query_all_elements = """
        {
            elements(filter: {
                and: [
                        {
                            or: [
                                    {
                                        fieldsFilters: [
                                            {
                                                name: "name"
                                                value: "Andrzej"
                                            }
                                        ]
                                    },
                                    {
                                        fieldsFilters: [
                                            {
                                                name: "city"
                                                value: "Warsaw"
                                            }
                                        ]
                                    }
                                ]
                        }
                        {
                            or: [
                                    {
                                        fieldsFilters: [
                                            {
                                                name: "age"
                                                value: "25"
                                            }
                                        ]
                                    },
                                    {
                                        fieldsFilters: [
                                            {
                                                name: "country"
                                                value: "Italy"
                                            }
                                        ]
                                    }
                                ]
                        }
                    ]
            }) {
                id
                fields {
                    name
                    value
                }
            }
        }"""

    while True:
        global spaces
        try:
            if spaces:
                space_name = random.choice(list(spaces))
                if spaces[space_name]:
                    bucket_name = random.choice(list(spaces[space_name]))
                    result = await client.filter_elements_by_query(
                        query=FilterQuery(space_name, bucket_name, query=query_all_elements))
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(FILTER_ELEMENT_MILLIS_INTERVAL / 1000)


async def main():
    tasks = []
    tasks.append(asyncio.ensure_future(create_space()))
    tasks.append(asyncio.ensure_future(remove_space()))
    tasks.append(asyncio.ensure_future(get_space()))
    tasks.append(asyncio.ensure_future(create_bucket()))
    tasks.append(asyncio.ensure_future(delete_bucket()))
    tasks.append(asyncio.ensure_future(add_element()))
    tasks.append(asyncio.ensure_future(delete_element()))
    tasks.append(asyncio.ensure_future(get_element()))
    tasks.append(asyncio.ensure_future(update_element()))
    tasks.append(asyncio.ensure_future(perform_transaction()))
    # tasks.append(asyncio.ensure_future(filter_elements()))
    await asyncio.gather(*tasks)

loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
finally:
    loop.close()
