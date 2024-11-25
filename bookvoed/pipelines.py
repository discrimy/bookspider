# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg
import os
import time
class BookvoedPipeline:
    def __init__(self):
        self.connection = psycopg.connect(os.getenv('CONNECTION_STRING'))
        cursor = self.connection.cursor()
        cursor.execute('select 1')
        test_result = cursor.fetchall()
        if test_result != [(1,)]:
            raise Exception('Error while initilizing DB connection')

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id serial PRIMARY KEY,
                name VARCHAR(265) NOT NULL,
                author VARCHAR(256),
                price VARCHAR(256)             
            )
        """)
        self.connection.commit()

    def process_item(self, item, spider):
        time.sleep(1)
        if 'error' in item:
            print(f'Error item recieved, skip: {item}')
            return item
        
        cursor = self.connection.cursor()
        cursor.execute('INSERT INTO items (name, author, price) VALUES (%s, %s, %s)', [item['name'], item['author'], item['price']])
        self.connection.commit()

        return item
