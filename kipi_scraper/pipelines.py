# file: kipi_scraper/pipelines.py

import psycopg2
import uuid
import datetime
import os
import json
from dotenv import load_dotenv

load_dotenv()

class PostgresPipeline:
    def open_spider(self, spider):
        self.conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            database=os.getenv("PGDATABASE", "kipi"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "postgres"),
            port=os.getenv("PGPORT", 5432),
        )
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        doc_id = item.get('id', uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc)

        try:
            self.cursor.execute("""
                INSERT INTO documents (
                    id, url, scraper, version, name,
                    timestamp, data, ingested_at, path
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            """, (
                str(doc_id),
                item['url'],
                item['scraper'],
                item['version'],
                item['name'],
                item['timestamp'],
                json.dumps(item['data']),
                item['ingested_at'],
                item['path']
            ))

            if item.get("file_bytes"):
                self.cursor.execute("""
                    INSERT INTO scraper_blob_store (
                        id, timestamp, file_content_type,
                        source_file, document_id
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    str(doc_id),
                    now,
                    item['file_content_type'],
                    psycopg2.Binary(item['file_bytes']),
                    str(doc_id)
                ))

            self.conn.commit()

        except Exception as e:
            spider.logger.error(f"[DB ERROR] Item ID {doc_id} → {e}")
            self.conn.rollback()

        return item
