# file: kipi_scraper/pipelines.py

import psycopg2
import json
from dotenv import load_dotenv

load_dotenv()

class PostgresPipeline:
    def open_spider(self, spider):

        spider.logger.info(f"Connecting to database: {os.getenv('PGDATABASE', 'kipi')}")

        self.conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            database=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD"),
            port=os.getenv("PGPORT"),
        )
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        print(f"[DEBUG] Processing item: {item.get('id') or item.get('document_id')}")

        doc_id = item.get('id') or item.get('document_id') or str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc)

        print(f"[PIPELINE] Processing item: {doc_id}")

        try:
            # Check for the unique 'file_bytes' field first
            if item.get("file_bytes"):
                print(f"[PIPELINE] Inserting into scraper_blob_store → document_id: {item['document_id']}")
                self.cursor.execute("""
                    INSERT INTO scraper_blob_store (
                        id, timestamp, file_content_type,
                        source_file, document_id
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (document_id) DO NOTHING
                """, (
                    str(doc_id),
                    now,
                    item['file_content_type'],
                    psycopg2.Binary(item['file_bytes']),
                    item['document_id']
                ))

            # check for the main page item
            elif 'name' in item and 'data' in item:
                print(f"[PIPELINE] Inserting into documents → ID: {doc_id}")
                self.cursor.execute("""
                    INSERT INTO documents (
                        id, a2_url, scraper, version, name,
                        timestamp, data, ingested_at, path
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (id) DO NOTHING
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

            else:
                print(f"[PIPELINE] SKIPPED: Missing required fields → {item}")

            self.conn.commit()

        except Exception as e:
            spider.logger.error(f"[DB ERROR] Item ID {doc_id} → {e}")
            self.conn.rollback()

        return item