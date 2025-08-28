# file: kipi_scraper/pipelines.py
import os
from pathlib import Path
import logging
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from kipi_scraper.db.session import SessionLocal
from kipi_scraper.db.models import Document, ScraperBlobStore
from kipi_scraper.items import DocumentItem

logger = logging.getLogger(__name__)

class KipiPipeline:
    def __init__(self):
        self.session = None

    def _ensure_session(self, spider):
        """Create a DB session if we don't have one yet."""
        if self.session is None:
            try:
                self.session = SessionLocal()
                spider.logger.info("SQLAlchemy DB session started (lazy).")
            except Exception as e:
                spider.logger.exception(f"Failed to create DB session: {e}")
                raise

    def open_spider(self, spider):
        # 
        # create it at spider open
        self._ensure_session(spider)

    def close_spider(self, spider):
        if not self.session:
            return
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
        finally:
            self.session.close()
            self.session = None
            spider.logger.info("SQLAlchemy DB session closed.")

    def process_item(self, item, spider):
        if not isinstance(item, DocumentItem):
            return item

        self._ensure_session(spider)

        try:
            data = item.get('data') or {}
            content_hash = data.get('content_hash')
            if not content_hash:
                spider.logger.error(f"Item missing content_hash: {item.get('name')}")
                return item

            #extract text from JSONB (documents.data ->> 'content_hash')
            existing_doc = self.session.execute(
                select(Document).where(
                    Document.data.op('->>')('content_hash') == content_hash
                )
            ).scalars().first()

            if existing_doc:
                spider.logger.info(f"DUPLICATE SKIPPED: {item.get('name')} (content_hash match)")
                if hasattr(spider, "crawler"):
                    spider.crawler.stats.inc_value("db/duplicate_items")
                return item

            # Build parent + blob (requires cascade="all, delete-orphan" on relationship)
            doc = Document(
                url=item["url"],
                name=item["name"],
                path=item["path"],
                scraper=item["scraper"],
                timestamp=item["timestamp"],
                version=item["version"],
                data=data,
            )
            doc.blob = ScraperBlobStore(
                file_content_type=item["file_content_type"],
                source_file=item["source_file"],
            )

            self.session.add(doc)
            try:
                self.session.commit()
            except IntegrityError:
                self.session.rollback()
                spider.logger.info(f"DUPLICATE SKIPPED (unique index): {item.get('name')}")
                if hasattr(spider, "crawler"):
                    spider.crawler.stats.inc_value("db/duplicate_items")
                return item

            spider.logger.info(f"DB OK: {doc.name} -> id={doc.id}")
            if hasattr(spider, "crawler"):
                spider.crawler.stats.inc_value("db/saved_items")

        except SQLAlchemyError as e:
            self.session.rollback()
            spider.logger.exception(
                f"SQLAlchemy insert failed: {e.__class__.__name__} | {getattr(e, 'orig', e)}"
            )
            if hasattr(spider, "crawler"):
                spider.crawler.stats.inc_value("db/save_errors")
        except Exception as e:
            # Make sure not to leave a half-used transaction open
            try:
                self.session.rollback()
            except Exception:
                pass
            spider.logger.exception(f"Unexpected pipeline error: {e}")

        return item






