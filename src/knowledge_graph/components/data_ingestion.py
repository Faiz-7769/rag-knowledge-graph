import os
import json
import sqlite3
import pandas as pd
from datetime import datetime
from src.knowledge_graph.utils.common import write_json
from src.knowledge_graph.logger.logging import logger
from src.knowledge_graph.exception.exception import KGException
import sys
import pypdf 
class DataIngestion:
    def __init__(self, config):
        self.config = config
        self.records = []
        self.counter = 1

    def _create_record(self, source_type, source_name, metadata, text):
        """Standardizes the record format."""
        # Skip empty text to reduce noise
        if not text or not text.strip():
            return None
            
        record = {
            "id": self.counter,
            "source_type": source_type,
            "source_name": source_name,
            "metadata": metadata,
            "text": text.strip(),
            "ingestion_timestamp": datetime.utcnow().isoformat()
        }
        self.counter += 1
        return record

    def _row_to_text(self, row, columns):
        """
        Converts a dataframe row to a semantic string.
        Before: "John 30 Engineer"
        After: "Name: John, Age: 30, Role: Engineer"
        """
        try:
            return ", ".join([f"{col}: {val}" for col, val in zip(columns, row) if pd.notna(val)])
        except Exception:
            return " ".join(map(str, row))

    # ---------- EMAIL INGESTION ----------
    def ingest_emails(self):
        logger.info("Starting Email Ingestion...")
        try:
            files = [f for f in os.listdir(self.config.email_dir) if f.endswith(".txt")]
            
            for file in files:
                path = os.path.join(self.config.email_dir, file)
                try:
                    with open(path, "r", encoding="utf-8", errors="replace") as f:
                        content = f.read()

                    # Split header and body (assuming standard double newline separation)
                    parts = content.split("\n\n", 1)
                    header_block = parts[0] if len(parts) > 0 else ""
                    body_text = parts[1] if len(parts) > 1 else ""
                    
                    # Parse headers safely
                    headers = {}
                    for line in header_block.splitlines():
                        if ":" in line:
                            key, value = line.split(":", 1)
                            headers[key.strip().lower()] = value.strip()

                    # Fallback if body is empty but content exists
                    if not body_text and not headers:
                        body_text = content

                    record = self._create_record(
                        source_type="email",
                        source_name=file,
                        metadata={
                            "from": headers.get("from"),
                            "to": headers.get("to"),
                            "date": headers.get("date"),
                            "subject": headers.get("subject")
                        },
                        text=body_text
                    )
                    if record: self.records.append(record)

                except Exception as e:
                    logger.warning(f"Failed to process email {file}: {e}")

        except Exception as e:
            logger.error(f"Critical error in email ingestion: {e}")

    # ---------- PDF INGESTION ----------
    def ingest_pdfs(self):
        logger.info("Starting PDF Ingestion...")
        try:
            files = [f for f in os.listdir(self.config.pdf_dir) if f.endswith(".pdf")]
            
            for file in files:
                path = os.path.join(self.config.pdf_dir, file)
                try:
                    text_content = []
                    reader = pypdf.PdfReader(path)
                    
                    for page in reader.pages:
                        extracted = page.extract_text()
                        if extracted:
                            text_content.append(extracted)
                    
                    full_text = "\n".join(text_content)

                    record = self._create_record(
                        source_type="pdf",
                        source_name=file,
                        metadata={"pages": len(reader.pages)},
                        text=full_text
                    )
                    if record: self.records.append(record)

                except Exception as e:
                    logger.warning(f"Failed to process PDF {file}: {e}")

        except Exception as e:
            logger.error(f"Critical error in PDF ingestion: {e}")

    # ---------- CSV INGESTION ----------
    def ingest_csvs(self):
        logger.info("Starting CSV Ingestion...")
        try:
            files = [f for f in os.listdir(self.config.csv_dir) if f.endswith(".csv")]
            
            for file in files:
                path = os.path.join(self.config.csv_dir, file)
                try:
                    # Use chunksize to handle large CSVs without memory crash
                    chunk_iterator = pd.read_csv(path, chunksize=1000)
                    
                    for chunk in chunk_iterator:
                        columns = list(chunk.columns)
                        for row in chunk.values:
                            text = self._row_to_text(row, columns)
                            
                            record = self._create_record(
                                source_type="csv",
                                source_name=file,
                                metadata={"columns": columns},
                                text=text
                            )
                            if record: self.records.append(record)
                            
                except Exception as e:
                    logger.warning(f"Failed to process CSV {file}: {e}")

        except Exception as e:
            logger.error(f"Critical error in CSV ingestion: {e}")

    # ---------- DATABASE INGESTION ----------
    def ingest_db(self):
        logger.info("Starting Database Ingestion...")
        conn = None
        try:
            conn = sqlite3.connect(self.config.db_path)
            cursor = conn.cursor()

            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            for (table_name,) in tables:
                try:
                    # Read in chunks using pandas
                    # 'chunksize' returns a generator of DataFrames
                    for chunk in pd.read_sql_query(f"SELECT * FROM {table_name}", conn, chunksize=1000):
                        columns = list(chunk.columns)
                        
                        for row in chunk.values:
                            text = self._row_to_text(row, columns)
                            
                            record = self._create_record(
                                source_type="database",
                                source_name=table_name,
                                metadata={"columns": columns, "db_source": self.config.db_path},
                                text=text
                            )
                            if record: self.records.append(record)
                            
                except Exception as e:
                    logger.warning(f"Failed to ingest table {table_name}: {e}")

        except Exception as e:
            logger.error(f"Database connection error: {e}")
        finally:
            if conn: conn.close()

    # ---------- MAIN PIPELINE ----------
    def ingest(self):
        try:
            logger.info(f">>> Ingestion Started at {datetime.now()}")
            
            self.ingest_emails()
            self.ingest_pdfs()
            self.ingest_csvs()
            self.ingest_db()

            write_json(self.config.output_json, self.records)
            
            logger.info(f"<<< Ingestion Completed. Total Records: {len(self.records)}")
            
        except Exception as e:
            raise KGException(e, sys)