import asyncio
from src.db_clients.clients import get_db_connection
from src.db_clients.config import TablesConfig

tables = TablesConfig()

async def check_tables_info():
    def sync_check():
        result = {}
        try:
            conn = get_db_connection()
        except Exception as e:
            raise ConnectionError(f"Failed to connect to DB: {e}")

        try:
            cursor = conn.cursor()
            for table_name in tables.__dict__.values():
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    result[table_name] = f"Connection OK, rows: {count}"
                except Exception as e:
                    result[table_name] = f"Error accessing table: {e}"
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return result

    return await asyncio.to_thread(sync_check)
