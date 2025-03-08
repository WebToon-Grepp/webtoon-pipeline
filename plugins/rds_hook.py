from airflow.hooks.base import BaseHook

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.subprocess import SubprocessHook

class RDSHook():
    
    def __init__(
        self, *args, query: str | None = None, rds_conn_id: str = "postgres_default", **kwargs
    ) -> None:
        self.query = query
        self.rds_conn_id = rds_conn_id
        
    def copy_file(self, table, tmp_file, tmp_col):
        rds_hook = SubprocessHook()
        conn = BaseHook.get_connection(self.rds_conn_id)
        try:
            query = f"\copy {table} ({tmp_col}) from '{tmp_file}' with delimiter ',' csv header;"
            rds_hook.run_command(
                command=["bash", "-c", f"PGPASSWORD=\"{conn.password}\" psql --host {conn.host} --username {conn.login} --port {conn.port} --dbname {conn.schema} -c \"{query}\""]
            )
            print(f"Successfully copied data from {tmp_file} to table {table}.")
        except Exception as e:
            print(f"Failed to copy data from {tmp_file} to table {table}: {e}")
            raise Exception("COPY operation to error.")

    def execute_query(self, query=None, autocommit=True):
        if not query:
            if not self.query:
                raise Exception("Query not provided and no default query found.")
            query = self.query
        print(query)

        rds_hook = PostgresHook(postgres_conn_id=self.rds_conn_id)
        conn = rds_hook.get_conn()
        conn.autocommit = autocommit
        cursor = conn.cursor()
        
        try:
            cursor.execute(query)
            conn.commit()
            print("Query executed successfully and changes committed.")
        except Exception as e:
            conn.rollback()
            print(f"Error executing query: {e}")
            raise Exception("Changes rolled back due to error.")
        finally:
            cursor.close()
            conn.close()
            print("Cursor and connection closed.")