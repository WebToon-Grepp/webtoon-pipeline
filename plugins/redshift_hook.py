from airflow.providers.postgres.hooks.postgres import PostgresHook

class RedshiftHook():
    
    def __init__(
        self, *args, query: str | None = None, redshift_conn_id: str = "redshift_default", **kwargs
    ) -> None:
        self.query = query
        self.redshift_conn_id = redshift_conn_id

    def execute_query(self, query=None, autocommit=True):
        if not query:
            if not self.query:
                raise Exception("Query not provided and no default query found.")
            query = self.query
        print(query)

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = redshift_hook.get_conn()
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