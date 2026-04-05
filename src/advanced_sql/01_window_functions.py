import marimo

__generated_with = "0.22.4"
app = marimo.App(width="full", app_title="01_window_functions")

with app.setup:
    import marimo as mo
    import os
    import psycopg
    import sqlalchemy

    from pathlib import Path
    from sqlalchemy import Engine
    from advanced_sql.postgres_factory import PostgresFactory


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # 01. Window Functions
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Setup and Connection
    """)
    return


@app.cell
def _():
    # Connect to postgres
    factory: PostgresFactory = PostgresFactory()
    engine: Engine = factory.create_engine()
    return (engine,)


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        SELECT VERSION();
        """
    )
    return


@app.cell(hide_code=True)
def _(engine: Engine):
    _df = mo.sql(
        f"""
        SELECT * FROM information_schema.tables;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Create database from SQL file
    """)
    return


@app.cell
def _():
    def reset_database() -> None:
        """Reset the employee database."""
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        database = os.environ["POSTGRES_DB"]
        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = 5432

        conn = psycopg.connect(
            host=host, user=user, password=password, port=port, autocommit=True
        )
    
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS employees;")
        cur.execute("CREATE DATABASE employees;")
        cur.close()
        conn.close()

        print("'employees' database created.")


    def create_database(sql_file: Path, engine: Engine) -> None:
        """Create database using the supplied SQL file"""
        with open(sql_file, "r") as f:
            sql: str = f.read()

        with engine.raw_connection() as raw_conn:
            # with raw_conn.cursor() as cur:
            #     cur.execute(sql)
            # raw_conn.commit()
            cur = raw_conn.cursor()
            cur.execute(sql)
            cur.close()
            raw_conn.commit()

    return create_database, reset_database


@app.cell
def _(create_database, engine: Engine, reset_database):
    sql_file: Path = Path(__file__).parent / "employees.sql"
    reset_database()
    create_database(sql_file=sql_file, engine=engine)
    return


if __name__ == "__main__":
    app.run()
