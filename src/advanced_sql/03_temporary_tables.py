import marimo

__generated_with = "0.22.4"
app = marimo.App(width="full", app_title="03_temporary_tables")

with app.setup:
    import marimo as mo
    import os
    import psycopg
    import sqlalchemy

    from pathlib import Path
    from sqlalchemy import Engine, Connection
    from advanced_sql.postgres_factory import PostgresFactory


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # 03. Temporary Tables
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
    connection: Connection = engine.connect()
    return (engine,)


@app.cell
def _(engine: Engine):
    _df = mo.sql(
        f"""
        SELECT VERSION();
        """,
        engine=engine
    )
    return


@app.cell
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
    def reset_schema(engine: Engine) -> None:
        """
        Resets the public schema to a clean state.
        Equivalent to recreating the database for the purpose of this tutorial.
        """
        with engine.begin() as conn:
            conn.exec_driver_sql("DROP SCHEMA public CASCADE;")
            conn.exec_driver_sql("CREATE SCHEMA public;")

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

    return create_database, reset_schema


@app.cell
def _(create_database, engine: Engine, reset_schema):
    sql_file: Path = Path(__file__).parent / "employees.sql"
    reset_schema(engine=engine)
    create_database(sql_file=sql_file, engine=engine)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Database Tables

    - `salaries`: Salary contract values and their dates by employee.
    - `dept_emp_latest_date`: Start and finish dates for by employee. `9999-01-01` indicates that the employee is active.
    - `current_dept_emp`: Start and finish dates in current/latest department by employee number. `9999-01-01` indicates that the employee is active in the current department.
    - `employees`: Employee details, including hire date.
    - `dept_manager`: Employee numbers of managers with their current department, start and finish details. Includes past managers for each department.
    - `departments`: Department name by department number.
    - `dept_emp`: Dates when an employee was in a particular department.
    - `titles`: Job titles with start end finish dates by employee number.
    """)
    return


if __name__ == "__main__":
    app.run()
