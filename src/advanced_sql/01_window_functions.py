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
    ## `ROW_NUMBER()`
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Rank each employee's salary in descending order.
    """)
    return


@app.cell
def _(engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
            emp_no,
            salary,
            ROW_NUMBER()
            	OVER(
            		PARTITION BY emp_no
            		ORDER BY salary DESC
        		) AS row_num
        FROM
            salaries;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Rank all managers by employee number in ascending order.
    """)
    return


@app.cell
def _(dept_manager, engine: Engine):
    _df = mo.sql(
        f"""
        SELECT
            emp_no,
            dept_no,
            ROW_NUMBER() OVER(ORDER BY emp_no) AS row_num
        FROM
            dept_manager;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### List all employees partitioned by first name, sequentially numbered by last name order in each partition.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    List all employees in the `employees` table and assign a sequential number for each employee number. Partition by first name and order by last name in ascending order (for each partition).
    """)
    return


@app.cell
def _(employees, engine: Engine):
    _df = mo.sql(
        f"""
        SELECT
            emp_no,
            first_name,
            last_name,
            ROW_NUMBER() OVER(PARTITION BY first_name ORDER BY last_name) AS row_num
        FROM
            employees;
        """,
        engine=engine
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
