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


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Introduction
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Create a temporary table with a list of highest contract salaries signed by all female employees who have worked in the company.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS f_highest_salaries AS
        SELECT
            e.emp_no,
        	MAX(s.salary) AS f_highest_salary
        FROM
        	employees e
        	INNER JOIN
        	salaries s
        		ON e.emp_no = s.emp_no
        			AND e.gender = 'F'
        GROUP BY
        	e.emp_no;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Retrieve data from the temporary table.
    """)
    return


@app.cell
def _(engine: Engine, f_highest_salaries):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            f_highest_salaries;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Get data for `emp_no` 10010 and below from the `f_highest_salaries` temporary table.
    """)
    return


@app.cell
def _(engine: Engine, f_highest_salaries):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
        	f_highest_salaries
        WHERE
        	emp_no <= 10010;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Drop the `f_highest_salaries` temporary table.
    """)
    return


@app.cell
def _(engine: Engine, f_highest_salaries):
    _df = mo.sql(
        f"""
        DROP TABLE IF EXISTS f_highest_salaries;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Store the highest contract salaries of all male employees in a temporary table called `male_max_salaries`.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS male_max_salaries AS 
        SELECT
        	e.emp_no,
            MAX(s.salary) AS max_salary
        FROM
        	employees e
        	INNER JOIN
        	salaries s
        		ON e.emp_no = s.emp_no
        			AND e.gender = 'M'
        GROUP BY
        	e.emp_no;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Retrieve data from the temporary table.
    """)
    return


@app.cell
def _(engine: Engine, male_max_salaries):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
        	male_max_salaries;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Drop the `male_max_salaries` table.
    """)
    return


@app.cell
def _(male_max_salaries):
    _df = mo.sql(
        f"""
        DROP TABLE IF EXISTS male_max_salaries;
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Replace a temporary table with a CTE.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    **This section is specific to MySQL.**

    - Although this notebook uses PostgreSQL, the original course is based on MySQL, which has a specific limitation where temporary tables are locked for use and can be invoked only once, otherwise an `ERROR 1137: Can't reopen table: temp_table` error occurs. The query processor cannot open multiple references to the same temporary table simultaneously within a single statement.
    - Therefore, in MySQL, they cannot be used in **self joins, `UNION`** or **`UNION ALL`** operators.

    **Workaround**

    - Use a CTE to define the query that would have been used by the temporary table.

    **Caveat**

    - A temporary table is a snapshot of data at the moment that it was created. A CTE is re-evaluated each time a query is run. Therefore, **the result may be different if the source tables have changed in the interim.**
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Create a temporary table with a list of highest contract salaries signed by all female employees who have worked in the company. Limit the list to 10 records.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS f_highest_salaries_limited AS
        SELECT
            e.emp_no,
        	MAX(s.salary) AS f_highest_salary
        FROM
        	employees e
        	INNER JOIN
        	salaries s
        		ON e.emp_no = s.emp_no
        			AND e.gender = 'F'
        GROUP BY
        	e.emp_no
        LIMIT 10;
        """,
        engine=engine
    )
    return


@app.cell
def _(engine: Engine, f_highest_salaries_limited):
    _df = mo.sql(
        f"""
        SELECT * FROM f_highest_salaries_limited;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Temporary tables with self joins in MySQL
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Self join which, in MySQL, would return `ERROR 1137: Can't reopen table`, but works on PostgreSQL.
    """)
    return


@app.cell
def _(engine: Engine, f_highest_salaries_limited):
    _df = mo.sql(
        f"""
        SELECT
            t1.emp_no AS t1_emp_no,
            t1.f_highest_salary AS t1_f_highest_salary,
            t2.emp_no AS t2_emp_no,
            t2.f_highest_salary AS t2_f_highest_salary
        FROM
            f_highest_salaries_limited t1
            JOIN
            f_highest_salaries_limited t2
            	ON t1.emp_no = t2.emp_no;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### **MySQL Workaround:** Self join using a CTE.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        WITH
            cte AS
            (
            SELECT
                e.emp_no,
            	MAX(s.salary) AS f_highest_salary
            FROM
            	employees e
            	INNER JOIN
            	salaries s
            		ON e.emp_no = s.emp_no
            			AND e.gender = 'F'
            GROUP BY
            	e.emp_no
            LIMIT 10
            )

        SELECT
            t1.emp_no AS t1_emp_no,
            t1.f_highest_salary AS t1_f_highest_salary,
            t2.emp_no AS t2_emp_no,
            t2.f_highest_salary AS t2_f_highest_salary
        FROM
            cte t1
            JOIN
            cte t2
            	ON t1.emp_no = t2.emp_no;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Temporary tables with `UNION` in MySQL
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### `UNION` which, in MySQL, would return `ERROR 1137: Can't reopen table`, but works on PostgreSQL.
    """)
    return


@app.cell
def _(engine: Engine, f_highest_salaries_limited):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            f_highest_salaries_limited
        UNION
        SELECT
            *
        FROM
            f_highest_salaries_limited;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### **MySQL Workaround:** `UNION` using a CTE.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        WITH
            cte AS
            (
            SELECT
                e.emp_no,
            	MAX(s.salary) AS f_highest_salary
            FROM
            	employees e
            	INNER JOIN
            	salaries s
            		ON e.emp_no = s.emp_no
            			AND e.gender = 'F'
            GROUP BY
            	e.emp_no
            LIMIT 10
            )

        SELECT
        	*
        FROM
        	cte
        UNION
        SELECT
        	*
        FROM
        	cte;
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
