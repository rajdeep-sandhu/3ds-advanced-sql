import marimo

__generated_with = "0.22.4"
app = marimo.App(width="full", app_title="02_cte")

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
    # 02. Common Table Expressions (CTE)
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
    ### Get the number of salary contracts signed by female employees that have been valued above the all-time average contract salary value.
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
            	AVG(salary) AS avg_salary
        	FROM
            	salaries
            )

        SELECT
            COUNT(e.emp_no) AS f_salaries_above_avg
        FROM
            employees e
            INNER JOIN
            salaries s
            	ON e.emp_no = s.emp_no
        		AND e.gender = 'F'
        WHERE
            s.salary > (SELECT avg_salary FROM cte)
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Get the number of salary contracts signed by female employees that have been valued above the all-time average contract salary value, the total contracts signed by women, and the total contracts signed overall.

    NB `CROSS JOIN` is cheap for scalar values.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Using inner and cross join. Gender condition in `CASE WHEN`.

    NB `total_avg_salary` is calculated from `cte.avg_salary`, but `s.salary` can also be used, as it is not filtered by gender.
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
            	AVG(salary) AS avg_salary
        	FROM
            	salaries
            )

        SELECT
        	SUM(CASE
            		WHEN e.gender = 'F' AND s.salary > cte.avg_salary
            		THEN 1 ELSE 0
            	END
            ) AS f_salaries_above_avg,
            SUM(CASE WHEN e.gender = 'F' THEN 1 ELSE 0 END) AS total_f_salary_contracts,
            COUNT(s.salary) AS total_salary_contracts,
            ROUND(AVG(cte.avg_salary)) AS total_avg_salary -- Aggregation here is only to satisfy postgres syntax.
        FROM
        	employees e
            INNER JOIN
            salaries s
            	ON e.emp_no = s.emp_no
        	CROSS JOIN
        		cte;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Using inner and cross join. Gender condition in `INNER JOIN`.

    NB `total_avg_salary` is calculated from `cte.avg_salary`, as `s.salary` is already filtered by gender.
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
            	AVG(salary) AS avg_salary
        	FROM
            	salaries
            ),

            total_contracts AS
            (
            SELECT
            	COUNT(*) AS total_salary_contracts
            FROM
            	salaries
            )

        SELECT
        	SUM(CASE
            		WHEN s.salary > cte.avg_salary
            		THEN 1 ELSE 0
            	END
            ) AS f_salaries_above_avg,
            COUNT(s.salary) AS total_f_salary_contracts,
            (SELECT total_salary_contracts FROM total_contracts) AS total_salary_contracts,
            ROUND(AVG(cte.avg_salary)) AS total_avg_salary -- Aggregation here is only to satisfy postgres syntax.
        FROM
        	employees e
            INNER JOIN
            salaries s
            	ON e.emp_no = s.emp_no
            	AND e.gender = 'F'
        	CROSS JOIN
        		cte;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Using a scalar subquery, without CTE.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
        	SUM(
            	CASE
            		WHEN s.salary > (SELECT AVG(salary) FROM salaries)
            		AND e.gender = 'F'
            		THEN 1 ELSE 0
            	END
            ) AS f_salaries_above_avg,
            SUM(
            	CASE
            		WHEN e.gender = 'F'
            		THEN 1 ELSE 0
            	END
            ) AS total_f_salary_contracts,
            COUNT(salary) AS total_salary_contracts,
            ROUND(AVG(salary)) AS total_avg_salary
        FROM
            employees e
        	INNER JOIN
        	salaries s
        		ON e.emp_no = s.emp_no;
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
