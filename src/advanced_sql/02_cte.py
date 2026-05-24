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

    - Shows both `SUM()` and `COUNT()` methods to get female employee salaries above average salary.
    - NB `total_avg_salary` is calculated from `cte.avg_salary`, as `s.salary` is already filtered by gender.
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
            ) AS f_salaries_above_avg_using_sum,
            	COUNT(CASE
            		WHEN s.salary > cte.avg_salary
            		THEN s.salary ELSE NULL
            	END
            ) AS f_salaries_above_avg_using_count,
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


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Get the sumber of salary contracts signed by male employees with a salary value below or equal to the all-time average salary.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Using a CTE and `SUM()` as well as `COUNT()` in the SELECT statement.
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
            	AVG(salary) as avg_salary
            FROM
            	salaries
            )

        SELECT
            SUM(CASE
            		WHEN e.gender = 'M'
            			AND s.salary <= cte.avg_salary
            		THEN 1 ELSE 0
            	END
            ) AS m_salaries_below_avg_using_sum,
            COUNT(CASE
            		WHEN e.gender = 'M'
            			AND s.salary <= cte.avg_salary
            		THEN s.salary ELSE NULL
            	END
            ) AS m_salaries_below_avg_using_count,
            SUM(CASE
            		WHEN e.gender = 'M'
            		THEN 1 ELSE 0
            	END
            ) AS total_m_salary_contracts,
            COUNT(s.salary) AS total_salary_contracts,
            ROUND(AVG(s.salary)) AS total_avg_salary
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
    #### Using a joined subquery and `SUM()` as well as `COUNT()` in the SELECT statement.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
            SUM(CASE
            		WHEN e.gender = 'M'
            			AND s.salary <= a.avg_salary
            		THEN 1 ELSE 0
            	END
            ) AS m_salaries_below_avg_using_sum,
            COUNT(CASE
            		WHEN e.gender = 'M'
            			AND s.salary <= a.avg_salary
            		THEN s.salary ELSE NULL
            	END
            ) AS m_salaries_below_avg_using_count,
            SUM(CASE
            		WHEN e.gender = 'M'
            		THEN 1 ELSE 0
            	END
            ) AS total_m_salary_contracts,
            COUNT(s.salary) AS total_salary_contracts,
            ROUND(AVG(s.salary)) AS total_avg_salary
        FROM
        	employees e
        	INNER JOIN
        	salaries s
        		ON e.emp_no = s.emp_no
        	CROSS JOIN
        	(
            SELECT
            	AVG(salary) as avg_salary 
            FROM
            	salaries
            ) AS a;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Considering the salary contracts signed by female employees in the company, how many have been signed for a value below the average? Store the output in a column named no_f_salaries_below_avg. In a second column named no_of_f_salary_contracts, provide the total number of contracts signed by women.
    Use the salary column from the salaries table and the gender column from the employees table. Match the two tables on the employee number column (emp_no).
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Get the number of salary contracts signed by female employees that have been valued below the average contract salary value as `no_f_salaries_below_avg`, and the total contracts signed by women as `no_of_f_salary_contracts`.

    - Use a CTE and aggregate functions.
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
        	SUM(
            	CASE	
            		WHEN s.salary < cte.avg_salary
            			AND e.gender = 'F'
            		THEN 1 ELSE 0
            	END
            ) AS no_f_salaries_below_avg,
            SUM(
            	CASE	
            		WHEN e.gender = 'F'
            		THEN 1 ELSE 0
            	END
            ) AS no_of_f_salary_contracts
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
    ### Get the number of salary contracts signed by male employees that have been valued above the average contract salary value as `no_m_salaries_above_avg`, and the total contracts signed by men as `no_of_m_salary_contracts`.

    - Use a CTE and aggregate functions.
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
        	SUM(
            	CASE	
            		WHEN s.salary > cte.avg_salary
            			AND e.gender = 'M'
            		THEN 1 ELSE 0
            	END
            ) AS no_m_salaries_above_avg,
            SUM(
            	CASE	
            		WHEN e.gender = 'M'
            		THEN 1 ELSE 0
            	END
            ) AS no_of_m_salary_contracts,
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
    ## `WITH` clause with multiple subclauses.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Get the number of female employees whose highest contract salary values were higher than the company average.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        WITH
            avg_salary AS
            (
            SELECT
            	AVG(salary) AS avg_salary
            FROM
            	salaries
            ),

        	max_f_salaries AS
        	(
            SELECT
                e.emp_no,
                MAX(s.salary) AS max_salary
            FROM
            	employees e
            	INNER JOIN
            	salaries s
            		ON e.emp_no = s.emp_no
            			AND e.gender = 'F'
            GROUP BY
            	e.emp_no
            )

        SELECT
        	SUM(
            	CASE
            		WHEN max_f_salaries.max_salary > avg_salary.avg_salary
            		THEN 1 ELSE 0
            	END
            ) AS f_highest_salaries_above_avg,
            COUNT(max_f_salaries.emp_no) AS total_f_highest_salaries,
            CONCAT(
                ROUND(
            		SUM(CASE WHEN max_f_salaries.max_salary > avg_salary.avg_salary
                		THEN 1 ELSE 0 END)::numeric
                	/ COUNT(max_f_salaries.emp_no) * 100, 2),
            	'%') AS percentage
        FROM
        	max_f_salaries
        	CROSS JOIN
        	avg_salary;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Get the number of male employees whose highest contract salary values were lower than the company average.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Using two CTE and a `SUM()` function in the SELECT statement.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        WITH
            avg_salary AS
            (
            SELECT
            	AVG(salary) AS avg_salary
            FROM
            	salaries
            ),

        	max_m_salaries AS
        	(
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
            	e.emp_no
            )

        SELECT
        	SUM(
            	CASE
            		WHEN max_m_salaries.max_salary < avg_salary.avg_salary
            		THEN 1 ELSE 0
            	END
            ) AS m_highest_salaries_above_avg,
            COUNT(max_m_salaries.emp_no) AS total_m_highest_salaries
        FROM
        	max_m_salaries
        	CROSS JOIN
        	avg_salary;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Using two CTE and a `COUNT()` function in the SELECT statement.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        WITH
            avg_salary AS
            (
            SELECT
            	AVG(salary) AS avg_salary
            FROM
            	salaries
            ),

        	max_m_salaries AS
        	(
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
            	e.emp_no
            )

        SELECT
        	COUNT(
            	CASE
            		WHEN max_m_salaries.max_salary < avg_salary.avg_salary
            		THEN max_m_salaries.max_salary ELSE NULL
            	END
            ) AS m_highest_salaries_above_avg,
            COUNT(max_m_salaries.emp_no) AS total_m_highest_salaries
        FROM
        	max_m_salaries
        	CROSS JOIN
        	avg_salary;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Get the highest contract salaries of all employees hired in 2000 or later.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        WITH
        	max_salary AS
            (
            SELECT
            	emp_no,
                MAX(salary) as max_salary
            FROM
            	salaries
            GROUP BY
            	emp_no
            )

        SELECT
        	e.emp_no,
            e.hire_date,
        	ms.max_salary AS highest_salary
        FROM
        	employees e
        	INNER JOIN
        	max_salary ms
        		ON e.emp_no = ms.emp_no
        		AND e.hire_date >= '2000-01-01'
        ORDER BY
        	e.emp_no;

        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Get the number of contracts signed by female employees below the company average (`no_f_salaries_below_avg`) and the total number of contracts signed by all employees (`total_no_of_salary_contracts`) using two CTEs.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        WITH
            avg_salary AS
            (
            SELECT
                AVG(salary) AS avg_salary
            FROM
            	salaries
            ),
            tot_contracts AS
            (
            SELECT
            	COUNT(salary) AS tot_contracts
            FROM
            	salaries
            )

        SELECT
        	SUM(
            	CASE
            		WHEN s.salary < a.avg_salary
            		THEN 1 ELSE 0
            	END
            ) AS no_f_salaries_below_avg,
            (SELECT tot_contracts FROM tot_contracts) AS total_no_of_salary_contracts
        FROM
        	employees e
        	INNER JOIN
        	salaries s
            	ON e.emp_no = s.emp_no
            	AND e.gender = 'F'
        	CROSS JOIN
        	avg_salary a;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Get the number of contracts signed by male employees above the company average (`no_m_salaries_above_avg`) and the total number of contracts signed by all employees (`total_no_of_salary_contracts`) using two CTEs.
    """)
    return


@app.cell
def _(employees, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        WITH
            avg_salary AS
            (
            SELECT
                AVG(salary) AS avg_salary
            FROM
            	salaries
            ),
            tot_contracts AS
            (
            SELECT
            	COUNT(salary) AS tot_contracts
            FROM
            	salaries
            )

        SELECT
        	SUM(
            	CASE
            		WHEN s.salary > a.avg_salary
            		THEN 1 ELSE 0
            	END
            ) AS no_m_salaries_above_avg,
            (SELECT tot_contracts FROM tot_contracts) AS total_no_of_salary_contracts
        FROM
        	employees e
        	INNER JOIN
        	salaries s
            	ON e.emp_no = s.emp_no
            	AND e.gender = 'M'
        	CROSS JOIN
        	avg_salary a;
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
