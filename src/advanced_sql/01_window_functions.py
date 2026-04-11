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


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### List all department managers ranked by employee number in descending order.
    List department managers as `emp_no` and `dept_no` with row numbers assigned in descending order of `emp_no`.
    """)
    return


@app.cell
def _(dept_manager, engine: Engine):
    _df = mo.sql(
        f"""
        SELECT
            emp_no,
            dept_no,
            ROW_NUMBER() OVER(ORDER BY emp_no DESC) AS row_num
        FROM
            dept_manager;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### List all employees partititioned by last name and ranked by employee number
    List `emp_no`, `first_name` and `last_name` for all employees, partitioned by last name and assigned row numbers by employee number in ascending order.
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
            ROW_NUMBER() OVER(PARTITION BY last_name ORDER BY emp_no) AS row_num
        FROM
        	employees;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Multiple window functions in a `SELECT` statement
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### The output of the following query is messy
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.callout(mo.md
               ("""
               Query results in PostgreSQL will be different from the MySQL version.
               - This is because MySQL (especially with InnoDB) returns rows in **clustered index order** for columns without a specified order,
               which may appear as a **stable incidental order**.
               - However, PostgreSQL **does not** reliably preserve physical order and may use different scan strategies, e.g. heap scan, parallelism, etc.
               """), kind="info")
    return


@app.cell
def _(engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
            emp_no,
            salary,
            ROW_NUMBER() OVER() AS row_num1,
            ROW_NUMBER() OVER(PARTITION BY emp_no) AS row_num2,
            ROW_NUMBER() OVER(PARTITION BY emp_no ORDER BY salary DESC) AS row_num3,
            ROW_NUMBER() OVER(ORDER BY salary DESC) AS row_num4
        FROM
            salaries;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### To fix this:
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Add an `ORDER BY` clause in the outer query.
    """)
    return


@app.cell
def _(engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
            emp_no,
            salary,
            ROW_NUMBER() OVER() AS row_num1,
            ROW_NUMBER() OVER(PARTITION BY emp_no) AS row_num2,
            ROW_NUMBER() OVER(PARTITION BY emp_no ORDER BY salary DESC) AS row_num3,
            ROW_NUMBER() OVER(ORDER BY salary DESC) AS row_num4
        FROM
            salaries
        ORDER BY
            emp_no,
            salary;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Use only window specifications requiring identical partitions.
    """)
    return


@app.cell
def _(engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
            emp_no,
            salary,
            ROW_NUMBER() OVER(PARTITION BY emp_no) AS row_num2,
            ROW_NUMBER() OVER(PARTITION BY emp_no ORDER BY salary DESC) AS row_num3
        FROM
            salaries;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### List managers with overall row numbering and ranked salary per manager.

    List managers and their salaries using `dept_manager` and `salaries`. Add two window function columns.

    - A global row number column without a specific ordering.
    - A per manager rank by salary in ascending order (lowest  1).

    Return the results ordered by `row_num` and `salary` in ascending order.
    """)
    return


@app.cell
def _(dept_manager, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
            dm.emp_no,
            s.salary,
            ROW_NUMBER() OVER() AS row_num,
            ROW_NUMBER() OVER(PARTITION BY dm.emp_no ORDER BY s.salary ASC) AS salary_rank
        FROM
            dept_manager dm
        	INNER JOIN
        	salaries s
        		ON dm.emp_no = s.emp_no
        ORDER BY
        	row_num,
        	s.salary ASC;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### List managers with salary per manager, ranked in ascending and descending order in separate columns.

    List managers and their salaries using dept_manager and salaries. Add two window function columns.

    - A per manager rank by salary in ascending order (lowest 1).
    - A per manager rank by salary in descending order (highest 1).

    Do not use an `ORDER BY` clause in the `SELECT` statement.
    """)
    return


@app.cell
def _(dept_manager, engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
        	dm.emp_no,
            s.salary,
            ROW_NUMBER() OVER(PARTITION BY dm.emp_no ORDER BY s.salary ASC) AS salary_rank_asc,
            ROW_NUMBER() OVER(PARTITION BY dm.emp_no ORDER BY s.salary DESC) AS salary_rank_desc
        FROM
        	dept_manager dm
        	INNER JOIN
        	salaries s
        		ON dm.emp_no = s.emp_no
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### List `Staff` employees with salaries in ascending order, but inversely ranked.

    - Retrieve the employee number (`emp_no`) and job title (`title`) from the `titles` table, and the salary (`salary`) from the `salaries` table.
    - Add a column to the left, named `row_num1`, starting from `1` and incrementing by `1` for each row from the obtained result.
    - Also, add a fifth column, named `row_num2`, which provides a row position for each record per employee, starting from the total number of records obtained for that employee and continuing down to `1`.

    Include only data about `'Staff'` members and employees with a number no greater than `10006`. Order the result by `emp_no`, `salary`, and `row_num1` in ascending order.
    """)
    return


@app.cell
def _(engine: Engine, salaries, titles):
    _df = mo.sql(
        f"""
        SELECT
            ROW_NUMBER() OVER(ORDER BY t.emp_no ASC, s.salary ASC) AS row_num1,
        	t.emp_no,
            t.title,
        	s.salary,
            ROW_NUMBER() OVER(PARTITION BY t.emp_no ORDER BY s.salary DESC) AS row_num2
        FROM
        	titles t
        	INNER JOIN
        	salaries s
        	ON t.emp_no = s.emp_no
        WHERE
        	t.emp_no <= 10006
        AND t.title = 'Staff'
        ORDER BY
        	t.emp_no ASC,
            s.salary ASC,
        	row_num1 ASC;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### List `Staff` employees with salaries in ascending order, with row numbers assigned in ascending and descending order.

    - Retrieve the employee number (`emp_no`) and job title (`title`) from the `titles` table, and the salary (`salary`) from the salaries table.
    - Add a fourth column, named `row_num1`, starting from `1` and incrementing by 1 for each row for every employee from the obtained result.
    - Also, add a fifth column, named `row_num2`, which provides the opposite values, starting from the total number of records obtained for a given employee and continuing down to 1.
    - Include only data about `'Staff'` members and employees with a number no greater than `10006`.
    - Order the result by `emp_no`, `salary`, and `row_num1` in ascending order.
    """)
    return


@app.cell
def _(engine: Engine, salaries, titles):
    _df = mo.sql(
        f"""
        SELECT
            t.emp_no,
            t.title,
            s.salary,
            ROW_NUMBER() OVER(PARTITION BY t.emp_no ORDER BY s.salary ASC) AS row_num1,
            ROW_NUMBER() OVER(PARTITION BY t.emp_no ORDER BY s.salary DESC) AS row_num2
        FROM
        	titles t
        	INNER JOIN
        	salaries s
        		ON t.emp_no = s.emp_no
        WHERE
        	t.emp_no <= 10006
        	AND t.title = 'Staff'
        ORDER BY
        	t.emp_no,
        	s.salary,
        	row_num1;
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## `WINDOW()` Clause
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### List `emp_no` and `salary`, ranked by `salary` in descending order across each `emp_no` partition.
    """)
    return


@app.cell
def _(engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
            emp_no,
            salary,
            ROW_NUMBER() OVER w AS row_num
        FROM
        	salaries
        WINDOW w AS (PARTITION BY emp_no ORDER BY salary DESC);
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Using `WINDOW()`, list employees partitioned by first name, with each partition ordered by employee number in ascending order.

    - List all workers from the `employees` table, partitioned by first name, with row numbers across each partition ordered by employee number in ascending order.
    - Do **not** use an `ORDER BY` clause in the relevant `SELECT` statement.
    - Use a `WINDOW` clause to specify the window.
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
            ROW_NUMBER() OVER w AS row_num 
        FROM
        	employees
        WINDOW w AS (PARTITION BY first_name ORDER BY emp_no);
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Using the `WINDOW` clause, list employee number, first and last name, partitioned by first name and assigned row numbers in ascending order of employee number.

    - Retrieve everything stored in the `emp_no`, `first_name`, and `last_name` columns from the `employees` table.
    - Add a fourth column named `row_num`, which partitions the data by first name, sorts it by employee number in ascending order, and assigns a row number starting from `1` and incrementing for each row in every partition.
    - Use the `WINDOW` keyword to solve the exercise.
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
            ROW_NUMBER() OVER w AS row_num
        FROM
        	employees
        WINDOW w AS (PARTITION BY first_name ORDER BY emp_no ASC);
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## `PARTITION BY` vs `GROUP BY`
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### List minimum and maximum salary for each employee number.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    #### Using `GROUP BY`.
    """)
    return


@app.cell
def _(engine: Engine, salaries):
    _df = mo.sql(
        f"""
        SELECT
        	s.emp_no,
            MIN(s.salary) AS min_salary,
            MAX(s.salary) AS max_salary
        FROM
        	(
            SELECT
            	emp_no,
            	salary
            FROM
            	salaries
            ) s
        GROUP BY
        	s.emp_no
        ORDER BY s.emp_no
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
