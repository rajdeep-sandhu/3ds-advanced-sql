import marimo

__generated_with = "0.21.1"
app = marimo.App(width="full", app_title="01_window_functions")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 01. Window Functions
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Setup and Connection
    """)
    return


@app.cell
def _():
    import marimo as mo
    import os
    import psycopg
    import sqlalchemy

    return mo, os, sqlalchemy


@app.cell
def _(os, sqlalchemy):
    # Connect to postgres
    _password = os.environ.get("POSTGRES_PASSWORD")
    _username = os.environ.get("POSTGRES_USER")
    _database = os.environ.get("POSTGRES_DB")

    # postgresql to use psycopg2, posgresql+psycopg to use psycopg3
    DATABASE_URL = f"postgresql+psycopg://{_username}:{_password}@db:5432/{_database}"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return (engine,)


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT VERSION();
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM information_schema.tables;
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
