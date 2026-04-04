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
    from postgres_factory import PostgresFactory

    return PostgresFactory, mo


@app.cell
def _(Engine, PostgresFactory):
    # Connect to postgres
    factory: PostgresFactory = PostgresFactory()
    engine: Engine = factory.create_engine()
    return (engine,)


@app.cell
def _(engine: "Engine", mo):
    _df = mo.sql(
        f"""
        SELECT VERSION();
        """,
        engine=engine
    )
    return


@app.cell
def _(engine: "Engine", mo):
    _df = mo.sql(
        f"""
        SELECT * FROM information_schema.tables;
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
