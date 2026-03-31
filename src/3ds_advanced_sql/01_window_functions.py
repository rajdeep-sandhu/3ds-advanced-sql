import marimo

__generated_with = "0.21.1"
app = marimo.App(width="full", app_title="01_window_functions")


@app.cell
def _():
    import marimo as mo
    import os
    import psycopg
    import sqlalchemy

    return os, sqlalchemy


@app.cell
def _(os, sqlalchemy):
    # Connect to postgres
    _password = os.environ.get("POSTGRES_PASSWORD")
    _username = os.environ.get("POSTGRES_USER")
    _database = os.environ.get("POSTGRES_DB")

    # postgresql to use psycopg2, posgresql+psycopg to use psycopg3
    DATABASE_URL = f"postgresql+psycopg://{_username}:{_password}@db:5432/{_database}"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return


if __name__ == "__main__":
    app.run()
