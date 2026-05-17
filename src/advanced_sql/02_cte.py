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


if __name__ == "__main__":
    app.run()
