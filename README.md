# Advanced SQL Tutorial (365 Data Science)

- The original tutorial uses a local MySQL environment.
- This version uses a containerised Python and PostgreSQL setup, with Marimo

## Setup
- Fork the repository.
- Either clone it to use with a DevContainer, or create a codespace.
- `employees.sql` is Git LFS tracked. It is modified for PostgreSQL from its original MySQL version. The `Dockerfile` installs Git LFS. However, the file still needs to be pulled into the codespace or into the local repository using `git lfs pull`.
- Database credentials (set these in repository Codespace secrets or locally in a `.env` file in the `.devcontainer` folder):
    
    ```text
    POSTGRES_USER: dbtest
    POSTGRES_PASSWORD: dbtest
    POSTGRES_DB: employees
    ```