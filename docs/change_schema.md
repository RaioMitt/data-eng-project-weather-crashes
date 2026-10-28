# Changing the database schema

The SQL that creates the weather DB's schemas and tables is in [`src/schema/`](../src/schema/):

| File | Schema | Contents |
|---|---|---|
| `bronze.sql` | `raw` | Raw data loaded from the APIs (see [bronze_layer.md](bronze_layer.md)) |
| `silver.sql` | `silver` | Cleaned data |
| `gold.sql` | `gold` | Star schema used for analysis |

## How the files are run

The folder is mounted into the Postgres container at `/docker-entrypoint-initdb.d`. Postgres runs every `.sql` file in that folder, in alphabetical order, **only when it creates a new, empty database**: the first `docker compose up`, or after the `src/pgdata_weather/` folder has been deleted.

If the database already exists, editing a file changes nothing until you apply the change yourself (see below).

## Making a change

1. Edit the SQL file for the layer you're changing.
2. Write every statement so it can safely run more than once: `CREATE SCHEMA IF NOT EXISTS ...`, `CREATE TABLE IF NOT EXISTS ...`.
3. End every statement with `;`. If one statement fails, the whole database fails to start on a fresh init.
4. If you add a new file whose tables depend on another file's tables, give the files number prefixes (`01_bronze.sql`, `02_silver.sql`, ...) so they run in the right order.

## Applying a change to an existing database

Pick one of these options.

### Option 1: Rebuild the database (deletes all data)

Use this when you don't need to keep the data. The DAGs can download it again.

```bash
cd src
docker compose down
sudo rm -rf pgdata_weather
docker compose up -d
```

All the schema files run again on startup.

### Option 2: Run the file in the running container (keeps data)

```bash
cd src
docker compose exec -T weather-db psql < schema/bronze.sql
```

### Option 3: Run the SQL in pgAdmin (keeps data)

Open http://localhost:5051, connect to the weather DB (see [startup.md](startup.md)), open the Query Tool, paste the SQL and run it.

## Changing a table that already exists

`CREATE TABLE IF NOT EXISTS` does **nothing** if the table is already there, so options 2 and 3 only create *new* tables. To change an existing table (add or change a column, change the primary key), do one of these:

- Use option 1 and rebuild the database.
- Run an `ALTER TABLE` by hand (option 2 or 3), for example:
  ```sql
  ALTER TABLE raw.weather_data_hourly ADD COLUMN IF NOT EXISTS quality_flag varchar(5);
  ```
  Then also update the `CREATE TABLE` in the SQL file, so that a fresh database gets the same structure.
- For a `raw` table: drop it, run the file again, and re-run the DAG to reload the data.
