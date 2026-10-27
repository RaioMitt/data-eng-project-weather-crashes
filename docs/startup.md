# Startup

## 1. Start the stack

```bash
cd src
cp .env_example .env      # optional — compose.yml falls back to the same defaults
docker compose up -d --build
```

The first start takes a few minutes. `airflow-init` runs the Airflow DB migration, creates the admin user and the `weather_db` connection, then exits.

Check that everything is up:

```bash
docker compose ps
```

Stop the stack with `docker compose down`. Database data is kept in `src/pgdata_weather/` and `src/pgdata_airflow/`.

## 2. Web UIs

| Service | URL | Login | Password |
|---|---|---|---|
| Airflow | http://localhost:8080 | `airflow` | `airflow` |
| pgAdmin | http://localhost:5051 | `admin@example.com` | `admin` |

## 3. Databases

| Database | Container | Host port | Database | User | Password |
|---|---|---|---|---|---|
| Weather DB (dbt target) | `weather-db` | `5434` | `weather_db` | `weather_user` | `weather_pass` |
| Airflow metadata DB | `dbt-airflow-db` | `5435` | `airflow` | `airflow` | `airflow` |

Connect from your machine (e.g. psql, DBeaver) via `localhost` and the host port:

```bash
psql -h localhost -p 5434 -U weather_user -d weather_db
```

### Adding the servers in pgAdmin

pgAdmin runs inside the Docker network, so use the **compose service name** and the internal port `5432` (not `localhost`):

| Field | Weather DB | Airflow DB |
|---|---|---|
| Host name/address | `weather-db` | `airflow-db` |
| Port | `5432` | `5432` |
| Maintenance database | `weather_db` | `airflow` |
| Username | `weather_user` | `airflow` |
| Password | `weather_pass` | `airflow` |

## 4. Airflow

- Admin user: `airflow` / `airflow`, email `admin@example.com`
- Connection `weather_db` (postgres → `weather-db:5432/weather_db`, user `weather_user`) 
## Default values reference

All values come from `src/compose.yml` defaults and can be overridden in `src/.env`.

| Variable | Default |
|---|---|
| `POSTGRES_VERSION` | `16.4` |
| `WEATHER_DB_USER` | `weather_user` |
| `WEATHER_DB_PASSWORD` | `weather_pass` |
| `WEATHER_DB_NAME` | `weather_db` |
| `WEATHER_DB_PORT` | `5434` |
| `AIRFLOW_DB_USER` | `airflow` |
| `AIRFLOW_DB_PASSWORD` | `airflow` |
| `AIRFLOW_DB_NAME` | `airflow` |
| `AIRFLOW_DB_PORT` | `5435` |
| `AIRFLOW_WEB_PORT` | `8080` |
| `AIRFLOW_ADMIN_USER` | `airflow` |
| `AIRFLOW_ADMIN_PASSWORD` | `airflow` |
| `AIRFLOW_ADMIN_EMAIL` | `admin@example.com` |
| `PGADMIN_PORT` | `5051` |
| `PGADMIN_EMAIL` | `admin@example.com` |
| `PGADMIN_PASSWORD` | `admin` |
| `PGADMIN_VERSION` | `REL-8_10` |
