# mavedb-api

API for MaveDB. MaveDB is a biological database for Multiplex Assays of Variant Effect (MAVE) datasets.
The API powers the MaveDB website at [mavedb.org](https://www.mavedb.org) and can also be called separately (see
instructions [below](#using-mavedb-api)).


For more information about MaveDB or to cite MaveDB please refer to the
[MaveDB paper in Genome Biology](https://genomebiology.biomedcentral.com/articles/10.1186/s13059-019-1845-6).

## Using mavedb-api

### Using the library as an API client or validator for MaveDB data sets

Simply install the package using PIP:

```
pip install mavedb
```

Or add `mavedb` to your Python project's dependencies.

## Building and running mavedb-api

### Prerequisites

- Python 3.9 or later
- PIP
- [Poetry](https://python-poetry.org/) for building and publishing distributions. For details on installing poetry, consult its [documentation](https://python-poetry.org/docs/#installation).

### Building distribution packages

To build the source distribution and wheel, run

```
poetry build
```

The build utility will look at `pyproject.toml` and invoke Poetry to build the distributions. Note that it will output build artifacts to `./dist` by default.

The distribution can be uploaded to PyPI using Poetry as well. After building the packaged, simply invoke

```
poetry publish -r pypi -u <username> -p <password>
```

To build and publish the package in one go, just pass the `--build` flag to the publish command.

For use as a server, this distribution includes an optional set of dependencies, which are only invoked if the package
is installed with `poetry install mavedb --extras server`.

### Running a local version of the API server

First build the application's Docker image:
```
docker build --tag mavedb-api/mavedb-api .
```
Then start the application and its database:
```
docker-compose -f docker-compose-local.yml up -d
```
Omit `-d` (daemon) if you want to run the application in your terminal session, for instance to see startup errors without having
to inspect the Docker container's log.

To stop the application when it is running as a daemon, run
```
docker-compose -f docker-compose-local.yml down
```

`docker-compose-local.yml` configures four containers: one for the API server, one for the PostgreSQL database, one for the
worker node and one for the Redis cache which acts as the job queue for the worker node. The worker node stores data in a Docker
volume named `mavedb-redis` and the database stores data in a Docker volume named `mavedb-data`. Both these volumes will persist
after running `docker-compose down`.

**Notes**
1. The `mavedb-api` container requires the following environment variables, which are configured in
  `docker-compose-local.yml`:

    - DB_HOST
    - DB_PORT
    - DB_DATABASE_NAME
    - DB_USERNAME
    - DB_PASSWORD
    - NCBI_API_KEY
    - REDIS_IP
    - REDIS_PORT

    The database username and password should be edited for production deployments. `NCBI_API_KEY` will be removed in
    the future. **TODO** Move these to an .env file.

### Running the API server in Docker for development

A similar procedure can be followed to run the API server in development mode on your local machine. There are a couple
of differences:

- Your local source code directory is mounted to the Docker container, instead of copying it into the container.
- The Uvicorn web server is started with a `--reload` option, so that code changes will cause the application to be
  reloaded, and you will not have to restart the container.
- The API uses HTTP, whereas in production it uses encrypted communication via HTTPS.

To start the Docker container for development, make sure that the mavedb-api directory is allowed to be shared with
Docker.  In Docker Desktop, this can be configured under Settings > Resources > File sharing.

To start the application, run
```
docker-compose -f docker-compose-dev.yml up --build -d
```

Docker integration can also be configured in IDEs like PyCharm.

### Running the API server directly for development

Sometimes you may want to run the API server outside of Docker. There are two ways to do this:

Before using either of these methods, configure the environment variables described above.

1. Run the server_main.py script. This script will create the FastAPI application, start up an instance of the Uvicorn,
  and pass the application to it.
  ```
  export PYTHONPATH=${PYTHONPATH}:"`pwd`/src"
  python src/mavedb/server_main.py
  ```
2. Run Uvicorn and pass it the application. This method supports code change auto-reloading.
  ```
  export PYTHONPATH=${PYTHONPATH}:"`pwd`/src"
  uvicorn mavedb.server_main:app --reload
  ```

If you use PyCharm, the first method can be used in a Python run configuration, but the second method supports PyCharm's
FastAPI run configuration.

### Running the API server for production

We maintain deployment configuration options and steps within a [private repository](https://github.com/VariantEffect/mavedb-deployment) used for deploying this source code to
the production MaveDB environment. The main difference between the production setup and these local setups is that
the worker and api services are split into distinct environments, allowing them to scale up or down individually
dependent on need.
