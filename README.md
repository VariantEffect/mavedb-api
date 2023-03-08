# mavedb-api

API for MaveDB, the database of the database of Multiplexed Assays of Variant Effect

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
- [build](https://github.com/pypa/hatch) for building distributions. This can be installed with `pip install build`.
- [hatch](https://github.com/pypa/hatch) for building distributions. This can be installed with `pip install hatch`.

### Building distribution packages

To build the source distribution and wheel, run

```
python -m build
```

The build utility will look at `pyproject.toml` and invoke Hatchling to build the distributions.

The distribution can be uploaded to PyPI using [twine](https://twine.readthedocs.io/en/stable/).

For use as a server, this distribution includes an optional set of dependencies, which are only invoked if the package
is installed with `pip install mavedb[server]`.

### Running the API server in Docker on production and test systems

First build the application's Docker image:
```
docker build --tag mavedb-api/mavedb-api .
```
Then start the application and its database:
```
docker-compose -f docker-compose-prod.yml up -d
```
Omit `-d` (daemon) if you want to run the application in your terminal session, for instance to see startup errors without having
to inspect the Docker container's log.

To stop the application when it is running as a daemon, run
```
docker-compose -f docker-compose-prod.yml down
```

`docker-compose-prod.yml` configures two containers: one for the API server and one for the PostgreSQL database. The
The database stores data in a Docker volume named `mavedb-data`, which will persist after running `docker-compose down`.

**Notes**
1. The `mavedb-api` container requires the following environment variables, which are configured in
  `docker-compose-prod.yml`:

    - DB_HOST
    - DB_PORT
    - DB_DATABASE_NAME
    - DB_USERNAME
    - DB_PASSWORD
    - NCBI_API_KEY

    The database username and password should be edited for production deployments. `NCBI_API_KEY` will be removed in
    the future. **TODO** Move these to an .env file.

2. In the procedure given above, we do not push the Docker image to a repository like Docker Hub; we simply build the
  image on the machine where it will be used. But to deploy the API server on the AWS-hosted test site, first tag the
  image appropriately and push it to Elastic Container Repository. (These commands require )
  ```
  export ECRPASSWORD=$(aws ecr get-login-password --region us-west-2 --profile mavedb-test)
  echo $ECRPASSWORD | docker login --username AWS --password-stdin {aws_account_id}.dkr.ecr.us-west-2.amazonaws.com
  docker tag mavedb-api:latest {aws_account_id}.dkr.ecr.us-west-2.amazonaws.com/mavedb-api
  docker push {aws_account_id}.dkr.ecr.us-west-2.amazonaws.com/mavedb-api
  ```
  These commands presuppose that you have the [AWS CLI](https://aws.amazon.com/cli/) installed and have created a named
  profile, `mavedb-test`, with your AWS credentials.

  With the Docker image pushed to ECR, you can now deploy the application. **TODO** Add instructions if we want to
  document this.

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
docker-compose -f docker-compose-dev.yml up -d
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
