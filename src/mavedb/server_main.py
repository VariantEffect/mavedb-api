import logging

import uvicorn
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from requests import Request
from starlette import status
from starlette.responses import JSONResponse

from sqlalchemy.orm import configure_mappers

from mavedb.models import *

from mavedb import __version__
from mavedb.routers import access_keys, experiment_sets, experiments, target_genes, users
from mavedb.routers import (
    pubmed_identifiers,
    doi_identifiers,
    target_gene_identifiers,
    raw_read_identifiers,
    scoresets,
    reference_genomes,
)

logging.basicConfig()
# Un-comment this line to log all queries:
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Scan all our model classes and create backref attributes. Otherwise these attributes only get added to classes once an
# instance of the related class has been created.
configure_mappers()

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(access_keys.router)
app.include_router(doi_identifiers.router)
app.include_router(experiment_sets.router)
app.include_router(experiments.router)
app.include_router(pubmed_identifiers.router)
app.include_router(reference_genomes.router)
app.include_router(scoresets.router)
app.include_router(target_gene_identifiers.router)
app.include_router(target_genes.router)
app.include_router(users.router)
app.include_router(raw_read_identifiers.router)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print(exc.errors())
    print(map(lambda error: customize_validation_error(error), exc.errors()))
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": list(map(lambda error: customize_validation_error(error), exc.errors()))}),
    )


def customize_validation_error(error):
    print(error["type"])
    if error["type"] == "type_error.none.not_allowed":
        return {"loc": error["loc"], "msg": "Required", "type": error["type"]}
    return error


def customize_openapi_schema():
    title = "MaveDB API"
    version = __version__
    openapi_schema = get_openapi(title=title, version=version, routes=app.routes)
    openapi_schema["info"] = {
        "title": title,
        "version": version,
        "description": """MaveDB is a public repository for datasets from Multiplexed Assays of Variant Effect (MAVEs),
such as those generated by deep mutational scanning (DMS) or massively parallel reporter assay (MPRA) experiments.""",
        # 'termsOfService': 'url',
        "contact": {
            "name": "MavaDB/CAVA software group",
            "url": "https://github.com/VariantEffect/mavedb-api/issues",
            "email": "rubin.a@wehi.edu.au",
        },
        "license": {"name": "Gnu Affero General Public License 3.0", "url": "https://www.gnu.org/licenses/agpl-3.0.en.html"},
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


customize_openapi_schema()


# If the application is not already being run within a uvicorn server, start uvicorn here.
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
