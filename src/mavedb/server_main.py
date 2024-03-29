import json
import logging
import os

import uvicorn
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from requests import Request
from slack_sdk.webhook import WebhookClient
from sqlalchemy.orm import configure_mappers
from starlette import status
from starlette.responses import JSONResponse
from eutils._internal.exceptions import EutilsRequestError

from mavedb.models import *

from mavedb import __version__
from mavedb.routers import (
    access_keys,
    api_information,
    doi_identifiers,
    experiment_sets,
    experiments,
    hgvs,
    licenses,
    mapped_variant,
    publication_identifiers,
    target_gene_identifiers,
    raw_read_identifiers,
    reference_genomes,
    score_sets,
    target_genes,
    users,
)
from mavedb.lib.exceptions import AmbiguousIdentifierError, NonexistentIdentifierError, MixedTargetError

logging.basicConfig()
# Un-comment this line to log all database queries:
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logger = logging.getLogger(__name__)


# Scan all our model classes and create backref attributes. Otherwise, these attributes only get added to classes once
# an instance of the related class has been created.
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
app.include_router(api_information.router)
app.include_router(doi_identifiers.router)
app.include_router(experiment_sets.router)
app.include_router(experiments.router)
app.include_router(hgvs.router)
app.include_router(licenses.router)
app.include_router(mapped_variant.router)
app.include_router(publication_identifiers.router)
app.include_router(raw_read_identifiers.router)
app.include_router(reference_genomes.router)
app.include_router(score_sets.router)
app.include_router(target_gene_identifiers.router)
app.include_router(target_genes.router)
app.include_router(users.router)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": list(map(lambda error: customize_validation_error(error), exc.errors()))}),
    )


@app.exception_handler(AmbiguousIdentifierError)
async def ambiguous_identifier_error_exception_handler(request: Request, exc: AmbiguousIdentifierError):
    return JSONResponse(
        status_code=400,
        content={"message": str(exc)},
    )


@app.exception_handler(NonexistentIdentifierError)
async def nonexistent_identifier_error_exception_handler(request: Request, exc: NonexistentIdentifierError):
    return JSONResponse(
        status_code=404,
        content={"message": str(exc)},
    )


@app.exception_handler(EutilsRequestError)
async def nonexistent_pmid_error_exception_handler(request: Request, exc: EutilsRequestError):
    return JSONResponse(
        status_code=404,
        content={"message": str(exc)},
    )


@app.exception_handler(MixedTargetError)
async def mixed_target_exception_handler(request: Request, exc: MixedTargetError):
    return JSONResponse(
        status_code=400,
        content={"message": str(exc)},
    )


def customize_validation_error(error):
    # surface custom validation loc context
    if error.get("ctx", {}).get("custom_loc"):
        error = {"loc": error["ctx"]["custom_loc"], "msg": error["msg"], "type": error["type"]}

    if error["type"] == "type_error.none.not_allowed":
        return {"loc": error["loc"], "msg": "Required", "type": error["type"]}
    return error


def exception_as_dict(ex):
    return dict(
        type=ex.__class__.__name__,
        exception=str(ex),
    )


@app.exception_handler(Exception)
async def exception_handler(request, err):
    logger.error("Uncaught exception", exc_info=err)
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook_url is not None and len(slack_webhook_url) > 0:
        client = WebhookClient(url=slack_webhook_url)
        response = client.send(
            text=json.dumps(exception_as_dict(err)),
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": json.dumps(exception_as_dict(err)),
                    },
                }
            ],
        )
    return JSONResponse(status_code=500, content={"message": "Internal server error"})


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
        "license": {
            "name": "Gnu Affero General Public License 3.0",
            "url": "https://www.gnu.org/licenses/agpl-3.0.en.html",
        },
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


customize_openapi_schema()


# If the application is not already being run within a uvicorn server, start uvicorn here.
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
