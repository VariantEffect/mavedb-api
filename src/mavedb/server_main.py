import logging
import time

import uvicorn
from eutils._internal.exceptions import EutilsRequestError  # type: ignore
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from sqlalchemy.orm import configure_mappers
from starlette import status
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette_context.plugins import (
    CorrelationIdPlugin,
    RequestIdPlugin,
    UserAgentPlugin,
)

from mavedb import __version__
from mavedb.lib.exceptions import (
    AmbiguousIdentifierError,
    MixedTargetError,
    NonexistentIdentifierError,
)
from mavedb.lib.logging.canonical import log_request
from mavedb.lib.logging.context import (
    PopulatedRawContextMiddleware,
    format_raised_exception_info_as_dict,
    logging_context,
    save_to_logging_context,
)
from mavedb.lib.permissions import PermissionException
from mavedb.lib.slack import send_slack_message
from mavedb.models import *  # noqa: F403
from mavedb.routers import (
    access_keys,
    api_information,
    collections,
    controlled_keywords,
    doi_identifiers,
    experiment_sets,
    experiments,
    hgvs,
    licenses,
    mapped_variant,
    orcid,
    permissions,
    publication_identifiers,
    raw_read_identifiers,
    score_sets,
    statistics,
    target_gene_identifiers,
    target_genes,
    taxonomies,
    users,
)

logger = logging.getLogger(__name__)

# Scan all our model classes and create backref attributes. Otherwise, these attributes only get added to classes once
# an instance of the related class has been created.
configure_mappers()

app = FastAPI()
app.add_middleware(
    PopulatedRawContextMiddleware,
    plugins=(
        CorrelationIdPlugin(force_new_uuid=True),
        RequestIdPlugin(force_new_uuid=True),
        UserAgentPlugin(),
    ),
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(access_keys.router)
app.include_router(api_information.router)
app.include_router(collections.router)
app.include_router(controlled_keywords.router)
app.include_router(doi_identifiers.router)
app.include_router(experiment_sets.router)
app.include_router(experiments.router)
app.include_router(hgvs.router)
app.include_router(licenses.router)
# app.include_router(log.router)
app.include_router(mapped_variant.router)
app.include_router(orcid.router)
app.include_router(permissions.router)
app.include_router(publication_identifiers.router)
app.include_router(raw_read_identifiers.router)
app.include_router(score_sets.router)
app.include_router(statistics.router)
app.include_router(target_gene_identifiers.router)
app.include_router(target_genes.router)
app.include_router(taxonomies.router)
app.include_router(users.router)


@app.exception_handler(PermissionException)
async def permission_exception_handler(request: Request, exc: PermissionException):
    response = JSONResponse({"detail": exc.message}, status_code=exc.http_code)
    save_to_logging_context(format_raised_exception_info_as_dict(exc))
    log_request(request, response, time.time_ns())
    return response


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    response = JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": list(map(lambda error: customize_validation_error(error), exc.errors()))}),
    )
    save_to_logging_context(format_raised_exception_info_as_dict(exc))
    log_request(request, response, time.time_ns())
    return response


@app.exception_handler(AmbiguousIdentifierError)
async def ambiguous_identifier_error_exception_handler(request: Request, exc: AmbiguousIdentifierError):
    response = JSONResponse(status_code=400, content={"message": str(exc)})
    save_to_logging_context(format_raised_exception_info_as_dict(exc))
    log_request(request, response, time.time_ns())
    return response


@app.exception_handler(NonexistentIdentifierError)
async def nonexistent_identifier_error_exception_handler(request: Request, exc: NonexistentIdentifierError):
    response = JSONResponse(status_code=404, content={"message": str(exc)})
    save_to_logging_context(format_raised_exception_info_as_dict(exc))
    log_request(request, response, time.time_ns())
    return response


@app.exception_handler(EutilsRequestError)
async def nonexistent_pmid_error_exception_handler(request: Request, exc: EutilsRequestError):
    response = JSONResponse(status_code=404, content={"message": str(exc)})
    save_to_logging_context(format_raised_exception_info_as_dict(exc))
    log_request(request, response, time.time_ns())
    return response


@app.exception_handler(MixedTargetError)
async def mixed_target_exception_handler(request: Request, exc: MixedTargetError):
    response = JSONResponse(status_code=400, content={"message": str(exc)})
    save_to_logging_context(format_raised_exception_info_as_dict(exc))
    log_request(request, response, time.time_ns())
    return response


def customize_validation_error(error):
    # surface custom validation loc context
    if error.get("ctx", {}).get("custom_loc"):
        error = {
            "loc": error["ctx"]["custom_loc"],
            "msg": error["msg"],
            "type": error["type"],
        }

    if error["type"] == "type_error.none.not_allowed":
        return {"loc": error["loc"], "msg": "Required", "type": error["type"]}
    return error


@app.exception_handler(Exception)
async def exception_handler(request, err):
    save_to_logging_context(format_raised_exception_info_as_dict(err))
    response = JSONResponse(status_code=500, content={"message": "Internal server error"})

    try:
        logger.error(msg="Uncaught exception.", extra=logging_context(), exc_info=err)
        send_slack_message(err=err, request=request)
    finally:
        log_request(request, response, time.time_ns())

    return response


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
