"""Generic dependency for loading Pydantic models from either JSON body or multipart form data."""

from typing import Awaitable, Callable, Type, TypeVar

from fastapi import Form, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


def create_flexible_model_loader(
    model_class: Type[T], form_field_name: str = "item", error_detail_prefix: str = "Invalid request"
) -> Callable[..., Awaitable[T]]:
    """Create a flexible FastAPI dependency that can load a Pydantic model from either
    JSON request body or multipart form data containing JSON.
    
    This factory function creates a dependency that enables FastAPI routes to accept
    data in two formats:
    1. Standard JSON request body (Content-Type: application/json)
    2. Multipart form data with JSON string in a specified field
    
    This is particularly useful for endpoints that need to handle both pure JSON
    requests and file uploads with accompanying metadata, allowing clients to
    choose the most appropriate format for their use case.

    Args:
        model_class (Type[T]): The Pydantic model class to instantiate from the JSON data.
            Must be a subclass of BaseModel with proper field definitions and validation.
        form_field_name (str, optional): Name of the form field containing JSON data 
            when using multipart/form-data requests. This parameter is primarily for 
            documentation purposes - the actual form field in OpenAPI docs will be 
            named 'item'. Defaults to "item".
        error_detail_prefix (str, optional): Prefix text for error messages to provide
            context about which operation failed. Defaults to "Invalid request".

    Returns:
        Callable[..., Awaitable[T]]: An async dependency function that can be used 
            with FastAPI's Depends(). The returned function accepts a Request object
            and optional form data, returning an instance of the specified model_class.

    Raises:
        RequestValidationError: When the JSON data doesn't match the Pydantic model schema.
            This preserves FastAPI's standard validation error format for consistent
            client error handling.
        HTTPException: For other parsing errors like invalid JSON syntax, missing data,
            or unexpected exceptions during processing.

    Example:
        Basic usage with a simple model:
        
        >>> from pydantic import BaseModel
        >>> class UserModel(BaseModel):
        ...     name: str
        ...     email: str
        
        >>> user_loader = create_flexible_model_loader(UserModel)
        
        >>> @app.post("/users")
        ... async def create_user(user: UserModel = Depends(user_loader)):
        ...     return {"user": user}
        
        Advanced usage with file uploads:
        
        >>> calibration_loader = create_flexible_model_loader(
        ...     ScoreCalibrationCreate,
        ...     form_field_name="calibration_metadata",
        ...     error_detail_prefix="Invalid calibration data"
        ... )
        
        >>> @app.post("/calibrations")
        ... async def create_calibration(
        ...     calibration: ScoreCalibrationCreate = Depends(calibration_loader),
        ...     file: UploadFile = File(...)
        ... ):
        ...     # Process both calibration metadata and uploaded file
        ...     return process_calibration(calibration, file)

    Client Usage Examples:
        JSON request:
        ```bash
        curl -X POST "http://api/users" \\
             -H "Content-Type: application/json" \\
             -d '{"name": "John", "email": "john@example.com"}'
        ```
        
        Multipart form request:
        ```bash
        curl -X POST "http://api/calibrations" \\
             -F 'item={"name": "Test", "description": "Example"}' \\
             -F 'file=@data.csv'
        ```

    Note:
        The dependency prioritizes form data over JSON body - if both are provided,
        the form field data will be used. This ensures predictable behavior when
        clients mix content types.

    OpenAPI Documentation Enhancement:
        Without manual definition, OpenAPI docs will show the form field as 'item' for
        multipart requests, regardless of the form_field_name parameter. To customize the
        OpenAPI documentation and show both JSON and multipart form options clearly, use
        the `openapi_extra` parameter on your route decorator:

        ```python
        @router.post(
            "/example-endpoint",
            response_model=ExampleResponseModel,
            summary="Example endpoint using flexible model loader",
            description="Example endpoint description",
            openapi_extra={
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/YourModelName"},
                            "example": {
                                "example_field": "example_value",
                                "another_field": 123
                            }
                        },
                        "multipart/form-data": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "item": {
                                        "type": "string",
                                        "description": "JSON string containing the model data",
                                        "example": '{"example_field":"example_value","another_field":123}'
                                    },
                                    "file_upload": {
                                        "type": "string",
                                        "format": "binary",
                                        "description": "Optional file upload"
                                    }
                                }
                            }
                        }
                    },
                    "description": "Data can be sent as JSON body or multipart form data"
                }
            }
        )
        async def example_endpoint(
            model_data: YourModel = Depends(your_loader),
            file_upload: UploadFile = File(None)
        ):
            return process_data(model_data, file_upload)
        ```

        This configuration will display both content types clearly in the OpenAPI/Swagger UI,
        allowing users to choose between JSON and multipart form submission methods.
    """

    async def flexible_loader(
        request: Request,
        item: str = Form(None, description="JSON data for the request", alias=form_field_name),
    ) -> T:
        """Load Pydantic model from either JSON body or form field."""
        try:
            # Prefer form field if provided
            if item is not None:
                model_instance = model_class.model_validate_json(item)
            # Fall back to JSON body
            else:
                body = await request.body()
                if not body:
                    raise HTTPException(
                        status_code=422, detail=f"{error_detail_prefix}: No data provided in form field or request body"
                    )
                model_instance = model_class.model_validate_json(body)

            return model_instance

        # Raise validation errors in FastAPI's expected format
        except ValidationError as e:
            raise RequestValidationError(e.errors())
        # Any other parsing errors
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"{error_detail_prefix}: {str(e)}")

    return flexible_loader


# Convenience factory for common use cases
def json_or_form_loader(model_class: Type[T], field_name: str = "item") -> Callable[..., Awaitable[T]]:
    """Simplified factory function for creating flexible model loaders with sensible defaults.

    This is a convenience wrapper around create_flexible_model_loader() that provides
    a quick way to create loaders without specifying all parameters. It automatically
    generates an appropriate error message prefix based on the model class name.

    Args:
        model_class (Type[T]): The Pydantic model class to load from JSON data.
        field_name (str, optional): Name of the form field for documentation purposes.
            Defaults to "item".

    Returns:
        Callable[..., Awaitable[T]]: A flexible dependency function ready to use with Depends().

    Example:
        Quick setup for simple cases:

        >>> user_loader = json_or_form_loader(UserModel)
        >>> @app.post("/users")
        ... async def create_user(user: UserModel = Depends(user_loader)):
        ...     return user
    """
    return create_flexible_model_loader(
        model_class=model_class, form_field_name=field_name, error_detail_prefix=f"Invalid {model_class.__name__} data"
    )
