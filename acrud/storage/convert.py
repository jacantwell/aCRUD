# standard imports
from typing import Type
from io import BytesIO

# package imports
from multimethod import multidispatch

# local imports
from ..settings import (
    SUPPORTS_CSV,
    SUPPORTS_JSON,
    SUPPORTS_PICKLE,
    SUPPORTS_UPLOADFILE,
)

if SUPPORTS_UPLOADFILE:
    from fastapi import UploadFile
if SUPPORTS_JSON:
    import json
if SUPPORTS_PICKLE:
    import dill

"""Helper function for converting str to types"""


def get_type(file_path: str) -> Type:
    file_type = file_path.split(".")[-1]
    match file_type:
        case "txt":
            return str
        case "csv":
            return str
        case "json":
            return dict
        case "pickle":
            return object
        case "uploadfile":
            return UploadFile


# Define the `convert` function that will handle conversions to bytes
@multidispatch
def convert(data, return_type):
    raise NotImplementedError(
        f"Automatic conversion from {data.__class__} to {return_type} is not yet supported."
    )


if SUPPORTS_CSV:

    # Conversion to bytes
    @convert.register
    def _(data: str, return_type: Type[bytes]):
        # Convert CSV (string) to bytes
        print("Converting str to bytes")
        return data.encode("utf-8")

    # Conversion from bytes
    @convert.register
    def _(data: bytes, return_type: Type[str]):
        # Convert bytes to CSV (string)
        print("Converting bytes to str")
        return data.decode("utf-8")


if SUPPORTS_JSON:

    @convert.register
    def _(data: dict, return_type: Type[bytes]):
        # Convert JSON (dict) to bytes
        print("Converting JSON to bytes")
        return json.dumps(data).encode("utf-8")

    @convert.register
    def _(data: bytes, return_type: Type[dict]):
        # Convert bytes to JSON (dict)
        print("Converting bytes to JSON")
        return json.loads(data.decode("utf-8"))


if SUPPORTS_PICKLE:

    @convert.register
    def _(data: bytes, return_type: Type[object]):
        # Pickle is already in bytes, return as is
        print("Converting bytes to object")
        return data

    @convert.register
    def _(data: object, return_type: Type[bytes]):
        # Convert object to bytes using pickle
        print("Converting object to bytes")
        buffer = BytesIO()
        dill.dump(data, buffer)
        buffer.seek(0)
        return buffer.getvalue()


if SUPPORTS_UPLOADFILE:

    @convert.register
    def _(data: UploadFile, return_type: Type[bytes]):
        # Read bytes from FastAPI's UploadFile object
        return data.file.read()

    @convert.register
    def _(data: bytes, return_type: Type[UploadFile]):
        # Convert bytes to FastAPI's UploadFile object
        return UploadFile(data)
