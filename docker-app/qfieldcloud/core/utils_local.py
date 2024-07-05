import hashlib
import io
import json
import logging
import os
import posixpath
from datetime import datetime
from pathlib import Path, PurePath
from typing import IO, Generator, NamedTuple

import boto3
import jsonschema
import mypy_boto3_s3
from botocore.errorfactory import ClientError
from django.conf import settings
from django.core.files.uploadedfile import InMemoryUploadedFile, TemporaryUploadedFile

logger = logging.getLogger(__name__)


class FileObject(NamedTuple):
    version_id: int
    name: str
    key: str
    last_modified: datetime
    size: int
    md5sum: str
    is_latest: bool
    @property
    def display(self) -> str:
        return self.last_modified.strftime("v%Y%m%d%H%M%S")


class FileObjectWithVersions(NamedTuple):
    latest: FileObject
    versions: list[FileObject]

    @property
    def total_size(self) -> int:
        """Total size of all versions"""
        # latest is also in versions
        return sum(v.size for v in self.versions if v.size is not None)


def get_projects_dir() -> Path:

    if not os.path.exists(settings.PROJECTFILES_ROOT):
        os.makedirs(settings.PROJECTFILES_ROOT, exist_ok=True)

    return Path(settings.PROJECTFILES_ROOT)


def get_sha256(file: IO) -> str:
    """Return the sha256 hash of the file"""
    if type(file) is InMemoryUploadedFile or type(file) is TemporaryUploadedFile:
        return _get_sha256_memory_file(file)
    else:
        return _get_sha256_file(file)


def _get_sha256_memory_file(file: InMemoryUploadedFile | TemporaryUploadedFile) -> str:
    BLOCKSIZE = 65536
    hasher = hashlib.sha256()

    for chunk in file.chunks(BLOCKSIZE):
        hasher.update(chunk)

    file.seek(0)
    return hasher.hexdigest()


def _get_sha256_file(file: IO) -> str:
    BLOCKSIZE = 65536
    hasher = hashlib.sha256()
    buf = file.read(BLOCKSIZE)
    while len(buf) > 0:
        hasher.update(buf)
        buf = file.read(BLOCKSIZE)
    file.seek(0)
    return hasher.hexdigest()


def get_md5sum(file: IO) -> str:
    """Return the md5sum hash of the file"""
    if type(file) is InMemoryUploadedFile or type(file) is TemporaryUploadedFile:
        return _get_md5sum_memory_file(file)
    else:
        return _get_md5sum_file(file)


def _get_md5sum_memory_file(file: InMemoryUploadedFile | TemporaryUploadedFile) -> str:
    BLOCKSIZE = 65536
    hasher = hashlib.md5()

    for chunk in file.chunks(BLOCKSIZE):
        hasher.update(chunk)

    file.seek(0)
    return hasher.hexdigest()


def _get_md5sum_file(file: IO) -> str:
    BLOCKSIZE = 65536
    hasher = hashlib.md5()

    buf = file.read(BLOCKSIZE)
    while len(buf) > 0:
        hasher.update(buf)
        buf = file.read(BLOCKSIZE)
    file.seek(0)
    return hasher.hexdigest()


def strip_json_null_bytes(file: IO) -> IO:
    """Return JSON string stream without NULL chars."""
    result = io.BytesIO()
    result.write(file.read().decode().replace(r"\u0000", "").encode())
    file.seek(0)
    result.seek(0)

    return result


def safe_join(base: str, *paths: str) -> str:
    """
    A version of django.utils._os.safe_join for S3 paths.
    Joins one or more path components to the base path component
    intelligently. Returns a normalized version of the final path.
    The final path must be located inside of the base path component
    (otherwise a ValueError is raised).
    Paths outside the base path indicate a possible security
    sensitive operation.
    """
    base_path = base
    base_path = base_path.rstrip("/")
    paths = tuple(paths)

    final_path = base_path + "/"
    for path in paths:
        _final_path = posixpath.normpath(posixpath.join(final_path, path))
        # posixpath.normpath() strips the trailing /. Add it back.
        if path.endswith("/") or _final_path + "/" == final_path:
            _final_path += "/"
        final_path = _final_path
    if final_path == base_path:
        final_path += "/"

    # Ensure final_path starts with base_path and that the next character after
    # the base path is /.
    base_path_len = len(base_path)
    if not final_path.startswith(base_path) or final_path[base_path_len] != "/":
        raise ValueError(
            "the joined path is located outside of the base path" " component"
        )

    return final_path.lstrip("/")


def is_qgis_project_file(filename: str) -> bool:
    """Returns whether the filename seems to be a QGIS project file by checking the file extension."""
    path = PurePath(filename)

    if path.suffix.lower() in (".qgs", ".qgz"):
        return True

    return False


def get_qgis_project_file(project_id: str) -> str | None:
    """Return the relative path inside the project of the qgs/qgz file or
    None if no qgs/qgz file is present.

    Note that the file doesn't actually exist, instead there's a directory with
    the same name containing the file's various versions.
    """

    project_files_path = get_projects_dir().joinpath(f"{project_id}/files/")
    project_files = list_project_files(project_files_path)

    for file_path in project_files:
        if is_qgis_project_file(file_path.name.strip(".d")):
            relative_path = file_path.relative_to(project_files_path)
            return str(relative_path).strip(".d")

    return None


def list_project_files(path: Path) -> list[Path]: 
    """Lists all of the project file directories found inside the given project directory and all subdirectories"""

    all_files: list[Path] = []
    dirs = [entry for entry in path.glob("*") if entry.is_dir()]

    for entry in dirs:
        if entry.suffix.lower() == ".d":
            all_files.append(entry)
        else:
            all_files.extend(list_project_files(entry))

    return all_files


def check_file_path(path: str) -> str | None:
    """Check to see if a file exists in the projects directory.
    If it exists, the function returns the sha256 of the latest version of the file"""

    # 'path' esattamente cosa è? l'intero percorso del file partendo da projects/ o altro?
    # per ora consideriamolo come la prima opzione
    full_path = get_projects_dir().joinpath(path + ".d")
    if not os.path.exists(full_path):
        return None
    with open(get_latest_version(full_path), "r") as f:
        return get_sha256(f)
    

def get_latest_version(file_dir: Path) -> Path: 
    """Get the latest version of a file given its directory."""
    all_versions = [file for file in file_dir.glob("*.*")]
    # sort by leading id, ascending
    all_versions.sort(key = lambda f: int(f.name.split("_")[0]))
    return all_versions[-1]


def get_deltafile_schema_validator() -> jsonschema.Draft7Validator:
    """Creates a JSON schema validator to check whether the provided delta
    file is valid.

    Returns:
        jsonschema.Draft7Validator -- JSON Schema validator
    """
    schema_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "deltafile_01.json"
    )

    with open(schema_file) as f:
        schema_dict = json.load(f)

    jsonschema.Draft7Validator.check_schema(schema_dict)

    return jsonschema.Draft7Validator(schema_dict)


def get_project_files(project_id: str, path: str = "") -> list[FileObject]:
    """Returns a list of files and their versions.

    Args:
        project_id (str): the project id
        path (str): additional filter prefix

    Returns:
        list[FileObject]: the list of files
    """
    root_prefix = f"{project_id}/files/"
    prefix = f"{project_id}/files/{path}"
    return list_files(get_projects_dir(), prefix, root_prefix)


def list_files(
    base_dir: PurePath,
    prefix: str,
    strip_prefix: str = "",
) -> list[FileObject]:
    """List a directory's files under prefix."""

    files_path = Path(base_dir.joinpath(prefix))
    files: list[FileObject] = []
    
    for f in list_project_files(files_path):
        if strip_prefix:
            start_idx = len(str(base_dir.joinpath(strip_prefix)))
            name = str(f)[start_idx:]
        else:
            name = str(f.relative_to(base_dir))

        latest_version = get_latest_version(f)
        version_id = int(latest_version.name.split("_")[0])
        stats = os.stat(latest_version)
        with open(latest_version, "r") as opened_file:
            md5 = get_md5sum(opened_file)

        files.append(
            FileObject(
                name = name,
                version_id = version_id,
                key = str(f.relative_to(base_dir)),
                last_modified = datetime.fromtimestamp(stats.st_mtime),
                is_latest = True,
                size = stats.st_size,
                md5sum = md5
            )
        )

    files.sort(key = lambda f: f.name)
    return files