import logging
import os
import shutil
from datetime import datetime
from pathlib import Path, PurePath
from typing import IO, NamedTuple

from django.conf import settings

from qfieldcloud.core.utils import get_sha256, get_md5sum

logger = logging.getLogger(__name__)


class FileObject(NamedTuple):
    """Represents a single version of a project file."""
    name: str
    version_id: int
    absolute_path: Path
    key: str
    last_modified: datetime
    size: int
    md5sum: str
    is_latest: bool
    @property
    def display(self) -> str:
        return self.last_modified.strftime("v%Y%m%d%H%M%S")


class FileObjectWithVersions(NamedTuple):
    """Represents all of the versions of a project file."""
    latest: FileObject
    versions: list[FileObject]
    @property
    def total_size(self) -> int:
        """Total size of all versions"""
        # latest is also in versions
        return sum(v.size for v in self.versions if v.size is not None)


def get_projects_dir() -> Path:
    """Return the root directory where the project files are stored."""
    if not os.path.exists(settings.PROJECTFILES_ROOT):
        os.makedirs(settings.PROJECTFILES_ROOT, exist_ok=True)
    return Path(settings.PROJECTFILES_ROOT)


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


def list_project_files(project_dir: Path) -> list[Path]: 
    """Lists all of the project file directories found inside the given project directory and all subdirectories"""

    all_files: list[Path] = []
    dirs = [entry for entry in project_dir.glob("*") if entry.is_dir()]

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
    with open(get_latest_version(full_path), "rb") as f:
        return get_sha256(f)


def get_all_versions(file_dir: Path) -> list[Path]: 
    """Get all the versions of a file given its directory."""
    all_versions = [file for file in file_dir.glob("*.*")]
    all_versions.sort(key = lambda f: int(f.name.split("_")[0]))
    return all_versions


def get_latest_version(file_dir: Path) -> Path: 
    """Get the latest version of a file given its directory."""
    versions = get_all_versions(file_dir)
    return versions[-1] if versions else None # type: ignore


def get_project_files(project_id: str, path: str = "") -> list[FileObject]:
    """Returns the list of files found in the given project, 
    additionally filtered by looking only in a specific path inside the project.
    All the files returned represent their latest version.

    Args:
        project_id (str): the project id
        path (str): additional filter path

    Returns:
        list[FileObject]: the list of files
    """
    root_prefix = f"{project_id}/files/"
    prefix = f"{project_id}/files/{path}"
    return list_files(get_projects_dir(), prefix, root_prefix)


def get_project_package_files(project_id: str, package_id: str) -> list[FileObject]:
    """Returns the list of files found in the given project package, 
    All the files returned represent their latest version.

    Args:
        project_id (str): the project id
        path (str): the package id

    Returns:
        list[FileObject]: the list of files
    """
    prefix = f"{project_id}/packages/{package_id}/"
    return list_files(get_projects_dir(), prefix)


def get_project_files_count(project_id: str) -> int:
    """Returns the number of files within a project."""
    files = list_project_files(get_projects_dir().joinpath(f"{project_id}/files/"))
    return len(files)


def get_project_package_files_count(project_id: str) -> int:
    """Returns the number of package files within a project package."""
    files = list_project_files(get_projects_dir().joinpath(f"{project_id}/export/"))
    return len(files)


def get_project_files_with_versions(project_id: str) -> list[FileObjectWithVersions]:
    """Returns the list of files and all of their versions found in the given project."""
    prefix = f"{project_id}/files/"
    return list_files_with_versions(get_projects_dir(), prefix, prefix)


def get_project_file_with_versions(project_id: str, filename: str) -> FileObjectWithVersions | None:
    """Returns the specified project file (if it exists) and its versions."""

    all_files = list_project_files(get_projects_dir().joinpath(project_id))
    files = [file for file in all_files if file.name.strip(".d") == filename]

    if not files:
        return None

    file = files[0]
    all_versions = get_all_versions(file)
    latest_version = all_versions[-1]

    name = str(file)[len(f"{project_id}/files/"):]
    key = str(file.relative_to(get_projects_dir()))

    latest_version_obj = _create_file_object(latest_version, name, key, True)

    all_versions_obj: list[FileObject] = []
    for version in all_versions:
        is_latest = _get_version_id(version) == latest_version_obj.version_id
        all_versions_obj.append(_create_file_object(version, name, key, is_latest))

    return FileObjectWithVersions(latest_version_obj, all_versions_obj)


def list_files(
    base_dir: PurePath,
    prefix: str,
    strip_prefix: str = "",
) -> list[FileObject]:
    """List a directory's project files under prefix."""

    files_path = Path(base_dir.joinpath(prefix))
    files: list[FileObject] = []
    
    for f in list_project_files(files_path):
        key = str(f.relative_to(base_dir))

        if strip_prefix:
            start_idx = len(str(base_dir.joinpath(strip_prefix)))
            name = str(f)[start_idx:]
        else:
            name = key

        latest_version = get_latest_version(f)
        if not latest_version:
            continue
        files.append(_create_file_object(latest_version, name, key, True))

    files.sort(key = lambda f: f.name)
    return files


def list_files_with_versions(
    base_dir: Path,
    prefix: str,
    strip_prefix: str = "",
) -> list[FileObjectWithVersions]:
    """List a directory's project filesc and all of their versions under prefix."""
    
    files_with_versions: list[FileObjectWithVersions] = []

    for f in list_files(base_dir, prefix, strip_prefix):

        full_path = base_dir.joinpath(f.key)
        versions: list[FileObject] = []

        for version in get_all_versions(full_path):
            is_latest = _get_version_id(version) == f.version_id
            versions.append(_create_file_object(version, f.name, f.key, is_latest))

        files_with_versions.append(FileObjectWithVersions(f, versions))
    
    return files_with_versions


def _create_file_object(file_path: Path, name: str, key: str, is_latest: bool) -> FileObject:

    with open(file_path, "rb") as f:
        md5 = get_md5sum(f)

    return FileObject(
        name = name,
        version_id = _get_version_id(file_path),
        absolute_path = file_path.resolve(),
        key = key,
        last_modified = datetime.fromtimestamp(file_path.stat().st_mtime),
        size = file_path.stat().st_size,
        md5sum = md5,
        is_latest = is_latest
    )


def _get_version_id(file_path: PurePath) -> int:
    return int(file_path.name.split("_")[0])


def upload_fileobj(file: IO, key: str):
    """Upload a file with the specified key to the projects directory."""

    dir_path = get_projects_dir().joinpath(key +".d")
    os.makedirs(dir_path, exist_ok=True)

    count: int
    count_list = get_all_versions(dir_path)
    if not count_list or len(count_list) == 0:
        count = 1
    else:
        count = int(_get_version_id(get_latest_version(dir_path))) + 1
    
    file_path = str(count) + "_" + PurePath(key).parts[-1]
    final_path = dir_path.joinpath(file_path)

    with open(final_path, "wb") as f:
        f.write(file.read())


def delete_objects(key: str):
    """"Delete a file/directory with the specified key from the projects directory."""

    path = get_projects_dir().joinpath(key)
    if not path.exists():
        return

    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
