from __future__ import annotations

import logging
import re
import os
from enum import Enum
from pathlib import PurePath
from typing import IO

import qfieldcloud.core.models
from django.conf import settings
from django.core.files.base import ContentFile
from django.db import transaction
from django.http import FileResponse, HttpRequest
from django.http.response import HttpResponse, HttpResponseBase
from qfieldcloud.core import utils_local
from qfieldcloud.core.utils2.audit import LogEntry, audit

logger = logging.getLogger(__name__)



def _delete_by_prefix_permanently(prefix: str):
    """
    Delete all objects and their versions starting with a given prefix.

    Similar concept to delete a directory.
    Do not use when deleting objects with precise key, as it will delete all objects that start with the same name.

    Args:
        prefix (str): Object's prefix to search and delete. Check the given prefix if it matches the expected format before using this function!

    Raises:
        RuntimeError: When the given prefix is not a string, empty string or leading slash. Check is very basic, do a throrogh checks before calling!
    """
    logging.info(f"Object deletion (permanent) with {prefix=}")

    # Illegal prefix is either empty string ("") or slash ("/"), it will delete random 1000 object versions.
    if not isinstance(prefix, str) or prefix == "" or prefix == "/":
        raise RuntimeError(f"Attempt to delete object with illegal {prefix=}")

    objects_to_remove = utils_local.list_files(utils_local.get_projects_dir(), prefix)
    for obj in objects_to_remove:
        utils_local.delete_objects(str(obj.absolute_path))


def _delete_by_key_permanently(key: str):
    """
    Delete an object with a given key.

    Deleting with this method will permanently delete objects and all their versions and the deletion is impossible to recover.
    In other words, it is a hard delete.

    Args:
        key (str): Object's key to search and delete. Check the given key if it matches the expected format before using this function!

    Raises:
        RuntimeError: When the given key is not a string, empty string or leading slash. Check is very basic, do a throrogh checks before calling!
    """
    logging.info(f"Delete (permanently) object with {key=}")

    # prevent disastrous results when prefix is either empty string ("") or slash ("/").
    if not isinstance(key, str) or key == "" or key == "/":
        raise RuntimeError(
            f"Attempt to delete (permanently) object with illegal {key=}"
        )

    utils_local.delete_objects(key)

def delete_version_permanently(version_obj: utils_local.FileObject):
    logging.info(
        f'File object version deletion (permanent) with "{version_obj.absolute_path=}"'
    )
    version_obj.absolute_path.unlink(True)


def get_attachment_dir_prefix(
    project: qfieldcloud.core.models.Project, filename: str
) -> str:  # noqa: F821
    """Returns the attachment dir where the file belongs to or empty string if it does not.

    Args:
        project (Project): project to check
        filename (str): filename to check

    Returns:
        str: the attachment dir or empty string if no match found
    """
    for attachment_dir in project.attachment_dirs:
        if filename.startswith(attachment_dir):
            return attachment_dir

    return ""


def file_response(
    request: HttpRequest,
    key: str,
    expires: int = 60,
    version: str | None = None,
    as_attachment: bool = False,
) -> HttpResponseBase:
    
    filename = PurePath(key).name
    file_path = utils_local.get_projects_dir().joinpath(key)

    # check if we are in NGINX proxy
    http_host = request.META.get("HTTP_HOST", "")
    https_port = http_host.split(":")[-1] if ":" in http_host else "443"

    if https_port == settings.WEB_HTTPS_PORT and not settings.IN_TEST_SUITE:

        with open(file_path, "rb") as return_file:
            file_data = return_file.read()
        # Let's NGINX handle the redirect to the storage and streaming the file contents back to the client
        response = HttpResponse(file_data)
        response['Content-Type'] = 'application/octet-stream'
        if as_attachment:
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response
    
    elif settings.DEBUG or settings.IN_TEST_SUITE:

        with open(file_path, "rb") as return_file:
            file_data = return_file.read()
        return FileResponse(
            file_data,
            as_attachment=as_attachment,
            filename=filename,
            content_type="application/octet-stream",
        )

    raise Exception(
        "Expected to either run behind nginx proxy, debug mode or within a test suite."
    )


class ImageMimeTypes(str, Enum):
    svg = "image/svg+xml"
    png = "image/png"
    jpg = "image/jpeg"

    @classmethod
    def or_none(cls, string: str) -> ImageMimeTypes | None:
        try:
            return cls(string)
        except ValueError:
            return None


def upload_user_avatar(
    user: qfieldcloud.core.models.User, file: IO, mimetype: ImageMimeTypes
) -> str:  # noqa: F821
    """Uploads a picture as a user avatar.

    NOTE this function does NOT modify the `UserAccount.avatar_uri` field

    Args:
        user (User):
        file (IO): file used as avatar
        mimetype (ImageMimeTypes): file mimetype

    Returns:
        str: URI to the avatar
    """
    key = f"users/{user.username}/avatar.{mimetype.name}"
    utils_local.upload_fileobj(file, key)
    return key


def delete_user_avatar(user: qfieldcloud.core.models.User) -> None:  # noqa: F821
    """Deletes the user's avatar file.

    NOTE this function does NOT modify the `UserAccount.avatar_uri` field

    Args:
        user (User):
    """
    key = user.useraccount.avatar_uri

    # it well could be the user has no avatar yet
    if not key:
        return

    # e.g. "users/suricactus/avatar.svg"
    if not key or not re.match(r"^users/[\w-]+/avatar\.(png|jpg|svg)$", key):
        raise RuntimeError(f"Suspicious deletion of user avatar {key=}")

    _delete_by_key_permanently(key)


def upload_project_thumbail(
    project: qfieldcloud.core.models.Project,
    file: IO,
    mimetype: str,
    filename: str,  # noqa: F821
) -> str:
    """Uploads a picture as a project thumbnail.

    NOTE this function does NOT modify the `Project.thumbnail_uri` field

    Args:
        project (Project):
        file (IO): file used as thumbail
        mimetype (str): file mimetype
        filename (str): filename

    Returns:
        str: URI to the thumbnail
    """

    # for now we always expect PNGs
    if mimetype == "image/svg+xml":
        extension = "svg"
    elif mimetype == "image/png":
        extension = "png"
    elif mimetype == "image/jpeg":
        extension = "jpg"
    else:
        raise Exception(f"Unknown mimetype: {mimetype}")

    key = f"{project.id}/meta/{filename}.{extension}"
    utils_local.upload_fileobj(file, key)
    return key


def delete_project_thumbnail(
    project: qfieldcloud.core.models.Project,
) -> None:  # noqa: F821
    """Delete a picture as a project thumbnail.

    NOTE this function does NOT modify the `Project.thumbnail_uri` field

    """
    key = project.thumbnail_uri

    # it well could be the project has no thumbnail yet
    if not key:
        return

    if not key or not re.match(
        # e.g. "9bf34e75-0a5d-47c3-a2f0-ebb7126eeccc/meta/thumbnail.png"
        r"^[\w]{8}(-[\w]{4}){3}-[\w]{12}/meta/thumbnail\.(png|jpg|svg)$",
        key,
    ):
        raise RuntimeError(f"Suspicious deletion of project thumbnail image {key=}")

    _delete_by_key_permanently(key)


def purge_old_file_versions(
    project: qfieldcloud.core.models.Project,
) -> None:  # noqa: F821
    """
    Deletes old versions of all files in the given project. Will keep __3__
    versions for COMMUNITY user accounts, and __10__ versions for PRO user
    accounts
    """

    keep_count = project.owner_aware_storage_keep_versions

    logger.info(f"Cleaning up old files for {project} to {keep_count} versions")

    # Process file by file
    for file in utils_local.get_project_files_with_versions(project.pk):
        # Skip the newest N
        old_versions_to_purge = sorted(
            file.versions, key=lambda v: v.last_modified, reverse=True
        )[keep_count:]

        # Debug print
        logger.debug(
            f'Purging {len(old_versions_to_purge)} out of {len(file.versions)} old versions for "{file.latest.name}"...'
        )

        # Remove the N oldest
        for old_version in old_versions_to_purge:
            if old_version.is_latest:
                # This is not supposed to happen, as versions were sorted above,
                # but leaving it here as a security measure in case version
                # ordering changes for some reason.
                raise Exception("Trying to delete latest version")

            if not old_version.key or not re.match(
                r"^[\w]{8}(-[\w]{4}){3}-[\w]{12}\/.+$", old_version.key
            ):
                raise RuntimeError(
                    f"Suspicious file version deletion {old_version.key=} {old_version.version_id=}"
                )
            # TODO: any way to batch those ? will probaby get slow on production
            delete_version_permanently(old_version)
            # TODO: audit ? take implementation from files_views.py:211

    # Update the project size
    project.save(recompute_storage=True)


def upload_file(file: IO, key: str):
    utils_local.upload_fileobj(
        file,
        key,
    )
    return key


def upload_project_file(
    project: qfieldcloud.core.models.Project, file: IO, filename: str  # noqa: F821
) -> str:
    key = f"{project.id}/files/{filename}"
    
    utils_local.upload_fileobj(
        file,
        key,
    )
    return key


def delete_all_project_files_permanently(project_id: str) -> None:
    prefix = f"{project_id}/"

    if not re.match(r"^[\w]{8}(-[\w]{4}){3}-[\w]{12}/$", prefix):
        raise RuntimeError(
            f"Suspicious deletion of all project files with {prefix=}"
        )

    _delete_by_prefix_permanently(prefix)


def delete_project_file_permanently(project: qfieldcloud.core.models.Project, filename: str):  # noqa: F821
    logger.info(f"Requested delete (permanent) of project file {filename=}")

    file = utils_local.get_project_file_with_versions(str(project.id), filename)

    if not file:
        raise Exception(
            f"No file with such name in the given project found {filename=}"
        )

    if not re.match(r"^[\w]{8}(-[\w]{4}){3}-[\w]{12}/.+$", file.latest.key):
        raise RuntimeError(f"Suspicious file deletion {file.latest.key=}")

    with transaction.atomic():
        _delete_by_key_permanently(file.latest.key)

        update_fields = ["file_storage_bytes"]

        if utils_local.is_qgis_project_file(filename):
            update_fields.append("project_filename")
            project.project_filename = None

        file_storage_bytes = project.file_storage_bytes - sum(
            [v.size for v in file.versions]
        )
        project.file_storage_bytes = max(file_storage_bytes, 0)

        project.save(update_fields=update_fields)

        # NOTE force audits to be required when deleting files
        audit(
            project,
            LogEntry.Action.DELETE,
            changes={f"{filename} ALL": [f'"{file.latest.md5sum}"', None]},
        )


def delete_project_file_version_permanently(
    project: qfieldcloud.core.models.Project,  # noqa: F821
    filename: str,
    version_id: int,
    include_older: bool = False,
) -> list[utils_local.FileObject]:
    """Deletes a specific version of given file.

    Args:
        project (Project): project the file belongs to
        filename (str): filename the version belongs to
        version_id (str): version id to delete
        include_older (bool, optional): when True, versions older than the passed `version` will also be deleted. If the version_id is the latest version of a file, this parameter will treated as False. Defaults to False.

    Returns:
        int: the number of versions deleted
    """
    file = utils_local.get_project_file_with_versions(str(project.id), filename)
    if not file:
        raise Exception(
            f"No file with such name in the given project found {filename=} {version_id=}"
        )

    if file.latest.version_id == version_id:
        include_older = False

        if len(file.versions) == 1:
            raise RuntimeError(
                "Forbidded attempt to delete a specific file version which is the only file version available."
            )

    versions_latest_first = list(reversed(file.versions))
    versions_to_delete: list[utils_local.FileObject] = []

    for file_version in versions_latest_first:
        if file_version.version_id == version_id:
            versions_to_delete.append(file_version)

            if include_older:
                continue
            else:
                break

        if versions_to_delete:
            assert (
                include_older
            ), "We should continue to loop only if `include_older` is True"
            assert (
                versions_to_delete[-1].last_modified > file_version.last_modified
            ), "Assert the other versions are really older than the requested one"

            versions_to_delete.append(file_version)

    with transaction.atomic():
        for file_version in versions_to_delete:
            if (
                not re.match(
                    r"^[\w]{8}(-[\w]{4}){3}-[\w]{12}/.+$",
                    file_version.key,
                )
                or not file_version.version_id
            ):
                raise RuntimeError(
                    f"Suspicious file version deletion {filename=} {version_id=} {include_older=}"
                )

            audit_suffix = file_version.display

            audit(
                project,
                LogEntry.Action.DELETE,
                changes={f"{filename} {audit_suffix}": [f'"{file_version.md5sum}"', None]},
            )

            delete_version_permanently(file_version)

    project.save(recompute_storage=True)

    return versions_to_delete


def get_stored_package_ids(project_id: str) -> set[str]:
    prefix = f"{project_id}/packages/"
    root_path = utils_local.get_projects_dir().joinpath(prefix)
    package_ids = set()

    for file in utils_local.list_files(utils_local.get_projects_dir(), prefix):
        file_path = PurePath(file.key)
        parts = file_path.relative_to(root_path).parts
        package_ids.add(parts[0])

    return package_ids


def delete_stored_package(project_id: str, package_id: str) -> None:
    prefix = f"{project_id}/packages/{package_id}/"

    if not re.match(
        # e.g. "projects/878039c4-b945-4356-a44e-a908fd3f2263/packages/633cd4f7-db14-4e6e-9b2b-c0ce98f9d338/"
        r"^[\w]{8}(-[\w]{4}){3}-[\w]{12}/packages/[\w]{8}(-[\w]{4}){3}-[\w]{12}/$",
        prefix,
    ):
        raise RuntimeError(
            f"Suspicious deletion on stored project package {project_id=} {package_id=}"
        )

    _delete_by_prefix_permanently(prefix)


def get_project_file_storage_in_bytes(project_id: str) -> int:
    """
    Calculates the project files storage in bytes, including their versions.
    WARNING This function can be quite slow on projects with thousands of files.
    """
    return utils_local.get_projects_dir().joinpath(project_id).stat().st_size
