from __future__ import annotations

import logging
import os
import re
from pathlib import PurePath
from typing import IO, List, Set

import qfieldcloud.core.models
import qfieldcloud.core.utils
from django.conf import settings
from django.core.files.base import ContentFile
from django.db import transaction
from django.http import FileResponse, HttpRequest
from django.http.response import HttpResponse, HttpResponseBase
from qfieldcloud.core.utils2.audit import LogEntry, audit

logger = logging.getLogger(__name__)

QFIELDCLOUD_HOST = os.environ.get("QFIELDCLOUD_HOST", None)
WEB_HTTPS_PORT = os.environ.get("WEB_HTTPS_PORT", None)


def _delete_by_prefix_versioned(prefix: str):
    """
    Delete all objects and their versions starting with a given prefix.

    Similar concept to delete a directory.
    Do not use when deleting objects with precise key, as it will delete all objects that start with the same name.
    Deleting with this method will leave a deleted version and the deletion is not permanent.
    In other words, it is a soft delete. Read more here: https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html

    Args:
        prefix (str): Object's prefix to search and delete. Check the given prefix if it matches the expected format before using this function!

    Raises:
        RuntimeError: When the given prefix is not a string, empty string or leading slash. Check is very basic, do a throrogh checks before calling!
    """
    logging.info(f"S3 object deletion (versioned) with {prefix=}")

    # Illegal prefix is either empty string ("") or slash ("/"), it will delete random 1000 objects.
    if not isinstance(prefix, str) or prefix == "" or prefix == "/":
        raise RuntimeError(f"Attempt to delete S3 object with illegal {prefix=}")

    bucket = qfieldcloud.core.utils.get_s3_bucket()
    return bucket.objects.filter(Prefix=prefix).delete()


def _delete_by_prefix_permanently(prefix: str):
    """
    Delete all objects and their versions starting with a given prefix.

    Similar concept to delete a directory.
    Do not use when deleting objects with precise key, as it will delete all objects that start with the same name.
    Deleting with this method will permanently delete objects and all their versions and the deletion is impossible to recover.
    In other words, it is a hard delete. Read more here: https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html

    Args:
        prefix (str): Object's prefix to search and delete. Check the given prefix if it matches the expected format before using this function!

    Raises:
        RuntimeError: When the given prefix is not a string, empty string or leading slash. Check is very basic, do a throrogh checks before calling!
    """
    logging.info(f"S3 object deletion (permanent) with {prefix=}")

    # Illegal prefix is either empty string ("") or slash ("/"), it will delete random 1000 object versions.
    if not isinstance(prefix, str) or prefix == "" or prefix == "/":
        raise RuntimeError(f"Attempt to delete S3 object with illegal {prefix=}")

    bucket = qfieldcloud.core.utils.get_s3_bucket()
    return bucket.object_versions.filter(Prefix=prefix).delete()


def _delete_by_key_versioned(key: str):
    """
    Delete an object with a given key.

    Deleting with this method will leave a deleted version and the deletion is not permanent.
    In other words, it is a soft delete.

    Args:
        key (str): Object's key to search and delete. Check the given key if it matches the expected format before using this function!

    Raises:
        RuntimeError: When the given key is not a string, empty string or leading slash. Check is very basic, do a throrogh checks before calling!
    """
    logging.info(f"S3 object deletion (versioned) with {key=}")

    # prevent disastrous results when prefix is either empty string ("") or slash ("/").
    if not isinstance(key, str) or key == "" or key == "/":
        raise RuntimeError(f"Attempt to delete S3 object with illegal {key=}")

    bucket = qfieldcloud.core.utils.get_s3_bucket()

    return bucket.delete_objects(
        Delete={
            "Objects": [
                {
                    "Key": key,
                }
            ],
        },
    )


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
    logging.info(f"S3 object deletion (versioned) with {key=}")

    # prevent disastrous results when prefix is either empty string ("") or slash ("/").
    if not isinstance(key, str) or key == "" or key == "/":
        raise RuntimeError(f"Attempt to delete S3 object with illegal {key=}")

    bucket = qfieldcloud.core.utils.get_s3_bucket()

    # NOTE filer by prefix will return all objects with that prefix. E.g. for given key="orho.tif", it will return "ortho.tif", "ortho.tif.aux.xml" and "ortho.tif.backup"
    temp_objects = bucket.object_versions.filter(
        Prefix=key,
    )
    object_to_delete = []
    for temp_object in temp_objects:
        # filter out objects that do not have the same key as the requested deletion key.
        if temp_object.key != key:
            continue

        object_to_delete.append(
            {
                "Key": key,
                "VersionId": temp_object.id,
            }
        )

    assert len(object_to_delete) > 0

    return bucket.delete_objects(
        Delete={
            "Objects": object_to_delete,
        },
    )


def delete_version_permanently(version_obj: qfieldcloud.core.utils.S3ObjectVersion):
    logging.info(
        f'S3 object version deletion (permanent) with "{version_obj.key=}" and "{version_obj.id=}"'
    )

    version_obj._data.delete()


def get_attachment_dir_prefix(project: "Project", filename: str) -> str:  # noqa: F821
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
    presigned: bool = False,
    expires: int = 60,
    version: str = None,
    as_attachment: bool = False,
) -> HttpResponseBase:
    url = ""
    filename = PurePath(key).name
    extra_params = {}

    if version is not None:
        extra_params["VersionId"] = version

    # check if we are in NGINX proxy
    http_host = request.META.get("HTTP_HOST", "")
    https_port = http_host.split(":")[-1] if ":" in http_host else "443"

    if https_port == WEB_HTTPS_PORT and not settings.IN_TEST_SUITE:
        if presigned:
            if as_attachment:
                extra_params["ResponseContentType"] = "application/force-download"
                extra_params[
                    "ResponseContentDisposition"
                ] = f'attachment;filename="{filename}"'

            url = qfieldcloud.core.utils.get_s3_client().generate_presigned_url(
                "get_object",
                Params={
                    **extra_params,
                    "Key": key,
                    "Bucket": qfieldcloud.core.utils.get_s3_bucket().name,
                },
                ExpiresIn=expires,
                HttpMethod="GET",
            )
        else:
            url = qfieldcloud.core.utils.get_s3_object_url(key)

        # Let's NGINX handle the redirect to the storage and streaming the file contents back to the client
        response = HttpResponse()
        response["X-Accel-Redirect"] = "/storage-download/"
        response["redirect_uri"] = url

        return response
    elif settings.DEBUG or settings.IN_TEST_SUITE:
        return_file = ContentFile(b"")
        qfieldcloud.core.utils.get_s3_bucket().download_fileobj(
            key,
            return_file,
            extra_params,
        )

        return FileResponse(
            return_file.open(),
            as_attachment=as_attachment,
            filename=filename,
            content_type="text/html",
        )

    raise Exception(
        "Expected to either run behind nginx proxy, debug mode or within a test suite."
    )


def upload_user_avatar(user: "User", file: IO, mimetype: str) -> str:  # noqa: F821
    """Uploads a picture as a user avatar.

    NOTE this function does NOT modify the `UserAccount.avatar_uri` field

    Args:
        user (User):
        file (IO): file used as avatar
        mimetype (str): file mimetype

    Returns:
        str: URI to the avatar
    """
    bucket = qfieldcloud.core.utils.get_s3_bucket()

    if mimetype == "image/svg+xml":
        extension = "svg"
    elif mimetype == "image/png":
        extension = "png"
    elif mimetype == "image/jpeg":
        extension = "jpg"
    else:
        raise Exception(f"Unknown mimetype: {mimetype}")

    key = f"users/{user.username}/avatar.{extension}"
    bucket.upload_fileobj(
        file,
        key,
        {
            "ACL": "public-read",
            "ContentType": mimetype,
        },
    )
    return key


def remove_user_avatar(user: "User") -> None:  # noqa: F821
    """Removes the user's avatar file.

    NOTE this function does NOT modify the `UserAccount.avatar_uri` field

    Args:
        user (User):
    """
    key = user.useraccount.avatar_uri

    # it well could be the user has no avatar yet
    if not key:
        return

    if not key or not re.match(r"^users/\w+/avatar.(png|jpg|svg)$", key):
        raise RuntimeError(f"Suspicious S3 deletion of user avatar {key=}")

    _delete_by_key_permanently(key)


def upload_project_thumbail(
    project: "Project", file: IO, mimetype: str, filename: str  # noqa: F821
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
    bucket = qfieldcloud.core.utils.get_s3_bucket()

    # for now we always expect PNGs
    if mimetype == "image/svg+xml":
        extension = "svg"
    elif mimetype == "image/png":
        extension = "png"
    elif mimetype == "image/jpeg":
        extension = "jpg"
    else:
        raise Exception(f"Unknown mimetype: {mimetype}")

    key = f"projects/{project.id}/meta/{filename}.{extension}"
    bucket.upload_fileobj(
        file,
        key,
        {
            # TODO most probably this is not public-read, since the project might be private
            "ACL": "public-read",
            "ContentType": mimetype,
        },
    )
    return key


def remove_project_thumbail(project: "Project") -> None:  # noqa: F821
    """Uploads a picture as a project thumbnail.

    NOTE this function does NOT modify the `Project.thumbnail_uri` field

    """
    key = project.thumbnail_uri

    # it well could be the project has no thumbnail yet
    if not key:
        return

    if not key or not re.match(
        r"^projects/[\w]{8}(-[\w]{4}){3}-[\w]{12}/meta/\w+.(png|jpg|svg)$", key
    ):
        raise RuntimeError(f"Suspicious S3 deletion of project thumbnail image {key=}")

    _delete_by_key_permanently(key)


def purge_old_file_versions(project: "Project") -> None:  # noqa: F821
    """
    Deletes old versions of all files in the given project. Will keep __3__
    versions for COMMUNITY user accounts, and __10__ versions for PRO user
    accounts
    """

    logger.info(f"Cleaning up old files for {project}")

    # Number of versions to keep is determined by the account type
    keep_count = (
        project.owner.useraccount.active_subscription.plan.storage_keep_versions
    )

    logger.debug(f"Keeping {keep_count} versions")

    # Process file by file
    for file in qfieldcloud.core.utils.get_project_files_with_versions(project.pk):

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
                r"^projects/[\w]{8}(-[\w]{4}){3}-[\w]{12}/.+$", old_version.key
            ):
                raise RuntimeError(
                    f"Suspicious S3 file version deletion {old_version.key=} {old_version.id=}"
                )
            # TODO: any way to batch those ? will probaby get slow on production
            delete_version_permanently(old_version)
            # TODO: audit ? take implementation from files_views.py:211

    # Update the project size
    project.save(recompute_storage=True)


def upload_file(file: IO, key: str):
    bucket = qfieldcloud.core.utils.get_s3_bucket()
    bucket.upload_fileobj(
        file,
        key,
    )
    return key


def upload_project_file(
    project: "Project", file: IO, filename: str  # noqa: F821
) -> str:
    key = f"projects/{project.id}/files/{filename}"
    bucket = qfieldcloud.core.utils.get_s3_bucket()
    bucket.upload_fileobj(
        file,
        key,
    )
    return key


def delete_all_project_files_permanently(project_id: str) -> None:
    prefix = f"projects/{project_id}/"

    if not re.match(r"^projects/[\w]{8}(-[\w]{4}){3}-[\w]{12}/$", prefix):
        raise RuntimeError(
            f"Suspicious S3 deletion of all project files with {prefix=}"
        )

    _delete_by_prefix_versioned(prefix)


def delete_project_file_permanently(project: "Project", filename: str):  # noqa: F821
    file = qfieldcloud.core.utils.get_project_file_with_versions(project.id, filename)

    if not file:
        raise Exception(
            f"No file with such name in the given project found {filename=}"
        )

    with transaction.atomic():
        if qfieldcloud.core.utils.is_qgis_project_file(filename):
            project.project_filename = None
            project.save(recompute_storage=True, update_fields=["project_filename"])

        # NOTE auditing the file deletion in the transation might be costly, but guarantees the audit
        audit(
            project,
            LogEntry.Action.DELETE,
            changes={f"{filename} ALL": [file.latest.e_tag, None]},
        )

        if not re.match(
            r"^projects/[\w]{8}(-[\w]{4}){3}-[\w]{12}/.+$", file.latest.key
        ):
            raise RuntimeError(f"Suspicious S3 file deletion {file.latest.key=}")

        _delete_by_key_permanently(file.latest.key)


def delete_project_file_version_permanently(
    project: "Project",  # noqa: F821
    filename: str,
    version_id: str,
    include_older: bool = False,
) -> List[qfieldcloud.core.utils.S3ObjectVersion]:
    """Deletes a specific version of given file.

    Args:
        project (Project): project the file belongs to
        filename (str): filename the version belongs to
        version_id (str): version id to delete
        include_older (bool, optional): when True, versions older than the passed `version` will also be deleted. If the version_id is the latest version of a file, this parameter will treated as False. Defaults to False.

    Returns:
        int: the number of versions deleted
    """
    file = qfieldcloud.core.utils.get_project_file_with_versions(project.id, filename)

    if not file:
        raise Exception(
            f"No file with such name in the given project found {filename=} {version_id=}"
        )

    if file.latest.id == version_id:
        include_older = False

        if len(file.versions) == 1:
            raise RuntimeError(
                "Forbidded attempt to delete a specific file version which is the only file version available."
            )

    versions_to_delete: List[qfieldcloud.core.utils.S3ObjectVersion] = []

    for file_version in file.versions:
        if file_version.id == version_id:
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
                    r"^projects/[\w]{8}(-[\w]{4}){3}-[\w]{12}/.+$",
                    file_version._data.key,
                )
                or not file_version.id
            ):
                raise RuntimeError(
                    f"Suspicious S3 file version deletion {filename=} {version_id=} {include_older=}"
                )

            audit_suffix = file_version.display

            audit(
                project,
                LogEntry.Action.DELETE,
                changes={f"{filename} {audit_suffix}": [file_version.e_tag, None]},
            )

            delete_version_permanently(file_version)

    project.save(recompute_storage=True)

    return versions_to_delete


def get_stored_package_ids(project_id: str) -> Set[str]:
    bucket = qfieldcloud.core.utils.get_s3_bucket()
    prefix = f"projects/{project_id}/packages/"
    root_path = PurePath(prefix)
    package_ids = set()

    for file in bucket.objects.filter(Prefix=prefix):
        file_path = PurePath(file.key)
        parts = file_path.relative_to(root_path).parts
        package_ids.add(parts[0])

    return package_ids


def delete_stored_package(project_id: str, package_id: str) -> None:
    prefix = f"projects/{project_id}/packages/{package_id}/"

    if not re.match(r"^projects/[\w]{8}(-[\w]{4}){3}-[\w]{12}/packages/\w+/$", prefix):
        raise RuntimeError(
            f"Suspicious S3 deletion on stored project package {project_id=} {package_id=}"
        )

    _delete_by_prefix_permanently(prefix)
