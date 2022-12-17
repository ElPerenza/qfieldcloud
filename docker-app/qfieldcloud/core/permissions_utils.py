from typing import List, Literal, Union

from deprecated import deprecated
from django.utils.translation import gettext as _
from qfieldcloud.core.models import (
    Delta,
    Organization,
    OrganizationMember,
    OrganizationQueryset,
    Project,
    ProjectCollaborator,
    ProjectQueryset,
    Team,
)
from qfieldcloud.core.models import User as QfcUser


class CheckPermError(Exception):
    ...


class AlreadyCollaboratorError(CheckPermError):
    ...


class ReachedCollaboratorLimitError(CheckPermError):
    ...


class UserHasProjectRoleOrigins(CheckPermError):
    ...


class UserOrganizationRoleError(CheckPermError):
    ...


class TeamOrganizationRoleError(CheckPermError):
    ...


class ExpectedPremiumUserError(CheckPermError):
    ...


def _project_for_owner(user: QfcUser, project: Project, skip_invalid: bool):
    return (
        Project.objects.for_user(user, skip_invalid)
        .select_related(None)
        .filter(pk=project.pk)
    )


def _organization_of_owner(user: QfcUser, organization: Organization):
    return (
        Organization.objects.of_user(user)
        .select_related(None)
        .filter(pk=organization.pk)
    )


def user_has_project_roles(
    user: QfcUser,
    project: Project,
    roles: List[ProjectCollaborator.Roles],
    skip_invalid: bool = False,
):
    return (
        _project_for_owner(user, project, skip_invalid)
        .filter(user_role__in=roles)
        .exists()
    )


def check_user_has_project_role_origins(
    user: QfcUser, project: Project, origins: List[ProjectQueryset.RoleOrigins]
) -> Literal[True]:
    if (
        _project_for_owner(user, project, skip_invalid=False)
        .filter(user_role_origin__in=origins)
        .exists()
    ):
        return True

    raise UserHasProjectRoleOrigins(
        'User "{}" has not role origins "{}" on project {}.'.format(
            user.username,
            [origin.name for origin in origins],
            project.name,
        )
    )


def user_has_project_role_origins(
    user: QfcUser, project: Project, origins: List[ProjectQueryset.RoleOrigins]
) -> bool:
    try:
        return check_user_has_project_role_origins(user, project, origins)
    except CheckPermError:
        return False


def check_user_has_organization_roles(
    user: QfcUser, organization: Organization, roles: List[OrganizationMember.Roles]
) -> Literal[True]:
    if (
        _organization_of_owner(user, organization)
        .filter(membership_role__in=roles)
        .exists()
    ):
        return True

    raise UserOrganizationRoleError(
        _('User "{}" does not have {} roles in organization "{}"').format(
            user.username,
            [role.name for role in roles],
            organization.username,
        )
    )


def user_has_organization_roles(
    user: QfcUser, organization: Organization, roles: List[OrganizationMember.Roles]
) -> bool:
    try:
        return check_user_has_organization_roles(user, organization, roles)
    except CheckPermError:
        return False


def user_has_organization_role_origins(
    user: QfcUser,
    organization: Organization,
    origins: List[OrganizationQueryset.RoleOrigins],
):
    return (
        _organization_of_owner(user, organization)
        .filter(membership_role_origin__in=origins)
        .exists()
    )


def get_param_from_request(request, param):
    """Try to get the param from the request data or the request
    context, returns None otherwise"""

    result = request.data.get(param, None)
    if not result:
        result = request.parser_context["kwargs"].get(param, None)
    return result


def can_create_project(
    user: QfcUser, organization: Union[QfcUser, Organization] = None
) -> bool:
    """Return True if the `user` can create a project. Accepts additional
    `organizaiton` to check whether the user has permissions to do so on
    that organization. Return False otherwise."""

    if organization is None:
        return True
    if user == organization:
        return True

    if organization.is_organization:
        if not isinstance(organization, Organization):
            organization = organization.organization  # type: ignore
    else:
        return False

    if user_has_organization_roles(
        user, organization, [OrganizationMember.Roles.ADMIN]
    ):
        return True

    return False


def can_access_project(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
            ProjectCollaborator.Roles.REPORTER,
            ProjectCollaborator.Roles.READER,
        ],
    )


def can_retrieve_project(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
            ProjectCollaborator.Roles.REPORTER,
            ProjectCollaborator.Roles.READER,
        ],
        True,
    )


def can_update_project(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
        ],
    )


def can_delete_project(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
        ],
    )


def can_create_files(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
            ProjectCollaborator.Roles.REPORTER,
        ],
    )


def can_read_projects(user: QfcUser, _account: QfcUser) -> bool:
    return user.is_authenticated


def can_read_public_projects(user: QfcUser) -> bool:
    return user.is_authenticated


def can_read_files(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
            ProjectCollaborator.Roles.REPORTER,
            ProjectCollaborator.Roles.READER,
        ],
    )


def can_delete_files(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
        ],
    )


def can_create_deltas(user: QfcUser, project: Project) -> bool:
    """Whether the user can store deltas in a project."""
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
            ProjectCollaborator.Roles.REPORTER,
        ],
    )


def can_read_deltas(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
            ProjectCollaborator.Roles.REPORTER,
        ],
    )


def can_apply_pending_deltas_for_project(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
        ],
    )


@deprecated("Use `can_set_delta_status_for_project` instead")
def can_apply_deltas(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
            ProjectCollaborator.Roles.REPORTER,
        ],
    )


@deprecated("Use `can_set_delta_status_for_project` instead")
def can_overwrite_deltas(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
        ],
    )


def can_set_delta_status_for_project(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
        ],
    )


def can_set_delta_status(user: QfcUser, delta: Delta) -> bool:
    if not can_set_delta_status_for_project(user, delta.project):
        return False

    if delta.last_status not in (
        Delta.Status.PENDING,
        Delta.Status.CONFLICT,
        Delta.Status.NOT_APPLIED,
        Delta.Status.ERROR,
        Delta.Status.APPLIED,
        Delta.Status.IGNORED,
        Delta.Status.UNPERMITTED,
    ):
        return False

    return True


def can_create_delta(user: QfcUser, delta: Delta) -> bool:
    """Whether the user can store given delta."""
    project: Project = delta.project

    if user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
        ],
    ):
        return True

    if user_has_project_roles(user, project, [ProjectCollaborator.Roles.REPORTER]):
        if delta.method == Delta.Method.Create:
            return True

    return False


@deprecated("Use `can_set_delta_status` instead")
def can_retry_delta(user: QfcUser, delta: Delta) -> bool:
    if not can_apply_deltas(user, delta.project):
        return False

    if delta.last_status not in (
        Delta.Status.CONFLICT,
        Delta.Status.NOT_APPLIED,
        Delta.Status.ERROR,
    ):
        return False

    return True


@deprecated("Use `can_set_delta_status` instead")
def can_overwrite_delta(user: QfcUser, delta: Delta) -> bool:
    if not can_overwrite_deltas(user, delta.project):
        return False

    if delta.last_status not in (Delta.Status.CONFLICT):
        return False

    return True


@deprecated("Use `can_set_delta_status` instead")
def can_ignore_delta(user: QfcUser, delta: Delta) -> bool:
    if not can_apply_deltas(user, delta.project):
        return False

    if delta.last_status not in (
        Delta.Status.CONFLICT,
        Delta.Status.NOT_APPLIED,
        Delta.Status.ERROR,
    ):
        return False

    return True


def can_read_jobs(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
            ProjectCollaborator.Roles.REPORTER,
        ],
    )


def can_create_secrets(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
        ],
    )


def can_delete_secrets(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
        ],
    )


def can_list_users_organizations(user: QfcUser) -> bool:
    """Return True if the `user` can list users and organizations.
    Return False otherwise."""

    return True


def can_create_organizations(user: QfcUser) -> bool:
    return user.is_authenticated


def can_update_user(user: QfcUser, account: QfcUser) -> bool:
    if user == account:
        return True

    if user_has_organization_roles(user, account, [OrganizationMember.Roles.ADMIN]):
        return True

    return False


def can_delete_user(user: QfcUser, account: QfcUser) -> bool:
    if user == account:
        return True

    if user_has_organization_roles(user, account, [OrganizationMember.Roles.ADMIN]):
        return True

    return False


def can_create_collaborators(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
        ],
    )


def can_read_collaborators(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
        ],
    )


def can_update_collaborators(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
        ],
    )


def can_delete_collaborators(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
        ],
    )


def can_read_packages(user: QfcUser, project: Project) -> bool:
    return user_has_project_roles(
        user,
        project,
        [
            ProjectCollaborator.Roles.ADMIN,
            ProjectCollaborator.Roles.MANAGER,
            ProjectCollaborator.Roles.EDITOR,
            ProjectCollaborator.Roles.REPORTER,
            ProjectCollaborator.Roles.READER,
        ],
    )


def can_create_members(user: QfcUser, organization: Organization) -> bool:
    """Return True if the `user` can create members (incl. teams) of `organization`.
    Return False otherwise."""

    return user_has_organization_roles(
        user, organization, [OrganizationMember.Roles.ADMIN]
    )


def can_read_members(user: QfcUser, organization: Organization) -> bool:
    """Return True if the `user` can list members (incl. teams) of `organization`.
    Return False otherwise."""

    if not organization.is_organization:
        return False

    return True


def can_update_members(user: QfcUser, organization: Organization) -> bool:
    return user_has_organization_roles(
        user, organization, [OrganizationMember.Roles.ADMIN]
    )


def can_delete_members(user: QfcUser, organization: Organization) -> bool:
    return user_has_organization_roles(
        user, organization, [OrganizationMember.Roles.ADMIN]
    )


def check_can_become_collaborator(user: QfcUser, project: Project) -> bool:
    if user == project.owner:
        raise AlreadyCollaboratorError(
            _("Cannot add the project owner as a collaborator.")
        )

    if project.collaborators.filter(collaborator=user).count() > 0:
        raise AlreadyCollaboratorError(
            _('The user "{}" is already a collaborator of project "{}".').format(
                user.username, project.name
            )
        )

    max_premium_collaborators_per_private_project = (
        project.owner.useraccount.plan.max_premium_collaborators_per_private_project
    )
    if max_premium_collaborators_per_private_project >= 0 and not project.is_public:
        project_collaborators_count = project.direct_collaborators.count()
        if project_collaborators_count >= max_premium_collaborators_per_private_project:
            raise ReachedCollaboratorLimitError(
                _(
                    "The subscription plan of the project owner does not allow any additional collaborators. "
                    "Please remove some collaborators first."
                )
            )

    # Rules for organization projects
    if project.owner.is_organization:
        if user.is_team:
            if (
                Team.objects.filter(
                    pk=user.pk,
                    team_organization=project.owner,
                ).count()
                != 1
            ):
                raise TeamOrganizationRoleError(
                    _(
                        'The team "{}" is not owned by the "{}" organization that owns the project.'
                    ).format(
                        user.username,
                        project.owner.username,
                    )
                )
        else:
            # And only members of these organizations can join
            check_user_has_organization_roles(
                user,
                project.owner,
                [OrganizationMember.Roles.MEMBER, OrganizationMember.Roles.ADMIN],
            )
    else:
        if user.is_team:
            raise TeamOrganizationRoleError(
                _(
                    "Teams can be added as collaborators only in projects owned by organizations."
                )
            )

        # Rules for private projects
        if not project.is_public:
            if not user.useraccount.plan.is_premium:
                raise ExpectedPremiumUserError(
                    _(
                        "Only premium users can be added as collaborators on private projects."
                    ).format(user.username)
                )

    return True


def can_become_collaborator(user: QfcUser, project: Project) -> bool:
    try:
        return check_can_become_collaborator(user, project)
    except CheckPermError:
        return False


def can_read_geodb(user: QfcUser, profile: QfcUser) -> bool:
    if not hasattr(profile, "useraccount") or not profile.useraccount.is_geodb_enabled:
        return False

    if can_update_user(user, profile):
        return True

    return False


def can_create_geodb(user: QfcUser, profile: QfcUser) -> bool:
    if not hasattr(profile, "useraccount") or not profile.useraccount.is_geodb_enabled:
        return False

    if profile.has_geodb:
        return False

    if can_update_user(user, profile):
        return True

    return False


def can_delete_geodb(user: QfcUser, profile: QfcUser) -> bool:
    if not hasattr(profile, "useraccount") or not profile.useraccount.is_geodb_enabled:
        return False

    if not profile.has_geodb:
        return False

    if can_update_user(user, profile):
        return True

    return False


def can_become_member(user: QfcUser, organization: Organization) -> bool:
    if user.user_type == QfcUser.TYPE_ORGANIZATION:
        return False

    if user.user_type == QfcUser.TYPE_TEAM:
        return Team.objects.get(pk=user.pk).team_organization == organization

    return not user_has_organization_role_origins(
        user,
        organization,
        [
            OrganizationQueryset.RoleOrigins.ORGANIZATIONOWNER,
            OrganizationQueryset.RoleOrigins.ORGANIZATIONMEMBER,
        ],
    )


def can_send_invitations(user: QfcUser, account: QfcUser) -> bool:
    if user.pk != account.pk:
        return False

    if account.is_user:
        return True

    return False
