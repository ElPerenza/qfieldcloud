from django.contrib.auth import get_user_model
from django.core.exceptions import ObjectDoesNotExist
from django.utils.decorators import method_decorator
from drf_yasg.utils import swagger_auto_schema
from qfieldcloud.core import pagination, permissions_utils
from qfieldcloud.core.models import Project, ProjectCollaborator
from qfieldcloud.core.serializers import ProjectCollaboratorSerializer
from rest_framework import generics, permissions, status
from rest_framework.response import Response
from rest_framework.settings import api_settings

User = get_user_model()


class ListCreateCollaboratorsViewPermissions(permissions.BasePermission):
    def has_permission(self, request, view):
        user = request.user
        projectid = permissions_utils.get_param_from_request(request, "projectid")
        try:
            project = Project.objects.get(id=projectid)
        except ObjectDoesNotExist:
            return False

        if request.method == "GET":
            return permissions_utils.can_read_collaborators(user, project)
        if request.method == "POST":
            return permissions_utils.can_create_collaborators(user, project)
        return False


@method_decorator(
    name="get",
    decorator=swagger_auto_schema(
        operation_description="Get all collaborators to the given project. Results are paginated: use 'limit' (integer) to limit the number of results and/or 'offset' (integer) to skip results in the reponse.",
        operation_id="Get collaborators of project",
    ),
)
@method_decorator(
    name="post",
    decorator=swagger_auto_schema(
        operation_description="Add a user as collaborator of the project",
        operation_id="Add collaborator to project",
    ),
)
class ListCreateCollaboratorsView(generics.ListCreateAPIView):

    permission_classes = [
        permissions.IsAuthenticated,
        ListCreateCollaboratorsViewPermissions,
    ]
    serializer_class = ProjectCollaboratorSerializer
    pagination_class = pagination.QfcLimitOffsetPagination()

    def get_queryset(self):

        project_id = self.request.parser_context["kwargs"]["projectid"]
        project_obj = Project.objects.get(id=project_id)

        return ProjectCollaborator.objects.filter(project=project_obj)

    def post(self, request, projectid):

        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        collaborator = User.objects.get(username=request.data["collaborator"])
        project = Project.objects.get(id=projectid)
        serializer.save(collaborator=collaborator, project=project)

        try:
            headers = {"Location": str(serializer.data[api_settings.URL_FIELD_NAME])}
        except (TypeError, KeyError):
            headers = {}

        return Response(
            serializer.data, status=status.HTTP_201_CREATED, headers=headers
        )


class GetUpdateDestroyCollaboratorViewPermissions(permissions.BasePermission):
    def has_permission(self, request, view):
        user = request.user
        projectid = permissions_utils.get_param_from_request(request, "projectid")

        try:
            project = Project.objects.get(id=projectid)
        except ObjectDoesNotExist:
            return False

        if request.method == "GET":
            return permissions_utils.can_read_collaborators(user, project)
        if request.method in ["PUT", "PATCH"]:
            return permissions_utils.can_update_collaborators(user, project)
        if request.method in ["DELETE"]:
            return permissions_utils.can_delete_collaborators(user, project)
        return False


@method_decorator(
    name="get",
    decorator=swagger_auto_schema(
        operation_description="Get the role of a collaborator",
        operation_id="Get role of collaborator to project",
    ),
)
@method_decorator(
    name="put",
    decorator=swagger_auto_schema(
        operation_description="Update a collaborator to the project",
        operation_id="Update collaborator",
    ),
)
@method_decorator(
    name="patch",
    decorator=swagger_auto_schema(
        operation_description="Partial update of collaborator to the project",
        operation_id="Patch collaborator",
    ),
)
@method_decorator(
    name="delete",
    decorator=swagger_auto_schema(
        operation_description="Remove a collaborator from the project",
        operation_id="Delete collaborator",
    ),
)
class GetUpdateDestroyCollaboratorView(generics.RetrieveUpdateDestroyAPIView):

    permission_classes = [
        permissions.IsAuthenticated,
        GetUpdateDestroyCollaboratorViewPermissions,
    ]
    serializer_class = ProjectCollaboratorSerializer

    def get_object(self):
        project_id = self.request.parser_context["kwargs"]["projectid"]
        collaborator = self.request.parser_context["kwargs"]["username"]

        project_obj = Project.objects.get(id=project_id)
        collaborator_obj = User.objects.get(username=collaborator)
        return ProjectCollaborator.objects.get(
            project=project_obj, collaborator=collaborator_obj
        )
