from uuid import UUID

from django.core.management.base import BaseCommand
from qfieldcloud.core.utils_local import get_projects_dir, list_files
from qfieldcloud.core.models import Project
from qfieldcloud.core.utils2.storage import delete_all_project_files_permanently


class Command(BaseCommand):
    help = "Delete orphaned project files when the project in the DB is deleted."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--limit", type=int, default=100)

    def get_orphaned_project_ids(self, project_ids: set[str]) -> set[str]:

        orphaned_project_ids: set[str] = set()
        existing_project_ids = Project.objects.filter(
            id__in=list(project_ids),
        ).values_list("id", flat=True)

        if len(existing_project_ids) < len(project_ids):
            existing_project_ids = [str(uid) for uid in existing_project_ids]

            for project_id in project_ids:
                if project_id not in existing_project_ids:
                    orphaned_project_ids.add(project_id)

        return orphaned_project_ids

    def handle(self, *args, **options):

        dry_run: bool | None = options.get("dry_run")
        limit: int | None = options.get("limit")

        project_ids: set[str] = set()
        orphaned_project_ids: set[str] = set()

        if dry_run:
            self.stdout.write("Dry run, no files will be deleted.")

        for f in list_files(get_projects_dir(), ""):
            project_id = f.name[:36]

            try:
                UUID(project_id)
            except Exception:
                self.stdout.write(f"Invalid uuid: {str(project_id)}")
                continue

            project_ids.add(project_id)

            # check for every `limit` projects if they exist, to keep the SQL query short and fast enough
            if len(project_ids) == limit:
                self.stdout.write(
                    f"Checking a batch of {limit} project ids from the storage..."
                )
                orphaned_project_ids |= self.get_orphaned_project_ids(project_ids)
                project_ids = set()

        if len(project_ids) > 0:
            self.stdout.write(
                f"Checking the last {len(project_ids)} project id(s) from the storage..."
            )
            orphaned_project_ids |= self.get_orphaned_project_ids(project_ids)

        if len(orphaned_project_ids) == 0:
            self.stdout.write("No project files to delete.")
            return

        # we need to sort the project ids to make the sorting predictable for testing purposes
        for project_id in sorted(orphaned_project_ids):
            self.stdout.write(f'Deleting project files for "{project_id}"...')

            if not dry_run:
                delete_all_project_files_permanently(project_id)
