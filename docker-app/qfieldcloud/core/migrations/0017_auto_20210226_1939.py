# Generated by Django 2.2.17 on 2021-02-26 19:39

import django.core.validators
import qfieldcloud.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0016_generate_user_account"),
    ]

    operations = [
        migrations.AlterField(
            model_name="project",
            name="name",
            field=models.CharField(
                help_text="Project name. Should start with a letter and contain only letters, numbers, underscores and hyphens.",
                max_length=255,
                validators=[
                    django.core.validators.RegexValidator(
                        "^[-a-zA-Z0-9_]+$",
                        "Only letters, numbers, underscores or hyphens are allowed.",
                    ),
                    django.core.validators.RegexValidator(
                        "^.{3,}$", "The name must be at least 3 characters long."
                    ),
                    django.core.validators.RegexValidator(
                        "^[a-zA-Z].*$", "The name must begin with a letter."
                    ),
                    qfieldcloud.core.validators.reserved_words_validator,
                ],
            ),
        ),
        migrations.AlterField(
            model_name="project",
            name="overwrite_conflicts",
            field=models.BooleanField(
                default=True,
                help_text="If enabled, QFieldCloud will automatically overwrite conflicts in this project. Disabling this will force the project manager to manually resolve all the conflicts.",
            ),
        ),
        migrations.AlterField(
            model_name="project",
            name="private",
            field=models.BooleanField(
                default=False,
                help_text="Projects that are not marked as private would be visible and editable to anyone.",
            ),
        ),
    ]
