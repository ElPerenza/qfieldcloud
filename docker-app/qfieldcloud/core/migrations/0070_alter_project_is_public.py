# Generated by Django 3.2.18 on 2023-06-30 08:02

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0069_auto_20230616_0827"),
    ]

    operations = [
        migrations.AlterField(
            model_name="project",
            name="is_public",
            field=models.BooleanField(
                default=False,
                help_text="Projects marked as public are visible to (but not editable by) anyone.",
            ),
        ),
    ]
