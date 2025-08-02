from django.db import models


class ImportedIdsMap(models.Model):
    model_name = models.CharField(max_length=100, db_index=True)
    connection = models.CharField(max_length=100, db_index=True)
    foreign_id = models.BigIntegerField()
    local_id = models.BigIntegerField()

    class Meta:
        unique_together = ("connection", "model_name", "foreign_id")
