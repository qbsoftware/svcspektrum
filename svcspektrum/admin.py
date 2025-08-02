from django.contrib import admin

from .models import ImportedIdsMap


@admin.register(ImportedIdsMap)
class ImportedIdsMapAdmin(admin.ModelAdmin):
    list_display = ("connection", "model_name", "foreign_id", "local_id")
    list_filter = ("connection", "model_name")

    def get_queryset(self, request):
        return super().get_queryset(request).order_by("connection", "model_name", "foreign_id")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
