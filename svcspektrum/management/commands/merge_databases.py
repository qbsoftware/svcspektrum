import logging
import subprocess
from typing import Callable
from uuid import uuid4

from bankreader.models import Account, AccountStatement
from bankreader.models import Transaction as BankTransaction
from cms.api import create_page
from cms.constants import TEMPLATE_INHERITANCE_MAGIC
from django.conf import settings
from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import F, Model
from filer.models import File, Folder, Image
from menus.menu_pool import menu_pool
from user_unique_email.models import User

from leprikon.models.activities import (
    ActivityGroup,
    ActivityModel,
    ActivityType,
    ActivityTypeAttachment,
    ActivityVariant,
    Registration,
)
from leprikon.models.agegroup import AgeGroup
from leprikon.models.agreements import Agreement, AgreementOption
from leprikon.models.calendar import CalendarEvent, CalendarExport, Resource, ResourceGroup
from leprikon.models.citizenship import Citizenship
from leprikon.models.courses import Course, CourseDiscount, CourseRegistrationPeriod
from leprikon.models.department import Department
from leprikon.models.events import Event, EventDiscount
from leprikon.models.journals import Journal, JournalEntry, JournalLeaderEntry, JournalTime
from leprikon.models.leprikonsite import LeprikonSite
from leprikon.models.activities import Activity
from leprikon.models.messages import Message, MessageAttachment, MessageRecipient
from leprikon.models.orderables import Orderable, OrderableDiscount
from leprikon.models.organizations import Organization
from leprikon.models.place import Place
from leprikon.models.printsetup import PrintSetup
from leprikon.models.question import Question
from leprikon.models.refundrequest import RefundRequest
from leprikon.models.registrationlink import RegistrationLink
from leprikon.models.roles import BillingInfo, GroupContact, Leader, Parent, Participant
from leprikon.models.school import School
from leprikon.models.schoolyear import SchoolYear, SchoolYearDivision, SchoolYearPeriod
from leprikon.models.statgroup import StatGroup
from leprikon.models.targetgroup import TargetGroup
from leprikon.models.timesheets import Timesheet, TimesheetEntry, TimesheetEntryType, TimesheetPeriod
from leprikon.models.transaction import Transaction
from svcspektrum.models import ImportedIdsMap

logger = logging.getLogger(__name__)


def vanilla_model(model_cls: type[Model], name=None) -> type[Model]:
    name = name or f"Vanilla{model_cls.__name__}"
    fields = {}
    for original_field in model_cls._meta.local_fields:
        field = original_field.clone()
        if getattr(field, "auto_now", False):
            field.auto_now = False
        if getattr(field, "auto_now_add", False):
            field.auto_now_add = False
        fields[original_field.name] = field
    for original_field in model_cls._meta.local_many_to_many:
        fields[original_field.name] = original_field.clone()

    Meta = type(
        "Meta",
        (),
        dict(
            app_label="vanilla",
            db_table=model_cls._meta.db_table,
        ),
    )
    fields["__module__"] = __name__
    fields["Meta"] = Meta

    return type(name, (Model,), fields)


File = vanilla_model(File)
Image = vanilla_model(Image)


import_connections = tuple(sorted(c for c in settings.DATABASES.keys() if c != "default"))


def get_ids_map() -> dict[str, dict[int, int]]:
    return {connection: {} for connection in import_connections}


def copy(original: Model) -> Model:
    kwargs = {f.attname: getattr(original, f.attname) for f in original._meta.fields if not f.primary_key}
    return original.__class__(**kwargs)


def save(obj: Model) -> Model:
    obj.save(using="default", force_insert=True)
    return obj


def ensure_unique_username(username: str, email: str, usernames: set[str]) -> str:
    if username not in usernames:
        return username
    possible_username = email.split("@")[0]
    if possible_username not in usernames:
        return possible_username
    if email not in usernames:
        return email
    while possible_username in usernames:
        possible_username = f"{username[:100]}-{uuid4().hex[:3]}"
    logger.warning("created random username %s for email %s", possible_username, email)
    return possible_username


class PersistentIdsMap(dict[str, dict[int, int]]):
    def __init__(self, model_name: str) -> None:
        self.model_name = model_name
        for connection in import_connections:
            self[connection] = {
                foreign_id: local_id
                for foreign_id, local_id in ImportedIdsMap.objects.using("default")
                .filter(model_name=model_name, connection=connection)
                .values_list("foreign_id", "local_id")
            }

    def save(self) -> None:
        for connection in import_connections:
            ImportedIdsMap.objects.using("default").bulk_create(
                (
                    ImportedIdsMap(
                        model_name=self.model_name, connection=connection, foreign_id=foreign_id, local_id=local_id
                    )
                    for foreign_id, local_id in self[connection].items()
                ),
                ignore_conflicts=True,
            )


class Command(BaseCommand):
    help = f"Merge data from all non-default databases {import_connections} into the default database"

    def handle(self, *args, **options):
        self.perform_operation("Performing local migrations", self.perform_local_migrations, True)
        self.perform_operation("Configuring site", self.configure_site)
        self.perform_operation("Fixing broken files", self.fix_broken_files)
        self.perform_operation("Performing foreign migrations", self.perform_foreign_migrations, True)
        self.perform_operation("Loading permissions", self.load_auth_permissions_map)
        self.perform_operation("Merging groups", self.merge_groups)
        self.perform_operation("Merging users", self.merge_users)
        self.perform_operation("Merging folders", self.merge_folders)
        self.perform_operation("Merging files", self.merge_files)
        self.perform_operation("Merging bank accounts", self.merge_bank_accounts)
        self.perform_operation("Merging bank account statements", self.merge_bank_account_statements)
        self.perform_operation("Merging bank account transactions", self.merge_bank_account_transactions)
        self.perform_operation("Merging print setups", self.merge_print_setups)
        self.perform_operation("Merging organizations", self.merge_organizations)
        self.perform_operation("Merging departments", self.merge_departments)
        self.perform_operation("Merging places", self.merge_places)
        self.perform_operation("Merging questions", self.merge_questions)
        self.perform_operation("Merging school years", self.merge_school_years)
        self.perform_operation("Merging school year divisions", self.merge_school_year_divisions)
        self.perform_operation("Merging school year periods", self.merge_school_year_periods)
        self.perform_operation("Merging stat groups", self.merge_stat_groups)
        self.perform_operation("Merging age groups", self.merge_age_groups)
        self.perform_operation("Merging target groups", self.merge_target_groups)
        self.perform_operation("Merging citizenships", self.merge_citizenships)
        self.perform_operation("Merging schools", self.merge_schools)
        self.perform_operation("Merging leaders", self.merge_leaders)
        self.perform_operation("Merging parents", self.merge_parents)
        self.perform_operation("Merging participants", self.merge_participants)
        self.perform_operation("Merging group contacts", self.merge_group_contacts)
        self.perform_operation("Merging billing infos", self.merge_billing_infos)
        self.perform_operation("Merging agreements", self.merge_agreements)
        self.perform_operation("Merging agreement options", self.merge_agreement_options)
        self.perform_operation("Merging activity types", self.merge_activity_types)
        self.perform_operation("Creating activity type pages", self.create_activity_type_pages)
        self.perform_operation("Merging activity groups", self.merge_activity_groups)
        self.perform_operation("Merging resources", self.merge_resources)
        self.perform_operation("Merging resource groups", self.merge_resource_groups)
        self.perform_operation("Fixing activities", self.fix_activities)
        self.perform_operation("Merging activities", self.merge_activities)
        self.perform_operation("Merging activity variants", self.merge_activity_variants)
        self.perform_operation("Merging calendar events", self.merge_calendar_events)
        self.perform_operation("Merging calendar exports", self.merge_calendar_exports)
        self.perform_operation("Merging registration links", self.merge_registration_links)
        self.perform_operation("Merging registrations", self.merge_registrations)
        self.perform_operation("Merging course registration periods", self.merge_course_registration_periods)
        self.perform_operation("Merging refund requests", self.merge_refund_requests)
        self.perform_operation("Merging discounts", self.merge_discounts)
        self.perform_operation("Merging transactions", self.merge_transactions)
        self.perform_operation("Merging timesheets", self.merge_timesheets)
        self.perform_operation("Merging timesheet entries", self.merge_timesheet_entries)
        self.perform_operation("Merging journals", self.merge_journals)
        self.perform_operation("Merging journal entries", self.merge_journal_entries)
        self.stdout.write(self.style.SUCCESS("Successfully merged all data"))

    def perform_operation(self, label: str, func: Callable[[], None], long: bool = False):
        self.stdout.write(f"{label} ... ", ending="\n" if long else " ")
        result_prefix = f"{label} ... " if long else ""
        try:
            with transaction.atomic():
                func()
        except Exception:
            self.stdout.write(f"{result_prefix}\u274c")
            raise
        # show checkmark
        self.stdout.write(f"{result_prefix}\u2705")

    def perform_local_migrations(self):
        if subprocess.run(["leprikon", "migrate"]).returncode != 0:
            raise Exception("Failed to migrate database")

    def configure_site(self):
        site: LeprikonSite = LeprikonSite.objects.get_current()
        site.domain = "svcspektrum.cz"
        site.name = "SVČ Spektrum"
        site.save()

    def fix_broken_files(self):
        # some of the files in the database doesn't really exist
        # and it makes migrations fail
        for connection in import_connections:
            File.objects.using(connection).filter(polymorphic_ctype__model="image", file="").update(
                file="filer_dummy/placeholder.png"
            )
            File.objects.using(connection).filter(polymorphic_ctype__model="file", file="").update(
                file="filer_dummy/placeholder.txt"
            )

    def perform_foreign_migrations(self):
        for connection in import_connections:
            result = subprocess.run(
                [
                    "leprikon",
                    "migrate",
                    "--settings",
                    f"svcspektrum.{connection}.settings",
                ]
            )
            if result.returncode != 0:
                raise Exception(f"Failed to migrate database {connection}")

    def load_auth_permissions_map(self):
        self.permission_ids_map = get_ids_map()
        local_permission_ids = {
            permission.natural_key(): permission.id
            for permission in Permission.objects.using("default").select_related("content_type")
        }
        for connection in import_connections:
            for foreign_permission in Permission.objects.using(connection).select_related("content_type"):
                local_permission_id = local_permission_ids.get(foreign_permission.natural_key())
                if local_permission_id:
                    self.permission_ids_map[connection][foreign_permission.id] = local_permission_id

    def merge_groups(self):
        self.group_ids_map = PersistentIdsMap("auth-groups")
        for connection in import_connections:
            for foreign_group in Group.objects.using(connection).prefetch_related("permissions"):
                if foreign_group.id in self.group_ids_map[connection]:
                    continue
                group = save(copy(foreign_group))
                group.permissions.set(
                    self.permission_ids_map[connection][permission.id]
                    for permission in foreign_group.permissions.all()
                    if permission.id in self.permission_ids_map[connection]
                )
                self.group_ids_map[connection][foreign_group.id] = group.id
        self.group_ids_map.save()

    def merge_users(self):
        self.user_ids_map = get_ids_map()
        all_users = list(User.objects.using("default").all())
        users_by_email: dict[str, User] = {user.email.lower(): user for user in all_users}
        usernames: set[str] = {user.username for user in all_users}
        for connection in import_connections:
            for foreign_user in User.objects.using(connection).prefetch_related("groups", "user_permissions"):
                # normalize email
                foreign_user.email = foreign_user.email.lower()
                local_user = users_by_email.get(foreign_user.email)
                if local_user is None:
                    # save user
                    local_user = copy(foreign_user)
                    local_user.username = ensure_unique_username(foreign_user.username, foreign_user.email, usernames)
                    save(local_user)
                    users_by_email[foreign_user.email] = local_user
                    usernames.add(local_user.username)
                local_user_updated = False
                if foreign_user.first_name and not local_user.first_name:
                    local_user.first_name = foreign_user.first_name
                    local_user_updated = True
                if foreign_user.last_name and not local_user.last_name:
                    local_user.last_name = foreign_user.last_name
                    local_user_updated = True
                if foreign_user.is_active and not local_user.is_active:
                    local_user.is_active = True
                    local_user_updated = True
                if foreign_user.is_staff and not local_user.is_staff:
                    local_user.is_staff = True
                    local_user_updated = True
                if foreign_user.is_superuser and not local_user.is_superuser:
                    local_user.is_superuser = True
                    local_user_updated = True
                if foreign_user.date_joined < local_user.date_joined:
                    local_user.date_joined = foreign_user.date_joined
                    local_user_updated = True
                if local_user_updated:
                    logger.debug("updated user %s", local_user)
                    local_user.save()
                # save groups
                for group in foreign_user.groups.all():
                    local_user.groups.add(self.group_ids_map[connection][group.id])
                # save permissions
                for permission in foreign_user.user_permissions.all():
                    if permission.id in self.permission_ids_map[connection]:
                        local_user.user_permissions.add(self.permission_ids_map[connection][permission.id])
                self.user_ids_map[connection][foreign_user.id] = local_user.id

    def merge_folders(self):
        self.folder_ids_map = get_ids_map()

        def merge_folder(connection: str, local_parent_folder: Folder, foreign_folder: Folder):
            local_folder = Folder.objects.using("default").get_or_create(
                parent=local_parent_folder,
                name=foreign_folder.name,
                defaults=dict(
                    owner_id=self.user_ids_map[connection].get(foreign_folder.owner_id),
                    uploaded_at=foreign_folder.uploaded_at,
                    created_at=foreign_folder.created_at,
                    modified_at=foreign_folder.modified_at,
                ),
            )[0]
            self.folder_ids_map[connection][foreign_folder.id] = local_folder.id
            for foreign_child_folder in foreign_folder.children.using(connection).all():
                merge_folder(connection, local_folder, foreign_child_folder)

        # merge folders
        for connection in import_connections:
            root_folder = Folder.objects.using("default").get_or_create(name=connection.capitalize(), parent=None)[0]
            for foreign_root_folder in Folder.objects.using(connection).filter(parent=None):
                merge_folder(connection, root_folder, foreign_root_folder)

    def merge_files(self):
        self.file_ids_map = get_ids_map()
        content_type_ids_by_models = {
            ct.model: ct.id for ct in ContentType.objects.using("default").filter(app_label="filer")
        }

        def get_key(f: File):
            return (f.folder_id, f.sha1)

        existing_file_ids = {get_key(f): f.id for f in File.objects.using("default").all()}
        for connection in import_connections:
            for foreign_file in (
                File.objects.using(connection).select_related("polymorphic_ctype").order_by("-modified_at")
            ):
                local_file = copy(foreign_file)
                local_file.folder_id = self.folder_ids_map[connection].get(foreign_file.folder_id)
                key = get_key(local_file)
                if key not in existing_file_ids:
                    local_file.polymorphic_ctype_id = content_type_ids_by_models[foreign_file.polymorphic_ctype.model]
                    local_file.owner_id = self.user_ids_map[connection].get(foreign_file.owner_id)
                    local_file.save(using="default")
                    existing_file_ids[key] = local_file.id
                self.file_ids_map[connection][foreign_file.id] = existing_file_ids[key]

        existing_image_ids = {i.pk for i in Image.objects.using("default").all()}
        for connection in import_connections:
            for foreign_image in Image.objects.using(connection).all():
                local_image = copy(foreign_image)
                local_image.pk = self.file_ids_map[connection][foreign_image.pk]
                if local_image.pk not in existing_image_ids:
                    local_image.save(using="default", force_insert=True)
                    existing_image_ids.add(local_image.pk)

    def merge_bank_accounts(self):
        self.bank_account_ids_map = get_ids_map()
        for connection in import_connections:
            for foreign_account in Account.objects.using(connection).all():
                local_account = Account.objects.using("default").get_or_create(
                    name=foreign_account.name,
                    defaults=dict(
                        iban=foreign_account.iban,
                        bic=foreign_account.bic,
                        reader=foreign_account.reader,
                    ),
                )[0]
                self.bank_account_ids_map[connection][foreign_account.id] = local_account.id

    def merge_bank_account_statements(self):
        self.bank_account_statement_ids_map = get_ids_map()
        existing_account_statements = {
            (statement.account_id, statement.statement): statement.id
            for statement in AccountStatement.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_statement in AccountStatement.objects.using(connection).all():
                local_account_id = self.bank_account_ids_map[connection][foreign_statement.account_id]
                local_statement_id = existing_account_statements.get((local_account_id, foreign_statement.statement))
                if local_statement_id is None:
                    local_statement_id = (
                        AccountStatement.objects.using("default")
                        .create(
                            account_id=local_account_id,
                            statement=foreign_statement.statement,
                            from_date=foreign_statement.from_date,
                            to_date=foreign_statement.to_date,
                        )
                        .id
                    )
                self.bank_account_statement_ids_map[connection][foreign_statement.id] = local_statement_id

    def merge_bank_account_transactions(self):
        self.bank_transaction_ids_map = get_ids_map()
        existing_transactions = {
            (transaction.account_id, transaction.transaction_id): transaction.id
            for transaction in BankTransaction.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_transaction in BankTransaction.objects.using(connection).all():
                local_account_id = self.bank_account_ids_map[connection][foreign_transaction.account_id]
                local_statement_id = self.bank_account_statement_ids_map[connection][
                    foreign_transaction.account_statement_id
                ]
                local_transaction_id = existing_transactions.get((local_account_id, foreign_transaction.transaction_id))
                if local_transaction_id is None:
                    transaction = copy(foreign_transaction)
                    transaction.account_id = local_account_id
                    transaction.account_statement_id = local_statement_id
                    local_transaction_id = save(transaction).id
                self.bank_transaction_ids_map[connection][foreign_transaction.id] = local_transaction_id

    def merge_print_setups(self):
        self.print_setup_ids_map = get_ids_map()
        existing_print_setups = {
            print_setup.name: print_setup.id for print_setup in PrintSetup.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_print_setup in PrintSetup.objects.using(connection).all():
                local_print_setup_id = existing_print_setups.get(foreign_print_setup.name)
                if local_print_setup_id is None:
                    local_print_setup = copy(foreign_print_setup)
                    local_print_setup.background_id = (
                        foreign_print_setup.background_id
                        and self.file_ids_map[connection][foreign_print_setup.background_id]
                    )
                    local_print_setup_id = save(local_print_setup).id
                self.print_setup_ids_map[connection][foreign_print_setup.id] = local_print_setup_id

    def merge_organizations(self):
        self.organization_ids_map = get_ids_map()
        organizations_by_iban = {
            organization.iban: organization for organization in Organization.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_organization in Organization.objects.using(connection).all():
                foreign_organization_id = foreign_organization.id
                local_organization = organizations_by_iban.get(foreign_organization.iban)
                if not local_organization:
                    local_organization = copy(foreign_organization)
                    if local_organization.donation_print_setup_id:
                        local_organization.donation_print_setup_id = self.print_setup_ids_map[connection][
                            local_organization.donation_print_setup_id
                        ]
                    save(local_organization)
                    organizations_by_iban[foreign_organization.iban] = local_organization
                self.organization_ids_map[connection][foreign_organization_id] = local_organization.id

    def merge_departments(self):
        self.department_ids_map = get_ids_map()
        existing_departments = {
            department.name: department.id for department in Department.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_department in Department.objects.using(connection).all():
                local_department_id = existing_departments.get(foreign_department.name)
                if local_department_id is None:
                    local_department_id = save(copy(foreign_department)).id
                    existing_departments[foreign_department.name] = local_department_id
                self.department_ids_map[connection][foreign_department.id] = local_department_id

    def merge_places(self):
        self.place_ids_map = get_ids_map()
        existing_places = {place.name: place.id for place in Place.objects.using("default").all()}
        for connection in import_connections:
            for foreign_place in Place.objects.using(connection).all():
                local_place_id = existing_places.get(foreign_place.name)
                if local_place_id is None:
                    local_place_id = save(copy(foreign_place)).id
                    existing_places[foreign_place.name] = local_place_id
                self.place_ids_map[connection][foreign_place.id] = local_place_id

    def merge_questions(self):
        self.question_ids_map = get_ids_map()
        existing_questions = {question.slug: question.id for question in Question.objects.using("default").all()}
        for connection in import_connections:
            for foreign_question in Question.objects.using(connection).all():
                local_question_id = existing_questions.get(foreign_question.slug)
                if local_question_id is None:
                    local_question_id = save(copy(foreign_question)).id
                    existing_questions[foreign_question.slug] = local_question_id
                self.question_ids_map[connection][foreign_question.id] = local_question_id

    def merge_school_years(self):
        self.school_year_ids_map = get_ids_map()
        # ensure years are created in the right order
        all_years = set(
            school_year.year
            for connection in import_connections
            for school_year in SchoolYear.objects.using(connection).all()
        )
        school_years_by_year = {
            school_year.year: school_year for school_year in SchoolYear.objects.using("default").all()
        }
        for year in sorted(all_years):
            if year not in school_years_by_year:
                school_years_by_year[year] = SchoolYear.objects.using("default").create(year=year)
        # merge them
        for connection in import_connections:
            for foreign_school_year in SchoolYear.objects.using(connection).all():
                foreign_school_year_id = foreign_school_year.id
                local_school_year = school_years_by_year[foreign_school_year.year]
                self.school_year_ids_map[connection][foreign_school_year_id] = local_school_year.id
                if foreign_school_year.active and not local_school_year.active:
                    local_school_year.active = True
                    local_school_year.save()

    def merge_school_year_divisions(self):
        self.school_year_division_ids_map = get_ids_map()
        existing_divisions = {
            (division.school_year_id, division.name): division.id
            for division in SchoolYearDivision.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_division in SchoolYearDivision.objects.using(connection).all():
                school_year_id = self.school_year_ids_map[connection][foreign_division.school_year_id]
                name = foreign_division.name
                key = (school_year_id, name)
                local_division_id = existing_divisions.get(key)
                if local_division_id is None:
                    division = copy(foreign_division)
                    division.school_year_id = school_year_id
                    division.save(using="default")
                    local_division_id = division.id
                    existing_divisions[key] = local_division_id
                self.school_year_division_ids_map[connection][foreign_division.id] = local_division_id

    def merge_school_year_periods(self):
        self.school_year_period_ids_map = get_ids_map()
        existing_periods = {
            (period.school_year_division_id, period.name): period.id
            for period in SchoolYearPeriod.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_period in SchoolYearPeriod.objects.using(connection).all():
                school_year_division_id = self.school_year_division_ids_map[connection][
                    foreign_period.school_year_division_id
                ]
                name = foreign_period.name
                key = (school_year_division_id, name)
                local_period_id = existing_periods.get(key)
                if local_period_id is None:
                    period = copy(foreign_period)
                    period.school_year_division_id = school_year_division_id
                    local_period_id = save(period).id
                    existing_periods[key] = local_period_id
                self.school_year_period_ids_map[connection][foreign_period.id] = local_period_id

    def merge_stat_groups(self):
        self.stat_group_ids_map = get_ids_map()
        existing_stat_groups = {
            stat_group.name: stat_group.id for stat_group in StatGroup.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_stat_group in StatGroup.objects.using(connection).all():
                local_stat_group_id = existing_stat_groups.get(foreign_stat_group.name)
                if local_stat_group_id is None:
                    local_stat_group_id = save(copy(foreign_stat_group)).id
                    existing_stat_groups[foreign_stat_group.name] = local_stat_group_id
                self.stat_group_ids_map[connection][foreign_stat_group.id] = local_stat_group_id

    def merge_age_groups(self):
        self.age_group_ids_map = get_ids_map()
        existing_age_groups = {age_group.name: age_group.id for age_group in AgeGroup.objects.using("default").all()}
        for connection in import_connections:
            for foreign_age_group in AgeGroup.objects.using(connection).all():
                local_age_group_id = existing_age_groups.get(foreign_age_group.name)
                if local_age_group_id is None:
                    age_group = copy(foreign_age_group)
                    age_group.stat_group_id = self.stat_group_ids_map[connection][foreign_age_group.stat_group_id]
                    local_age_group_id = save(age_group).id
                    existing_age_groups[foreign_age_group.name] = local_age_group_id
                self.age_group_ids_map[connection][foreign_age_group.id] = local_age_group_id

    def merge_target_groups(self):
        self.target_group_ids_map = get_ids_map()
        existing_target_groups = {
            target_group.name: target_group.id for target_group in TargetGroup.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_target_group in TargetGroup.objects.using(connection).all():
                local_target_group_id = existing_target_groups.get(foreign_target_group.name)
                if local_target_group_id is None:
                    target_group = copy(foreign_target_group)
                    target_group.stat_group_id = self.stat_group_ids_map[connection][foreign_target_group.stat_group_id]
                    local_target_group_id = save(target_group).id
                    existing_target_groups[foreign_target_group.name] = local_target_group_id
                self.target_group_ids_map[connection][foreign_target_group.id] = local_target_group_id

    def merge_citizenships(self):
        self.citizenship_ids_map = get_ids_map()
        existing_citizenships = {
            citizenship.name: citizenship.id for citizenship in Citizenship.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_citizenship in Citizenship.objects.using(connection).all():
                local_citizenship_id = existing_citizenships.get(foreign_citizenship.name)
                if local_citizenship_id is None:
                    local_citizenship_id = save(copy(foreign_citizenship)).id
                    existing_citizenships[foreign_citizenship.name] = local_citizenship_id
                self.citizenship_ids_map[connection][foreign_citizenship.id] = local_citizenship_id

    def merge_schools(self):
        self.school_ids_map = get_ids_map()
        existing_schools = {school.name: school.id for school in School.objects.using("default").all()}
        for connection in import_connections:
            for foreign_school in School.objects.using(connection).all():
                local_school_id = existing_schools.get(foreign_school.name)
                if local_school_id is None:
                    local_school_id = save(copy(foreign_school)).id
                    existing_schools[foreign_school.name] = local_school_id
                self.school_ids_map[connection][foreign_school.id] = local_school_id

    def merge_leaders(self):
        self.leader_ids_map = get_ids_map()
        existing_leaders = {leader.user_id: leader.id for leader in Leader.objects.using("default").all()}
        for connection in import_connections:
            for foreign_leader in Leader.objects.using(connection).prefetch_related("school_years", "contacts").all():
                user_id = self.user_ids_map[connection][foreign_leader.user_id]
                local_leader_id = existing_leaders.get(user_id)
                if local_leader_id is None:
                    leader = copy(foreign_leader)
                    leader.user_id = user_id
                    leader.photo_id = foreign_leader.photo_id and self.file_ids_map[connection][foreign_leader.photo_id]
                    leader.page = None
                    local_leader_id = save(leader).id
                    for school_year in foreign_leader.school_years.all():
                        school_year_id = self.school_year_ids_map[connection][school_year.id]
                        leader.school_years.add(school_year_id)
                    for foreign_contact in foreign_leader.contacts.all():
                        contact = copy(foreign_contact)
                        contact.leader_id = local_leader_id
                        save(contact)
                    existing_leaders[user_id] = local_leader_id
                self.leader_ids_map[connection][foreign_leader.id] = local_leader_id

    def merge_parents(self):
        existing_parents = {(parent.user_id, parent.full_name) for parent in Parent.objects.using("default").all()}
        for connection in import_connections:
            for foreign_parent in Parent.objects.using(connection).all():
                user_id = self.user_ids_map[connection][foreign_parent.user_id]
                full_name = foreign_parent.full_name
                key = (user_id, full_name)
                if key not in existing_parents:
                    parent = copy(foreign_parent)
                    parent.user_id = user_id
                    save(parent)
                    existing_parents.add(key)

    def merge_participants(self):
        existing_participants = {
            (participant.user_id, participant.full_name) for participant in Participant.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_participant in Participant.objects.using(connection).all():
                user_id = self.user_ids_map[connection][foreign_participant.user_id]
                full_name = foreign_participant.full_name
                key = (user_id, full_name)
                if key not in existing_participants:
                    participant = copy(foreign_participant)
                    participant.user_id = user_id
                    participant.age_group_id = self.age_group_ids_map[connection][foreign_participant.age_group_id]
                    participant.citizenship_id = self.citizenship_ids_map[connection][
                        foreign_participant.citizenship_id
                    ]
                    participant.school_id = (
                        participant.school_id and self.school_ids_map[connection][participant.school_id]
                    )
                    save(participant)
                    existing_participants.add(key)

    def merge_group_contacts(self):
        existing_group_contacts = {
            (group_contact.user_id, str(group_contact)) for group_contact in GroupContact.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_group_contact in GroupContact.objects.using(connection).all():
                user_id = self.user_ids_map[connection][foreign_group_contact.user_id]
                key = (user_id, str(foreign_group_contact))
                if key not in existing_group_contacts:
                    group_contact = copy(foreign_group_contact)
                    group_contact.user_id = user_id
                    group_contact.target_group_id = self.target_group_ids_map[connection][
                        foreign_group_contact.target_group_id
                    ]
                    group_contact.school_id = (
                        group_contact.school_id and self.school_ids_map[connection][group_contact.school_id]
                    )
                    save(group_contact)
                    existing_group_contacts.add(key)

    def merge_billing_infos(self):
        existing_billing_infos = {
            (billing_info.user_id, billing_info.name) for billing_info in BillingInfo.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_billing_info in BillingInfo.objects.using(connection).all():
                user_id = self.user_ids_map[connection][foreign_billing_info.user_id]
                key = (user_id, foreign_billing_info.name)
                if key not in existing_billing_infos:
                    billing_info = copy(foreign_billing_info)
                    billing_info.user_id = user_id
                    save(billing_info)
                    existing_billing_infos.add(key)

    def merge_agreements(self):
        self.agreement_ids_map = get_ids_map()
        existing_agreements = {agreement.name: agreement.id for agreement in Agreement.objects.using("default").all()}
        for connection in import_connections:
            for foreign_agreement in Agreement.objects.using(connection).all():
                local_agreement_id = existing_agreements.get(foreign_agreement.name)
                if local_agreement_id is None:
                    local_agreement_id = save(copy(foreign_agreement)).id
                    existing_agreements[foreign_agreement.name] = local_agreement_id
                self.agreement_ids_map[connection][foreign_agreement.id] = local_agreement_id

    def merge_agreement_options(self):
        self.agreement_option_ids_map = get_ids_map()
        existing_agreement_options = {
            (agreement_option.agreement_id, agreement_option.name): agreement_option.id
            for agreement_option in AgreementOption.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_agreement_option in AgreementOption.objects.using(connection).all():
                local_agreement_id = self.agreement_ids_map[connection][foreign_agreement_option.agreement_id]
                key = (local_agreement_id, foreign_agreement_option.name)
                local_agreement_option_id = existing_agreement_options.get(key)
                if local_agreement_option_id is None:
                    agreement_option = copy(foreign_agreement_option)
                    agreement_option.agreement_id = local_agreement_id
                    local_agreement_option_id = save(agreement_option).id
                    existing_agreement_options[key] = local_agreement_option_id
                self.agreement_option_ids_map[connection][foreign_agreement_option.id] = local_agreement_option_id

    def merge_activity_types(self):
        self.activity_type_ids_map = get_ids_map()
        existing_activity_types = {
            activity_type.slug: activity_type.id for activity_type in ActivityType.objects.using("default").all()
        }
        existing_attachments = {
            (attachment.activity_type_id, attachment.file_id)
            for attachment in ActivityTypeAttachment.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_activity_type in ActivityType.objects.using(connection).prefetch_related(
                "questions", "registration_agreements", "attachments"
            ):
                local_activity_type_id = existing_activity_types.get(foreign_activity_type.slug)
                if local_activity_type_id is None:
                    activity_type = copy(foreign_activity_type)
                    activity_type.page = None
                    activity_type.reg_print_setup_id = (
                        activity_type.reg_print_setup_id
                        and self.print_setup_ids_map[connection][activity_type.reg_print_setup_id]
                    )
                    activity_type.decision_print_setup_id = (
                        activity_type.decision_print_setup_id
                        and self.print_setup_ids_map[connection][activity_type.decision_print_setup_id]
                    )
                    activity_type.pr_print_setup_id = (
                        activity_type.pr_print_setup_id
                        and self.print_setup_ids_map[connection][activity_type.pr_print_setup_id]
                    )
                    activity_type.bill_print_setup_id = (
                        activity_type.bill_print_setup_id
                        and self.print_setup_ids_map[connection][activity_type.bill_print_setup_id]
                    )
                    activity_type.organization_id = (
                        activity_type.organization_id
                        and self.organization_ids_map[connection][activity_type.organization_id]
                    )
                    local_activity_type_id = save(activity_type).id
                    activity_type.questions.set(
                        self.question_ids_map[connection][question.id]
                        for question in foreign_activity_type.questions.all()
                    )
                    activity_type.registration_agreements.set(
                        self.agreement_ids_map[connection][agreement.id]
                        for agreement in foreign_activity_type.registration_agreements.all()
                    )
                    existing_activity_types[foreign_activity_type.slug] = local_activity_type_id
                for foreign_attachment in foreign_activity_type.attachments.all():
                    key = (local_activity_type_id, foreign_attachment.file_id)
                    if key not in existing_attachments:
                        attachment = copy(foreign_attachment)
                        attachment.activity_type_id = local_activity_type_id
                        attachment.file_id = self.file_ids_map[connection][foreign_attachment.file_id]
                        save(attachment)
                        existing_attachments.add(key)
                self.activity_type_ids_map[connection][foreign_activity_type.id] = local_activity_type_id

    def create_activity_type_pages(self):
        for activity_type in ActivityType.objects.using("default").filter(page=None).all():
            activity_type.page = create_page(
                title=activity_type.plural,
                template=TEMPLATE_INHERITANCE_MAGIC,
                language=settings.LANGUAGE_CODE,
                slug=activity_type.slug,
                apphook="LeprikonActivityTypeApp",
                apphook_namespace=activity_type.slug,
                in_navigation=True,
                published=True,
            )
            activity_type.save()
        menu_pool.clear()

    def merge_activity_groups(self):
        self.activity_group_ids_map = get_ids_map()
        existing_activity_groups = {
            activity_group.name: activity_group.id for activity_group in ActivityGroup.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_activity_group in (
                ActivityGroup.objects.using(connection).prefetch_related("activity_types").all()
            ):
                local_activity_group_id = existing_activity_groups.get(foreign_activity_group.name)
                if local_activity_group_id is None:
                    activity_group = copy(foreign_activity_group)
                    local_activity_group_id = save(activity_group).id
                    activity_group.activity_types.set(
                        self.activity_type_ids_map[connection][activity_type.id]
                        for activity_type in foreign_activity_group.activity_types.all()
                    )
                    existing_activity_groups[foreign_activity_group.name] = local_activity_group_id
                self.activity_group_ids_map[connection][foreign_activity_group.id] = local_activity_group_id

    def merge_resources(self):
        self.resource_ids_map = get_ids_map()
        existing_resources = {resource.name: resource.id for resource in Resource.objects.using("default").all()}
        for connection in import_connections:
            for foreign_resource in Resource.objects.using(connection).prefetch_related("availabilities").all():
                local_resource_id = existing_resources.get(foreign_resource.name)
                if local_resource_id is None:
                    resource = copy(foreign_resource)
                    resource.leader_id = (
                        foreign_resource.leader_id and self.leader_ids_map[connection][foreign_resource.leader_id]
                    )
                    local_resource_id = save(resource).id
                    for foreign_availability in foreign_resource.availabilities.all():
                        availability = copy(foreign_availability)
                        availability.resource_id = local_resource_id
                        save(availability)
                    existing_resources[foreign_resource.name] = local_resource_id
                self.resource_ids_map[connection][foreign_resource.id] = local_resource_id

    def merge_resource_groups(self):
        self.resource_group_ids_map = get_ids_map()
        existing_resource_groups = {
            resource_group.name: resource_group.id for resource_group in ResourceGroup.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_resource_group in ResourceGroup.objects.using(connection).prefetch_related("resources").all():
                local_resource_group_id = existing_resource_groups.get(foreign_resource_group.name)
                if local_resource_group_id is None:
                    resource_group = copy(foreign_resource_group)
                    local_resource_group_id = save(resource_group).id
                    resource_group.resources.set(
                        self.resource_ids_map[connection][resource.id]
                        for resource in foreign_resource_group.resources.all()
                    )
                    existing_resource_groups[foreign_resource_group.name] = local_resource_group_id
                self.resource_group_ids_map[connection][foreign_resource_group.id] = local_resource_group_id

    def fix_activities(self):
        for model, activity_model in (
            (Course, ActivityModel.COURSE),
            (Event, ActivityModel.EVENT),
            (Orderable, ActivityModel.ORDERABLE),
        ):
            for connection in import_connections:
                invalid_activities = model.objects.using(connection).exclude(activity_type__model=activity_model)
                if invalid_activities.count():
                    activity_type_id = ActivityType.objects.using(connection).filter(model=activity_model).first().id
                    invalid_activities.update(activity_type_id=activity_type_id)

    def merge_activities(self):
        self.activity_ids_map = PersistentIdsMap("activities")
        for connection in import_connections:
            for foreign_activity in (
                Activity.objects.using(connection)
                .select_related("activity_type", "course", "event", "orderable")
                .prefetch_related(
                    "groups",
                    "age_groups",
                    "target_groups",
                    "leaders",
                    "questions",
                    "registration_agreements",
                    "times",
                    "attachments",
                )
                .all()
            ):
                if foreign_activity.id in self.activity_ids_map[connection]:
                    continue
                if foreign_activity.activity_type.model == ActivityModel.COURSE:
                    activity = copy(foreign_activity.course)
                elif foreign_activity.activity_type.model == ActivityModel.EVENT:
                    activity = copy(foreign_activity.event)
                elif foreign_activity.activity_type.model == ActivityModel.ORDERABLE:
                    activity = copy(foreign_activity.orderable)
                else:
                    raise Exception("Unknown activity type model: " + foreign_activity.activity_type.model)
                activity.school_year_id = self.school_year_ids_map[connection][foreign_activity.school_year_id]
                activity.activity_type_id = self.activity_type_ids_map[connection][foreign_activity.activity_type_id]
                activity.department_id = (
                    foreign_activity.department_id
                    and self.department_ids_map[connection][foreign_activity.department_id]
                )
                activity.place_id = (
                    foreign_activity.place_id and self.place_ids_map[connection][foreign_activity.place_id]
                )
                activity.photo_id = (
                    foreign_activity.photo_id and self.file_ids_map[connection][foreign_activity.photo_id]
                )
                activity.page_id = None
                activity.reg_print_setup_id = (
                    foreign_activity.reg_print_setup_id
                    and self.print_setup_ids_map[connection][foreign_activity.reg_print_setup_id]
                )
                activity.decision_print_setup_id = (
                    foreign_activity.decision_print_setup_id
                    and self.print_setup_ids_map[connection][foreign_activity.decision_print_setup_id]
                )
                activity.pr_print_setup_id = (
                    foreign_activity.pr_print_setup_id
                    and self.print_setup_ids_map[connection][foreign_activity.pr_print_setup_id]
                )
                activity.bill_print_setup_id = (
                    foreign_activity.bill_print_setup_id
                    and self.print_setup_ids_map[connection][foreign_activity.bill_print_setup_id]
                )
                activity.organization_id = (
                    foreign_activity.organization_id
                    and self.organization_ids_map[connection][foreign_activity.organization_id]
                )
                save(activity)
                activity.groups.set(
                    self.activity_group_ids_map[connection][group.id] for group in foreign_activity.groups.all()
                )
                activity.age_groups.set(
                    self.age_group_ids_map[connection][age_group.id] for age_group in foreign_activity.age_groups.all()
                )
                activity.target_groups.set(
                    self.target_group_ids_map[connection][target_group.id]
                    for target_group in foreign_activity.target_groups.all()
                )
                activity.leaders.set(
                    self.leader_ids_map[connection][leader.id] for leader in foreign_activity.leaders.all()
                )
                activity.questions.set(
                    self.question_ids_map[connection][question.id] for question in foreign_activity.questions.all()
                )
                activity.registration_agreements.set(
                    self.agreement_ids_map[connection][agreement.id]
                    for agreement in foreign_activity.registration_agreements.all()
                )
                for foreign_activity_time in foreign_activity.times.all():
                    activity_time = copy(foreign_activity_time)
                    activity_time.activity_id = activity.id
                    save(activity_time)
                for foreign_activity_attachment in foreign_activity.attachments.all():
                    activity_attachment = copy(foreign_activity_attachment)
                    activity_attachment.activity_id = activity.id
                    activity_attachment.file_id = self.file_ids_map[connection][foreign_activity_attachment.file_id]
                    save(activity_attachment)
                self.activity_ids_map[connection][foreign_activity.id] = activity.id
        self.activity_ids_map.save()

    def merge_activity_variants(self):
        self.activity_variant_ids_map = PersistentIdsMap("activity-variants")
        for connection in import_connections:
            for foreign_variant in (
                ActivityVariant.objects.using(connection)
                .prefetch_related(
                    "age_groups",
                    "target_groups",
                    "required_resources",
                    "required_resource_groups",
                )
                .all()
            ):
                if foreign_variant.id in self.activity_variant_ids_map[connection]:
                    continue
                variant = copy(foreign_variant)
                variant.activity_id = self.activity_ids_map[connection][foreign_variant.activity_id]
                variant.school_year_division_id = (
                    variant.school_year_division_id
                    and self.school_year_division_ids_map[connection][variant.school_year_division_id]
                )
                save(variant)
                variant.age_groups.set(
                    self.age_group_ids_map[connection][age_group.id] for age_group in foreign_variant.age_groups.all()
                )
                variant.target_groups.set(
                    self.target_group_ids_map[connection][target_group.id]
                    for target_group in foreign_variant.target_groups.all()
                )
                variant.required_resources.set(
                    self.resource_ids_map[connection][resource.id]
                    for resource in foreign_variant.required_resources.all()
                )
                variant.required_resource_groups.set(
                    self.resource_group_ids_map[connection][resource_group.id]
                    for resource_group in foreign_variant.required_resource_groups.all()
                )
                self.activity_variant_ids_map[connection][foreign_variant.id] = variant.id
        self.activity_variant_ids_map.save()

    def merge_calendar_events(self):
        self.calendar_event_ids_map = PersistentIdsMap("calendar-events")
        for connection in import_connections:
            for foreign_event in (
                CalendarEvent.objects.using(connection)
                .prefetch_related(
                    "resources",
                    "resource_groups",
                )
                .all()
            ):
                if foreign_event.id in self.calendar_event_ids_map[connection]:
                    continue
                event = copy(foreign_event)
                event.activity_id = self.activity_ids_map[connection][foreign_event.activity_id]
                save(event)
                event.resources.set(
                    self.resource_ids_map[connection][resource.id] for resource in foreign_event.resources.all()
                )
                event.resource_groups.set(
                    self.resource_group_ids_map[connection][resource_group.id]
                    for resource_group in foreign_event.resource_groups.all()
                )
                self.calendar_event_ids_map[connection][foreign_event.id] = event.id
        self.calendar_event_ids_map.save()

    def merge_calendar_exports(self):
        existing_exports = {export.id for export in CalendarExport.objects.using("default").all()}
        for connection in import_connections:
            for foreign_export in CalendarExport.objects.using(connection).prefetch_related("resources").all():
                if foreign_export.id in existing_exports:
                    continue
                export = copy(foreign_export)
                # keep the same uuid
                export.id = foreign_export.id
                save(export)
                export.resources.set(
                    self.resource_ids_map[connection][resource.id] for resource in foreign_export.resources.all()
                )
                existing_exports.add(export.id)

    def merge_registration_links(self):
        self.registration_link_ids_map = get_ids_map()
        existing_links = {link.slug: link.id for link in RegistrationLink.objects.using("default").all()}
        for connection in import_connections:
            for foreign_link in RegistrationLink.objects.using(connection).prefetch_related("activity_variants").all():
                local_link_id = existing_links.get(foreign_link.slug)
                if local_link_id is None:
                    link = copy(foreign_link)
                    link.school_year_id = self.school_year_ids_map[connection][foreign_link.school_year_id]
                    link.activity_type_id = self.activity_type_ids_map[connection][foreign_link.activity_type_id]
                    local_link_id = save(link).id
                    link.activity_variants.set(
                        self.activity_variant_ids_map[connection][activity_variant.id]
                        for activity_variant in foreign_link.activity_variants.all()
                    )
                    existing_links[foreign_link.slug] = local_link_id
                self.registration_link_ids_map[connection][foreign_link.id] = local_link_id

    def merge_registrations(self):
        self.registration_ids_map = PersistentIdsMap("registrations")
        self.registration_participant_ids_map = PersistentIdsMap("registration-participants")
        for connection in import_connections:
            for foreign_registration in (
                Registration.objects.using(connection)
                .annotate(
                    activity_type_model=F("activity__activity_type__model"),
                    registration_type=F("activity__registration_type"),
                    has_billing_info=F("billing_info"),
                )
                .select_related(
                    "courseregistration",
                    "eventregistration",
                    "orderableregistration",
                    "group",
                    "billing_info",
                )
                .prefetch_related(
                    "questions",
                    "agreements",
                    "agreement_options",
                    "participants",
                    "group_members",
                )
                .all()
            ):
                if foreign_registration.id in self.registration_ids_map[connection]:
                    continue
                if foreign_registration.activity_type_model == ActivityModel.COURSE:
                    registration = copy(foreign_registration.courseregistration)
                elif foreign_registration.activity_type_model == ActivityModel.EVENT:
                    registration = copy(foreign_registration.eventregistration)
                elif foreign_registration.activity_type_model == ActivityModel.ORDERABLE:
                    registration = copy(foreign_registration.orderableregistration)
                else:
                    raise Exception("Unknown activity type model: " + foreign_registration.activity_type.model)
                registration.user_id = self.user_ids_map[connection][registration.user_id]
                registration.activity_id = self.activity_ids_map[connection][registration.activity_id]
                registration.activity_variant_id = self.activity_variant_ids_map[connection][
                    registration.activity_variant_id
                ]
                registration.calendar_event_id = (
                    registration.calendar_event_id
                    and self.calendar_event_ids_map[connection][registration.calendar_event_id]
                )
                registration.created_by_id = (
                    registration.created_by_id and self.user_ids_map[connection][registration.created_by_id]
                )
                registration.approved_by_id = (
                    registration.approved_by_id and self.user_ids_map[connection][registration.approved_by_id]
                )
                registration.payment_requested_by_id = (
                    registration.payment_requested_by_id
                    and self.user_ids_map[connection][registration.payment_requested_by_id]
                )
                registration.refund_offered_by_id = (
                    registration.refund_offered_by_id
                    and self.user_ids_map[connection][registration.refund_offered_by_id]
                )
                registration.cancelation_requested_by_id = (
                    registration.cancelation_requested_by_id
                    and self.user_ids_map[connection][registration.cancelation_requested_by_id]
                )
                registration.canceled_by_id = (
                    registration.canceled_by_id and self.user_ids_map[connection][registration.canceled_by_id]
                )
                registration.registration_link_id = (
                    registration.registration_link_id
                    and self.registration_link_ids_map[connection][registration.registration_link_id]
                )
                save(registration)
                registration.created = foreign_registration.created
                registration.save(update_fields=["created"])
                registration.questions.set(
                    self.question_ids_map[connection][question.id] for question in foreign_registration.questions.all()
                )
                registration.agreements.set(
                    self.agreement_ids_map[connection][agreement.id]
                    for agreement in foreign_registration.agreements.all()
                )
                registration.agreement_options.set(
                    self.agreement_option_ids_map[connection][agreement_option.id]
                    for agreement_option in foreign_registration.agreement_options.all()
                )
                if foreign_registration.registration_type == Activity.PARTICIPANTS:
                    for foreign_participant in foreign_registration.participants.all():
                        participant = copy(foreign_participant)
                        participant.registration_id = registration.id
                        participant.citizenship_id = self.citizenship_ids_map[connection][participant.citizenship_id]
                        participant.age_group_id = self.age_group_ids_map[connection][participant.age_group_id]
                        participant.school_id = (
                            participant.school_id and self.school_ids_map[connection][participant.school_id]
                        )
                        save(participant)
                        self.registration_participant_ids_map[connection][foreign_participant.id] = participant.id
                elif foreign_registration.registration_type == Activity.GROUPS:
                    group = copy(foreign_registration.group)
                    group.registration_id = registration.id
                    group.target_group_id = self.target_group_ids_map[connection][group.target_group_id]
                    group.school_id = group.school_id and self.school_ids_map[connection][group.school_id]
                    save(group)
                    for foreign_group_member in foreign_registration.group_members.all():
                        group_member = copy(foreign_group_member)
                        group_member.registration_id = registration.id
                        save(group_member)
                if foreign_registration.has_billing_info:
                    billing_info = copy(foreign_registration.billing_info)
                    billing_info.registration_id = registration.id
                    save(billing_info)
                self.registration_ids_map[connection][foreign_registration.id] = registration.id
        self.registration_ids_map.save()
        self.registration_participant_ids_map.save()

    def merge_course_registration_periods(self):
        existing_registration_periods = {
            (period.registration_id, period.period_id)
            for period in CourseRegistrationPeriod.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_registration_period in CourseRegistrationPeriod.objects.using(connection).all():
                local_registration_id = self.registration_ids_map[connection][
                    foreign_registration_period.registration_id
                ]
                local_period_id = self.school_year_period_ids_map[connection][foreign_registration_period.period_id]
                key = (local_registration_id, local_period_id)
                if key in existing_registration_periods:
                    continue
                registration_period = copy(foreign_registration_period)
                registration_period.registration_id = local_registration_id
                registration_period.period_id = local_period_id
                save(registration_period)

    def merge_refund_requests(self):
        existing_refund_requests = {request.registration_id for request in RefundRequest.objects.using("default").all()}
        for connection in import_connections:
            for foreign_refund_request in RefundRequest.objects.using(connection).all():
                local_registration_id = self.registration_ids_map[connection][foreign_refund_request.registration_id]
                if local_registration_id in existing_refund_requests:
                    continue
                refund_request = copy(foreign_refund_request)
                refund_request.requested_by_id = self.user_ids_map[connection][foreign_refund_request.requested_by_id]
                refund_request.registration_id = local_registration_id
                save(refund_request)

    def merge_discounts(self):
        for model, name in [
            (CourseDiscount, "course-discounts"),
            (EventDiscount, "event-discounts"),
            (OrderableDiscount, "orderable-discounts"),
        ]:
            discount_ids_map = PersistentIdsMap(name)
            for connection in import_connections:
                for foreign_discount in model.objects.using(connection).all():
                    if foreign_discount.id in discount_ids_map[connection] or foreign_discount.amount == 0:
                        continue
                    discount = copy(foreign_discount)
                    discount.registration_id = self.registration_ids_map[connection][discount.registration_id]
                    discount.registration_period_id = getattr(foreign_discount, "registration_period_id", 0)
                    discount.accounted_by_id = (
                        foreign_discount.accounted_by_id
                        and self.user_ids_map[connection][foreign_discount.accounted_by_id]
                    )
                    discount.last_updated_by_id = (
                        foreign_discount.last_updated_by_id
                        and self.user_ids_map[connection][foreign_discount.last_updated_by_id]
                    )
                    save(discount)
                    discount_ids_map[connection][foreign_discount.id] = discount.id
            discount_ids_map.save()

    def merge_transactions(self):
        self.transaction_ids_map = PersistentIdsMap("transactions")
        for connection in import_connections:
            for foreign_transaction in Transaction.objects.using(connection).all():
                if foreign_transaction.id in self.transaction_ids_map[connection]:
                    continue
                transaction: Transaction = copy(foreign_transaction)
                transaction.accounted_by_id = (
                    transaction.accounted_by_id and self.user_ids_map[connection][transaction.accounted_by_id]
                )
                transaction.last_updated_by_id = (
                    transaction.last_updated_by_id and self.user_ids_map[connection][transaction.last_updated_by_id]
                )
                transaction.source_registration_id = (
                    transaction.source_registration_id
                    and self.registration_ids_map[connection][transaction.source_registration_id]
                )
                transaction.target_registration_id = (
                    transaction.target_registration_id
                    and self.registration_ids_map[connection][transaction.target_registration_id]
                )
                transaction.donor_id = transaction.donor_id and self.user_ids_map[connection][transaction.donor_id]
                transaction.organization_id = (
                    transaction.organization_id and self.organization_ids_map[connection][transaction.organization_id]
                )
                transaction.bankreader_transaction_id = (
                    transaction.bankreader_transaction_id
                    and self.bank_transaction_ids_map[connection][transaction.bankreader_transaction_id]
                )
                save(transaction)
                self.transaction_ids_map[connection][foreign_transaction.id] = transaction.id
        self.transaction_ids_map.save()

    def merge_timesheets(self):
        # create all missing timesheet periods
        all_period_ranges = set(
            (tp.start, tp.end)
            for connection in import_connections
            for tp in TimesheetPeriod.objects.using(connection).all()
        )
        TimesheetPeriod.objects.using("default").bulk_create(
            (TimesheetPeriod(start=start, end=end) for start, end in sorted(all_period_ranges)),
            ignore_conflicts=True,
        )
        # map all timesheet periods
        timesheet_period_ids_map = get_ids_map()
        existing_timesheet_periods = {
            (tp.start, tp.end): tp.id for tp in TimesheetPeriod.objects.using("default").all()
        }
        for connection in import_connections:
            for foreign_tp in TimesheetPeriod.objects.using(connection).all():
                key = (foreign_tp.start, foreign_tp.end)
                timesheet_period_ids_map[connection][foreign_tp.id] = existing_timesheet_periods[key]
        self.timesheet_ids_map = PersistentIdsMap("timesheets")
        for connection in import_connections:
            for foreign_timesheet in Timesheet.objects.using(connection).all():
                if foreign_timesheet.id in self.timesheet_ids_map[connection]:
                    continue
                timesheet: Timesheet = copy(foreign_timesheet)
                timesheet.period_id = timesheet_period_ids_map[connection][timesheet.period_id]
                timesheet.leader_id = self.leader_ids_map[connection][timesheet.leader_id]
                save(timesheet)
                self.timesheet_ids_map[connection][foreign_timesheet.id] = timesheet.id
        self.timesheet_ids_map.save()

    def merge_timesheet_entries(self):
        existing_entry_types = {tet.name: tet.id for tet in TimesheetEntryType.objects.using("default").all()}
        entry_type_ids_map = get_ids_map()
        for connection in import_connections:
            for foreign_timesheet_entry_type in TimesheetEntryType.objects.using(connection).all():
                entry_type_ids_map[connection][foreign_timesheet_entry_type.id] = (
                    existing_entry_types.get(foreign_timesheet_entry_type.name)
                    or save(copy(foreign_timesheet_entry_type)).id
                )
        self.timesheet_entry_ids_map = PersistentIdsMap("timesheet-entries")
        for connection in import_connections:
            for foreign_entry in TimesheetEntry.objects.using(connection).all():
                if foreign_entry.id in self.timesheet_entry_ids_map[connection]:
                    continue
                entry: TimesheetEntry = copy(foreign_entry)
                entry.timesheet_id = self.timesheet_ids_map[connection][entry.timesheet_id]
                entry.entry_type_id = entry_type_ids_map[connection][entry.entry_type_id]
                save(entry)
                self.timesheet_entry_ids_map[connection][foreign_entry.id] = entry.id
        self.timesheet_entry_ids_map.save()

    def merge_journals(self):
        self.journal_ids_map = PersistentIdsMap("journals")
        for connection in import_connections:
            for foreign_journal in Journal.objects.using(connection).prefetch_related(
                "leaders", "participants", "times"
            ):
                if foreign_journal.id in self.journal_ids_map[connection]:
                    continue
                journal: Journal = copy(foreign_journal)
                journal.activity_id = self.activity_ids_map[connection][journal.activity_id]
                journal.school_year_division_id = (
                    journal.school_year_division_id
                    and self.school_year_division_ids_map[connection][journal.school_year_division_id]
                )
                save(journal)
                journal.leaders.set(
                    self.leader_ids_map[connection][leader.id] for leader in foreign_journal.leaders.all()
                )
                journal.participants.set(
                    self.registration_participant_ids_map[connection][participant.id]
                    for participant in foreign_journal.participants.all()
                )
                for foreign_journal_time in foreign_journal.times.all():
                    journal_time: JournalTime = copy(foreign_journal_time)
                    journal_time.journal_id = journal.id
                    save(journal_time)
                self.journal_ids_map[connection][foreign_journal.id] = journal.id
        self.journal_ids_map.save()

    def merge_journal_entries(self):
        self.journal_entry_ids_map = PersistentIdsMap("journal-entries")
        for connection in import_connections:
            for foreign_journal_entry in JournalEntry.objects.using(connection).prefetch_related(
                "participants", "participants_instructed", "leader_entries"
            ):
                if foreign_journal_entry.id in self.journal_entry_ids_map[connection]:
                    continue
                journal_entry: JournalEntry = copy(foreign_journal_entry)
                journal_entry.journal_id = self.journal_ids_map[connection][journal_entry.journal_id]
                journal_entry.period_id = (
                    journal_entry.period_id and self.school_year_period_ids_map[connection][journal_entry.period_id]
                )
                save(journal_entry)
                journal_entry.participants.set(
                    self.registration_participant_ids_map[connection][participant.id]
                    for participant in foreign_journal_entry.participants.all()
                )
                journal_entry.participants_instructed.set(
                    self.registration_participant_ids_map[connection][participant.id]
                    for participant in foreign_journal_entry.participants_instructed.all()
                )
                for foreign_leader_entry in foreign_journal_entry.leader_entries.all():
                    leader_entry: JournalLeaderEntry = copy(foreign_leader_entry)
                    leader_entry.journal_entry_id = journal_entry.id
                    leader_entry.timesheet_id = self.timesheet_ids_map[connection][leader_entry.timesheet_id]
                    save(leader_entry)
                self.journal_entry_ids_map[connection][foreign_journal_entry.id] = journal_entry.id
        self.journal_entry_ids_map.save()

    def merge_messages(self):
        self.message_ids_map = PersistentIdsMap("messages")
        for connection in import_connections:
            for foreign_message in Message.objects.using(connection).prefetch_related("recipients", "attachments"):
                if foreign_message.id in self.message_ids_map[connection]:
                    continue
                message: Message = copy(foreign_message)
                message.sender_id = message.sender_id and self.user_ids_map[connection][message.sender_id]
                save(message)
                # override auto_now_add field
                message.created = foreign_message.created
                message.save(update_fields=["created"])
                for foreign_recipient in foreign_message.recipients.all():
                    recipient: MessageRecipient = copy(foreign_recipient)
                    recipient.message_id = message.id
                    recipient.recipient_id = self.user_ids_map[connection][recipient.recipient_id]
                    save(recipient)
                    # override auto_now_add field
                    recipient.sent = foreign_recipient.sent
                    recipient.save(update_fields=["sent"])
                for foreign_attachment in foreign_message.attachments.all():
                    attachment: MessageAttachment = copy(foreign_attachment)
                    attachment.message_id = message.id
                    attachment.file_id = self.file_ids_map[connection][attachment.file_id]
                    save(attachment)
                self.message_ids_map[connection][foreign_message.id] = message.id
        self.message_ids_map.save()
