import os

from django.utils.translation import ugettext_lazy as _

from leprikon.site.settings import *

# Application definition
INSTALLED_APPS = (
    [
        "svcspektrum",
    ]
    + INSTALLED_APPS
    + [
        "cms_articles",
    ]
)

DATABASES["boskovice"] = {
    "ENGINE": "django.db.backends.mysql",
    "NAME": "svcboskovice",
    "HOST": "mysql-boskovice",
    "USER": "svcboskovice",
    "PASSWORD": os.environ.get("BOSKOVICE_DATABASE_PASSWORD"),
    "OPTIONS": {"charset": "utf8mb4", "use_unicode": True},
}

DATABASES["letovice"] = {
    "ENGINE": "django.db.backends.mysql",
    "NAME": "svcletovice",
    "HOST": "mysql-letovice",
    "USER": "svcletovice",
    "PASSWORD": os.environ.get("LETOVICE_DATABASE_PASSWORD"),
    "OPTIONS": {"charset": "utf8mb4", "use_unicode": True},
}

# search settings
HAYSTACK_CONNECTIONS = {
    "default": {
        "ENGINE": "haystack.backends.whoosh_backend.WhooshEngine",
        "PATH": os.path.join(BASE_DIR, "data", "search"),
    },
    "cs": {
        "ENGINE": "haystack.backends.whoosh_backend.WhooshEngine",
        "PATH": os.path.join(BASE_DIR, "data", "search"),
    },
}
