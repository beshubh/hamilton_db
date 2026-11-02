import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "dev-only-not-secret")
DEBUG = os.environ.get("DJANGO_DEBUG", "1") == "1"
ALLOWED_HOSTS = ["127.0.0.1", "localhost", "testserver"]

# No admin, auth, sessions or contenttypes: none of their tables are needed.
INSTALLED_APPS = [
    "django.contrib.staticfiles",
    "social",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "social.middleware.acting_user",
]

ROOT_URLCONF = "social_site.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "social.context_processors.acting_user",
            ],
        },
    },
]

WSGI_APPLICATION = "social_site.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": os.environ.get("HAMILTON_ENGINE", "hamilton_backend"),
        # Hamilton serves one database per server and ignores the name.
        "NAME": "hamilton",
        "USER": "django",
        "HOST": os.environ.get("HAMILTON_HOST", "127.0.0.1"),
        "PORT": os.environ.get("HAMILTON_PORT", "6942"),
    }
}

USE_TZ = True
TIME_ZONE = "UTC"
LANGUAGE_CODE = "en-us"
USE_I18N = False

STATIC_URL = "static/"
MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

TEST_RUNNER = "social_site.test_runner.HamiltonTestRunner"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "loggers": {
        "django.db.backends": {
            "handlers": ["console"],
            "level": os.environ.get("SQL_LOG_LEVEL", "WARNING"),
        },
    },
}
