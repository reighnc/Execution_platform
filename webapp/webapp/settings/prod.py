from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

DEBUG = False

ALLOWED_HOSTS = [
    "localhost",
    "127.0.0.1",
    "157.245.103.14",
    "54.227.88.249"
]

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [Path.joinpath(BASE_DIR, "frontend", "gui", "build")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "maruth-execution-platform",
        "USER": "123456789",
        "PASSWORD": "987654321",
        "HOST": "localhost",
        "PORT": 5432
    }
}

STATIC_URL = "/static/"

STATICFILES_DIRS = [
    Path.joinpath(BASE_DIR, "frontend", "gui", "build", "static"),
    Path.joinpath(BASE_DIR, "frontend", "gui", "build")
]

STATIC_ROOT = Path.joinpath(BASE_DIR, "staticfiles")

STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"
