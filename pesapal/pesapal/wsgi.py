# @Felix 2026

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pesapal.settings')

application = get_wsgi_application()
