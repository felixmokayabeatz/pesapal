# @Felix 2026

from django.urls import path, include

urlpatterns = [
    path('', include('pesapal_app.urls')),
]