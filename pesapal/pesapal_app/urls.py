from django.urls import path
from . import views
from .test import test_db_columns

urlpatterns = [
    path('', views.index, name='index'),
    path('users/', views.users_view, name='users'),
    path('users/add/', views.add_user, name='add_user'),
    path('users/edit/<int:user_id>/', views.edit_user, name='edit_user'),
    path('users/delete/<int:user_id>/', views.delete_user, name='delete_user'),
    path('products/', views.products_view, name='products'),
    path('products/add/', views.add_product, name='add_product'),
    path('api/query/', views.api_query, name='api_query'),
    path('api/schema/', views.api_schema, name='api_schema'),
    path('join/', views.run_join, name='join_demo'),
    path('terminal/', views.web_terminal, name='terminal'),

    path('test-columns/', test_db_columns, name='test_columns'),
]