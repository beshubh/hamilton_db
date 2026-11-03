from django.urls import path

from . import views

urlpatterns = [
    path("", views.timeline, name="timeline"),
    path("compose/", views.compose, name="compose"),
    path("tweet/<int:pk>/", views.tweet_detail, name="tweet_detail"),
    path("tweet/<int:pk>/reply/", views.reply, name="reply"),
    path("tweet/<int:pk>/quote/", views.quote, name="quote"),
    path("u/<str:username>/", views.profile, name="profile"),
    path("users/", views.users, name="users"),
    path("switch/", views.switch_user, name="switch_user"),
]
