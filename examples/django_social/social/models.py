from django.db import models
from django.urls import reverse
from django.utils import timezone


class User(models.Model):
    username = models.CharField(
        max_length=30, unique=True, error_messages={"unique": "That username is taken."}
    )
    display_name = models.CharField(max_length=50)
    bio = models.CharField(max_length=160, blank=True)

    class Meta:
        ordering = ["username"]

    def __str__(self):
        return f"@{self.username}"

    def get_absolute_url(self):
        return reverse("profile", args=[self.username])


class Tweet(models.Model):
    author = models.ForeignKey(User, on_delete=models.CASCADE, related_name="tweets")
    text = models.CharField(max_length=280)
    # ImageField is a varchar holding the path under MEDIA_ROOT.
    image = models.ImageField(upload_to="tweets/", blank=True, max_length=200)
    reply_to = models.ForeignKey(
        "self", null=True, blank=True, on_delete=models.CASCADE, related_name="replies"
    )
    # The thread's first tweet, so a whole thread loads in one query.
    thread_root = models.ForeignKey(
        "self", null=True, blank=True, on_delete=models.CASCADE, related_name="+"
    )
    quote_of = models.ForeignKey(
        "self", null=True, blank=True, on_delete=models.SET_NULL, related_name="quotes"
    )
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-created_at", "-id"]

    def __str__(self):
        return f"{self.author}: {self.text[:40]}"

    def get_absolute_url(self):
        return reverse("tweet_detail", args=[self.pk])
