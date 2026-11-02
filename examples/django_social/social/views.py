from django.db.models import Q
from django.http import HttpResponseBadRequest
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_POST

from .forms import TweetForm, UserForm
from .middleware import COOKIE, SALT
from .models import Tweet, User

TIMELINE_SIZE = 50


def with_related(tweets):
    return tweets.select_related("author", "reply_to__author", "quote_of__author")


def timeline(request):
    tweets = with_related(Tweet.objects.filter(reply_to__isnull=True))[:TIMELINE_SIZE]
    return render(request, "social/timeline.html", {"tweets": tweets, "form": TweetForm()})


def compose(request):
    if request.acting_user is None:
        return redirect("users")
    form = TweetForm(request.POST or None, request.FILES or None)
    if request.method == "POST" and form.is_valid():
        form.instance.author = request.acting_user
        tweet = form.save()
        return redirect(tweet)
    return render(request, "social/compose.html", {"form": form})


def tweet_detail(request, pk):
    tweet = get_object_or_404(with_related(Tweet.objects.all()), pk=pk)
    root_id = tweet.thread_root_id or tweet.pk
    thread = list(
        with_related(Tweet.objects.filter(Q(pk=root_id) | Q(thread_root_id=root_id))).order_by(
            "created_at", "id"
        )
    )
    by_id = {t.pk: t for t in thread}
    children = {}
    for t in thread:
        children.setdefault(t.reply_to_id, []).append(t)

    ancestors = []
    parent = by_id.get(tweet.reply_to_id)
    while parent is not None:
        ancestors.append(parent)
        parent = by_id.get(parent.reply_to_id)
    ancestors.reverse()

    replies = []

    def walk(parent_id, depth):
        for child in children.get(parent_id, []):
            replies.append((child, depth))
            walk(child.pk, depth + 1)

    walk(tweet.pk, 0)
    return render(
        request,
        "social/tweet_detail.html",
        {"tweet": tweet, "ancestors": ancestors, "replies": replies, "form": TweetForm()},
    )


def reply(request, pk):
    parent = get_object_or_404(Tweet, pk=pk)
    if request.acting_user is None:
        return redirect("users")
    form = TweetForm(request.POST or None, request.FILES or None)
    if request.method == "POST" and form.is_valid():
        form.instance.author = request.acting_user
        form.instance.reply_to = parent
        form.instance.thread_root_id = parent.thread_root_id or parent.pk
        form.save()
        return redirect(parent)
    return render(request, "social/compose.html", {"form": form, "reply_to": parent})


def quote(request, pk):
    quoted = get_object_or_404(with_related(Tweet.objects.all()), pk=pk)
    if request.acting_user is None:
        return redirect("users")
    form = TweetForm(request.POST or None, request.FILES or None)
    if request.method == "POST" and form.is_valid():
        form.instance.author = request.acting_user
        form.instance.quote_of = quoted
        tweet = form.save()
        return redirect(tweet)
    return render(request, "social/compose.html", {"form": form, "quoted": quoted})


def profile(request, username):
    user = get_object_or_404(User, username=username)
    tweets = with_related(user.tweets.all())[:TIMELINE_SIZE]
    return render(request, "social/profile.html", {"profile": user, "tweets": tweets})


def users(request):
    form = UserForm(request.POST or None)
    if request.method == "POST" and form.is_valid():
        user = form.save()
        response = redirect(user)
        response.set_signed_cookie(COOKIE, str(user.pk), salt=SALT, httponly=True, samesite="Lax")
        return response
    return render(request, "social/users.html", {"form": form, "users": User.objects.all()})


@require_POST
def switch_user(request):
    user_id = request.POST.get("user", "")
    next_url = request.POST.get("next") or reverse("timeline")
    if not url_has_allowed_host_and_scheme(next_url, {request.get_host()}):
        next_url = reverse("timeline")
    response = redirect(next_url)
    if user_id == "":
        response.delete_cookie(COOKIE)
        return response
    if not user_id.isdigit():
        return HttpResponseBadRequest("bad user")
    user = get_object_or_404(User, pk=int(user_id))
    response.set_signed_cookie(COOKIE, str(user.pk), salt=SALT, httponly=True, samesite="Lax")
    return response
