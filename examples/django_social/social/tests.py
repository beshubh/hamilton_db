import io
import shutil
import tempfile
from datetime import datetime, timedelta, timezone

from django.core.files.uploadedfile import SimpleUploadedFile
from unittest import mock

from django.db import IntegrityError, connection
from django.test import TransactionTestCase, override_settings
from django.urls import reverse
from PIL import Image

from .forms import UserForm
from .models import Tweet, User

MEDIA_ROOT = tempfile.mkdtemp(prefix="social-media-")


def png(name="pic.png"):
    buf = io.BytesIO()
    Image.new("RGB", (4, 4), (200, 60, 10)).save(buf, "PNG")
    return SimpleUploadedFile(name, buf.getvalue(), content_type="image/png")


# Not TestCase: it nests atomic blocks, which needs SAVEPOINT.
@override_settings(MEDIA_ROOT=MEDIA_ROOT)
class SocialTests(TransactionTestCase):
    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        shutil.rmtree(MEDIA_ROOT, ignore_errors=True)

    def make_user(self, username):
        return User.objects.create(username=username, display_name=username.title())

    def act_as(self, user):
        self.client.post(reverse("switch_user"), {"user": user.pk, "next": "/"})

    def post(self, url, text, **extra):
        return self.client.post(url, {"text": text, **extra})

    def test_text_columns_are_varchar(self):
        with connection.cursor() as cursor:
            cursor.execute("\\schema")
            columns = cursor.fetchall()
        types = {(table, column): type_ for table, column, type_, _ in columns}
        self.assertEqual(types[("social_tweet", "text")], "VARCHAR(280)")
        self.assertEqual(types[("social_tweet", "image")], "VARCHAR(200)")
        self.assertEqual(types[("social_tweet", "created_at")], "TIMESTAMPTZ")
        for (table, column), type_ in types.items():
            self.assertNotIn("TEXT", type_, f"{table}.{column}")

    def test_ids_and_timestamps_round_trip(self):
        alice = self.make_user("alice")
        at = datetime(2026, 11, 2, 10, 51, 0, 123456, tzinfo=timezone(timedelta(hours=5)))
        tweet = Tweet.objects.create(author=alice, text="hi", created_at=at)
        self.assertIsNotNone(tweet.pk)
        fetched = Tweet.objects.get(pk=tweet.pk)
        self.assertEqual(fetched.created_at, at)
        self.assertEqual(fetched.created_at.tzinfo, timezone.utc)
        later = Tweet.objects.create(author=alice, text="later")
        self.assertGreater(later.pk, tweet.pk)
        self.assertEqual(
            list(Tweet.objects.filter(created_at__lt=at + timedelta(seconds=1))), [tweet]
        )

    def test_create_user_and_act_as_them(self):
        response = self.client.post(
            reverse("users"), {"username": "Alice", "display_name": "Alice", "bio": "hi"}
        )
        alice = User.objects.get(username="alice")
        self.assertRedirects(response, alice.get_absolute_url())
        page = self.client.get(reverse("timeline")).content.decode()
        self.assertIn(f'<option value="{alice.pk}" selected>@alice</option>', page)

    def test_usernames_are_unique(self):
        self.make_user("alice")
        response = self.client.post(
            reverse("users"), {"username": "alice", "display_name": "Other", "bio": ""}
        )
        self.assertContains(response, "That username is taken.")
        self.assertEqual(User.objects.count(), 1)

    def test_the_database_rejects_a_taken_username(self):
        self.make_user("alice")
        with self.assertRaises(IntegrityError):
            self.make_user("alice")
        self.assertEqual(User.objects.count(), 1)

    def test_a_signup_that_loses_the_race_for_a_name_is_told_so(self):
        # As if another signup took the name after the form checked it.
        self.make_user("alice")
        with mock.patch.object(UserForm, "validate_unique"):
            response = self.client.post(
                reverse("users"), {"username": "alice", "display_name": "Other", "bio": ""}
            )
        self.assertContains(response, "That username is taken.")
        self.assertEqual(User.objects.count(), 1)

    def test_posting_needs_an_acting_user(self):
        response = self.post(reverse("compose"), "nobody")
        self.assertRedirects(response, reverse("users"))
        self.assertFalse(Tweet.objects.exists())

    def test_post_a_tweet_with_an_image(self):
        alice = self.make_user("alice")
        self.act_as(alice)
        response = self.post(reverse("compose"), "look", image=png())
        tweet = Tweet.objects.get()
        self.assertRedirects(response, tweet.get_absolute_url())
        self.assertEqual(tweet.author, alice)
        self.assertTrue(tweet.image.name.startswith("tweets/pic"))
        with tweet.image.open() as f:
            self.assertEqual(Image.open(f).size, (4, 4))
        self.assertContains(self.client.get(reverse("timeline")), tweet.image.url)

    def test_timeline_is_newest_first_without_replies(self):
        alice = self.make_user("alice")
        self.act_as(alice)
        for text in ["first", "second", "third"]:
            self.post(reverse("compose"), text)
        first = Tweet.objects.get(text="first")
        self.post(reverse("reply", args=[first.pk]), "a reply")
        page = self.client.get(reverse("timeline")).content.decode()
        self.assertLess(page.index("third"), page.index("second"))
        self.assertLess(page.index("second"), page.index("first"))
        self.assertNotIn("a reply", page)

    def test_replies_are_threaded_on_the_detail_page(self):
        alice, bob = self.make_user("alice"), self.make_user("bob")
        self.act_as(alice)
        self.post(reverse("compose"), "root")
        root = Tweet.objects.get()
        self.act_as(bob)
        self.post(reverse("reply", args=[root.pk]), "reply one")
        one = Tweet.objects.get(text="reply one")
        self.act_as(alice)
        self.post(reverse("reply", args=[one.pk]), "reply to reply")
        self.post(reverse("reply", args=[root.pk]), "reply two")
        nested = Tweet.objects.get(text="reply to reply")
        self.assertEqual(nested.reply_to, one)
        self.assertEqual(nested.thread_root, root)

        page = self.client.get(root.get_absolute_url()).content.decode()
        self.assertIn("3 replies", page)
        order = [page.index(t) for t in ["root", "reply one", "reply to reply", "reply two"]]
        self.assertEqual(order, sorted(order))
        self.assertIn(f'id="t{nested.pk}" style="margin-left: 24px"', page)

        # A reply's page shows the tweets above it.
        page = self.client.get(nested.get_absolute_url()).content.decode()
        self.assertLess(page.index("root"), page.index("reply one"))
        self.assertLess(page.index("reply one"), page.index("tweet highlight"))

    def test_quote_embeds_the_quoted_tweet(self):
        alice, bob = self.make_user("alice"), self.make_user("bob")
        original = Tweet.objects.create(author=alice, text="quotable")
        self.act_as(bob)
        response = self.post(reverse("quote", args=[original.pk]), "so true")
        quote = Tweet.objects.get(text="so true")
        self.assertRedirects(response, quote.get_absolute_url())
        self.assertEqual(quote.quote_of, original)
        page = self.client.get(quote.get_absolute_url()).content.decode()
        self.assertIn(f'<a class="quoted" href="{original.get_absolute_url()}">', page)
        self.assertIn("quotable", page)

    def test_profile_lists_the_users_tweets(self):
        alice, bob = self.make_user("alice"), self.make_user("bob")
        Tweet.objects.create(author=alice, text="by alice")
        Tweet.objects.create(author=bob, text="by bob")
        page = self.client.get(alice.get_absolute_url())
        self.assertContains(page, "by alice")
        self.assertNotContains(page, "by bob")
        self.assertEqual(self.client.get("/u/nobody/").status_code, 404)
