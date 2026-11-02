import re

from django import forms

from .models import Tweet, User


class UserForm(forms.ModelForm):
    class Meta:
        model = User
        fields = ["username", "display_name", "bio"]

    def clean_username(self):
        username = self.cleaned_data["username"].lower()
        if not re.fullmatch(r"[a-z0-9_]+", username):
            raise forms.ValidationError("Use letters, digits and underscores only.")
        return username


class TweetForm(forms.ModelForm):
    class Meta:
        model = Tweet
        fields = ["text", "image"]
        widgets = {
            "text": forms.Textarea(
                attrs={"rows": 3, "maxlength": 280, "placeholder": "What's happening?"}
            ),
        }
        labels = {"text": ""}
