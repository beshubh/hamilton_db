from .models import User

COOKIE = "acting_user"
SALT = "social.acting_user"


def acting_user(get_response):
    """Sets request.acting_user from a signed cookie, so no session table is needed."""

    def middleware(request):
        request.acting_user = None
        user_id = request.get_signed_cookie(COOKIE, default="", salt=SALT)
        if user_id.isdigit():
            request.acting_user = User.objects.filter(pk=int(user_id)).first()
        return get_response(request)

    return middleware
