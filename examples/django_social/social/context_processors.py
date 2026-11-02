from .models import User


def acting_user(request):
    return {
        "acting_user": getattr(request, "acting_user", None),
        "all_users": User.objects.all(),
    }
