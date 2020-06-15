from django.views.generic.base import View
from django.http import HttpResponseRedirect
from django.shortcuts import redirect
from django.contrib import messages

def Generic403Redirect(request, exception):
    message = ""
    if exception.args:
        message = exception.args[0]
    messages.error(
        request,
        "You do not have permission to access the requested page. "
        f"If you believe this is in error please contact the administrators. (403 Permission Denied: {message})"

    )
    return redirect("authentication:dashboard")