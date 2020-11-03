import os
import getpass

from django.conf import settings

def get_commcare_credentials():
    """
    Looks for the COMMCARE_HQ_USERNAME and COMMCARE_HQ_PASSWORD in the environment
    variables or the Django settings.
    Which ever ones it does not find, it'll prompt the user for.
    """
    # first try the Django settings
    if hasattr(settings, 'COMMCARE_HQ_USERNAME') and settings.COMMCARE_HQ_USERNAME != '':
        username = settings.COMMCARE_HQ_USERNAME
    else:
        # try the environment variable for username
        if 'COMMCARE_HQ_USERNAME' not in os.environ or settings.ENV('COMMCARE_HQ_USERNAME') == '':
            username = input("Enter your CommcareHQ Username: ")
        else:
            username = settings.ENV('COMMCARE_HQ_USERNAME')

    # now try the password
    if hasattr(settings, 'COMMCARE_HQ_PASSWORD') and settings.COMMCARE_HQ_PASSWORD != '':
        password = settings.COMMCARE_HQ_PASSWORD
    else:
        if 'COMMCARE_HQ_PASSWORD' not in os.environ or settings.ENV('COMMCARE_HQ_PASSWORD') == '':
            password = getpass.getpass("Enter your CommcareHQ Password: ")
        else:
            password = settings.ENV('COMMCARE_HQ_PASSWORD')

    return {
        'username': username,
        'password': password
    }
