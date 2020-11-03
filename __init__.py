from django.conf import settings

if not hasattr(settings, 'MAX_CHARVAR_LENGTH'):
    settings.MAX_CHARVAR_LENGTH = 1024