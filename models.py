from django.db import models
from django.conf import settings


class FormControl(models.Model):
    """
    This model allows us to control which forms are enabled for ingestion.
    This model also allows us to mantain the relationship between models (parent/child)
        as form loops will need to be children of their parents.
    """
    form_name = models.CharField(max_length=1024, blank=True, null=True)
    form_parent = models.ForeignKey(
        'FormControl', related_name='children',
        on_delete=models.CASCADE, null=True, blank=True
    )
    # name of the spreadsheet to query
    sheet_name = models.CharField(max_length=4096, blank=True, null=True)
    # do we want to ingest this form on the next run?
    ingest = models.BooleanField(default=False)

    def __str__(self):
        return self.form_name
    
    # custom save model
    def save(self, *args, **kwargs):
        # if enabled, recursively enable/disable all children forms based
        # on what this form's ingestion state is.
        if hasattr(settings, 'FORM_CHILDREN_PROPOGATE') and settings.FORM_CHILDREN_PROPOGATE:
            for child_form in self.children.all():
                child_form.ingest = self.ingest
                child_form.save()

        super(FormControl, self).save()


class CaseControl(models.Model):
    """
    This model allows us to control which cases are enabled for ingestion.
    """
    case_name = models.CharField(max_length=1024, blank=True, null=True)
    # name of the spreadsheet to query
    sheet_name = models.CharField(max_length=4096, blank=True, null=True)
    # do we want to ingest this query on the next run?
    ingest = models.BooleanField(default=False)

    def __str__(self):
        return self.case_name


class CommCareManager(models.Manager):
    """
    Manager for selecting the commcare database for certain models.
    """
    def get_queryset(self):
        qs = super().get_queryset()
        commcare_db = 'default'
        if hasattr(settings, 'COMMCARE_DB') and settings.COMMCARE_DB != '':
            commcare_db = settings.COMMCARE_HQ_USERNAME
        else:
            # try the environment variable for username
            if 'COMMCARE_DB' in os.environ and settings.ENV('COMMCARE_DB') == '':
                commcare_db = settings.ENV('COMMCARE_DB')
        return qs.using(commcare_db)


class CommCareBaseAbstractModel(models.Model):
    objects = CommCareManager()

    class Meta:
        abstract = True