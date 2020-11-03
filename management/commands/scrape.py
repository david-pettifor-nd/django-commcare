"""
Using the Firefox browser and Selenium, reach out to CommCare HQ and
try to pull down a list of forms, creating spreadsheets to use in the export tool.
"""
import json
import os
import datetime
from io import StringIO
import xml.etree.ElementTree as ET
import requests
import xlsxwriter

from django.conf import settings
from django.core.management import call_command

from ...commcare_tools import get_commcare_credentials
from ...utils import get_table_name_from_excel_file, get_form_table, confirm_table_schema, confirm_table_columns, confirm_case_table_columns, confirm_case_table_schema

from django.core.management.base import BaseCommand

from ...models import FormControl, CaseControl


class Command(BaseCommand):
    help = 'Pull case and form schema information from CommCare HQ and generate query files.'

    def __init__(self):
        self.output_directory = None
        self.forms_as_json = []
        self.form_relations = []
        self.database_table_names = []
        self.cases_as_json = []
        self.form_xml_url = None
        self.commcare_db = 'default'
        self.init_to_write = []

        """
        Construct the form_xml_url based on the project namespace and project identifier (found in the URL of CommCare's portal when you're editing forms)
        Example:
            The URL should look something like this: https://www.commcarehq.org/a/[PROJECT-NAMESPACE]/apps/view/[PROJECT-IDENTIFIER]/
        """
        if (not hasattr(settings, 'PROJECT_NAMESPACE') or settings.PROJECT_NAMESPACE == '') and ('PROJECT_NAMESPACE' not in os.environ or settings.ENV('PROJECT_NAMESPACE') == ''):
            raise ValueError('Must set `PROJECT_NAMESPACE` in your Django settings or environment variable!')

        if (not hasattr(settings, 'PROJECT_IDENTIFIER') or settings.PROJECT_NAMESPACE == '') and ('PROJECT_IDENTIFIER' not in os.environ or settings.ENV('PROJECT_IDENTIFIER') == ''):
            raise ValueError('Must set `PROJECT_IDENTIFIER` in your Django settings or environment variable!')

        if hasattr(settings, 'PROJECT_NAMESPACE') and settings.PROJECT_NAMESPACE != '':
            project_namespace = settings.PROJECT_NAMESPACE
        else:
            project_namespace = settings.ENV('PROJECT_NAMESPACE')

        if hasattr(settings, 'PROJECT_IDENTIFIER') and settings.PROJECT_IDENTIFIER != '':
            project_identifier = settings.PROJECT_IDENTIFIER
        else:
            project_identifier = settings.ENV('PROJECT_IDENTIFIER')

        self.form_xml_url = 'www.commcarehq.org/a/'\
                + project_namespace + '/api/v0.5/application/'+project_identifier + '/'
        
        # figure out which database to use for storing commcare data in (defined with COMMCARE_DB)
        if hasattr(settings, 'COMMCARE_DB') and settings.COMMCARE_DB != '':
            self.commcare_db = settings.COMMCARE_HQ_USERNAME
        else:
            # try the environment variable for username
            if 'COMMCARE_DB' in os.environ and settings.ENV('COMMCARE_DB') == '':
                self.commcare_db = settings.ENV('COMMCARE_DB')


    def find_parent(self, form_obj, parent_name):
        # does this form object's value match the parent name?
        if form_obj['value'] == parent_name:
            return form_obj

        # otherwise, loop through the groups
        for group_obj in form_obj['groups']:
            obj = self.find_parent(group_obj, parent_name)
            if obj is not None:
                return obj

        # if we haven't found it yet, search the loops
        for loop_obj in form_obj['loops']:
            obj = self.find_parent(loop_obj, parent_name)
            if obj is not None:
                return obj

        # if we still haven't returned yet, the parent we're looking for is not in this branch, so return none
        return None

    def get_all_cases(self, credentials):
        """
        Pulls the JSON version of the app structure and creates a list of all cases.
        """
        # load the XML
        login_url = 'https://'+credentials['username']+':'+credentials['password']+'@'+self.form_xml_url+'?format=json'

        response = requests.get(
            login_url,
            auth=(credentials['username'], credentials['password'])
        )
        application_structure = json.loads(response.content)

        case_list = []

        for module in application_structure['modules']:
            if module['case_type'] == '':
                continue
            # does this case already exist in our case list?
            found = False
            for case_obj in case_list:
                if case_obj['name'] == module['case_type']:
                    found = True
                    break
            if found:
                continue

            new_case = {
                'name': module['case_type'],
                'properties': []
            }
            for case_property in module['case_properties']:
                if case_property.startswith('parent/'):
                    continue
                new_case['properties'].append(case_property)

            case_list.append(new_case)

        return case_list
    
    def get_all_forms(self, credentials):
        # load the XML
        login_url = 'https://'+self.form_xml_url+"?format=xml"

        response = requests.get(
            login_url,
            auth=(credentials['username'], credentials['password'])
        )

        # parse that baby out!
        tree = ET.fromstring(response.content)

        master_tree = []

        forms_as_json = []

        # traverse the XML, looking at our forms
        for meta in tree:
            if meta.tag == 'modules':
                for folder in meta:
                    for prop in folder:
                        if prop.tag == 'forms':
                            for xml_form in prop:
                                # setup a new form object (empty for now)
                                form_obj = {
                                    'group': False,
                                    'group_member': None,
                                    'loop_member': None,
                                    'groups': [],
                                    'loops': [],
                                    'questions': [],
                                    'case': False,
                                    'parent': None,
                                    'value': None,
                                    'calculation': None
                                }

                                for p in xml_form:
                                    # grab the form name
                                    if p.tag == 'name':
                                        form_obj['name'] = p[0].text

                                    # grab the XML namespace
                                    if p.tag == 'xmlns':
                                        form_obj['xmlns'] = p.text

                                    # grab the unique ID
                                    if p.tag == 'unique_id':
                                        form_obj['form_id'] = p.text

                                    # look for the questions
                                    if p.tag == 'questions':
                                        # loop through the form questions!
                                        parent_group = None
                                        if parent_group is None:
                                            parent_group = form_obj
                                            master_tree.append(form_obj)

                                        for question in p:
                                            new_question = {
                                                'group': False,
                                                'group_member': None,
                                                'loop_member': None,
                                                'groups': [],
                                                'loops': [],
                                                'questions': [],
                                                'case': False,
                                                'parent': None,
                                                'value': None,
                                                'text': None,
                                                'calculation': None
                                            }

                                            for question_property in question:
                                                # check if this is a group
                                                if question_property.tag == 'is_group' and question_property.text == 'True':
                                                    new_question['group'] = True
                                                    new_question['members'] = []

                                                # check if this is part of a group
                                                if question_property.tag == 'group':
                                                    new_question['group_member'] = question_property.text

                                                # also check if this is part of a loop
                                                if question_property.tag == 'repeat' and question_property.text is not None:
                                                    new_question['loop_member'] = question_property.text

                                                # look for the question ID (in the `hashtagValue` field)
                                                if question_property.tag == 'hashtagValue':
                                                    # get rid of the `#form/` at the beginning
                                                    # and store it in the question id
                                                    new_question['id'] = question_property.text.replace('#form/', '')

                                                # look for the other type of question id (value)
                                                if question_property.tag == 'value':
                                                    new_question['value'] = question_property.text

                                                # store the quesiton type
                                                if question_property.tag == 'type':
                                                    new_question['type'] = question_property.text

                                                # store the question text
                                                if question_property.tag == 'translations' and len(question_property) > 0:
                                                    new_question['text'] = question_property[0].text

                                                # store the calculation field
                                                if question_property.tag == 'calculate':
                                                    new_question['calculation'] = question_property.text

                                            # find the parent object for this question
                                            group_name = None
                                            loop_name = None
                                            if new_question['group_member']:
                                                group_name = new_question['group_member']
                                            if new_question['loop_member']:
                                                loop_name = new_question['loop_member']

                                            # now figure out which parent to look for...
                                            parent_name = None
                                            if group_name:
                                                if loop_name:
                                                    if loop_name in group_name and len(group_name) > len(loop_name):
                                                        parent_name = group_name
                                                    else:
                                                        parent_name = loop_name
                                                else:
                                                    parent_name = group_name

                                            parent_group = self.find_parent(form_obj, parent_name)

                                            # skip if this is a trigger
                                            if new_question['type'] == 'Trigger':
                                                continue

                                            # if this is a member of a group (and not a loop), add it to the parent's group
                                            if new_question['group'] and new_question['type'] != 'Repeat':
                                                parent_group['groups'].append(new_question)

                                            # if this is a member of a loop, add it to the parent's loop
                                            if new_question['type'] == 'Repeat':
                                                parent_group['loops'].append(new_question)

                                            # if this is a normal question (not a group or loop),
                                            # then add it to the questions list
                                            if not new_question['group']:
                                                parent_group['questions'].append(new_question)

                                forms_as_json.append(form_obj)
        return forms_as_json
    
    def strip_sheet_name(self, sheet_name):
        # replace "and" with "&" (reduces 2 characters)
        sheet_name = sheet_name.replace('and', '&')

        # get rid of parenthesis (reduces at least 2 characters, if they exist)
        sheet_name = sheet_name.replace('(', '').replace(')', '')

        # if "enrollment" exists, replace it with a shorter "enroll" (reduces 4 characters, if they exist)
        sheet_name = sheet_name.replace('enrollment', 'enroll')

        # if "change" exists, replace it with a shorter "chng" (reduces 2 characters, if they exist)
        sheet_name = sheet_name.replace('change', 'chng')

        # if it's STILL longer than 31 characters, then start chopping away words (separated by "_") and leaving
        # only their first letter (thus, "compound_&_structure_..." could result in "c&s_...")
        last_name = None
        while len(sheet_name) > 31 and sheet_name != last_name:
            last_name = sheet_name
            words = sheet_name.split('_')
            for i in range(0, len(words)-1):
                if len(words[i]) > 1:
                    words[i] = words[i][0]
                    break
            sheet_name = '_'.join(words)

        return sheet_name

    def add_form_sheet(self, form, workbook):
        # create a new sheet
        current_sheet = workbook.add_worksheet(form['table_target'])

        current_sheet.write('A1', 'Question ID')
        current_sheet.write('B1', 'Question Text')
        current_sheet.write('C1', 'Question Type')
        current_sheet.write('D1', 'Question Calculations')
        current_sheet.write('E1', 'Database Table')
        current_sheet.write('F1', 'Database Column')

        current_row = 1
        for column in form['columns']:
            current_row += 1
            current_sheet.write('A'+str(current_row), column['question_id'])
            current_sheet.write('B'+str(current_row), column['question_text'])
            current_sheet.write('C'+str(current_row), column['question_type'])
            current_sheet.write('D'+str(current_row), column['calculation'])
            current_sheet.write('E'+str(current_row), form['table_target'])
            current_sheet.write('F'+str(current_row), column['database_column'])

        # now for each child, add another sheet!
        for child_form in form['children']:
            workbook = self.add_form_sheet(form=child_form, workbook=workbook)

        return workbook

    def generate_form_documentation(self, form):
        """
        Takes in a parent form and creates an excel file with sheets for each table that is generated from it.
        """
        # make sure our mapping sub directory exists
        output_dir = os.path.join(self.output_directory, 'mapping')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # create a new workbook
        form_name = form['form_name'].replace('/', '-')
        workbook = xlsxwriter.Workbook(os.path.join(
            output_dir, form_name+'_MAPPING_'+datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')+'.xlsx'
        ))

        workbook = self.add_form_sheet(form=form, workbook=workbook)

        workbook.close()

    def generate_form_spreadsheet(self, form_information, is_loop=False, parent_name=''):
        """
        Controller that splits out questions, groups, and loops of a form and calls the appropriate
        functions to generate those spreadsheets.
        """
        # first do the base spreadsheet (normal questions):
        spreadsheet_relationships = self.create_form_spreadsheet(
            form_information=form_information, is_loop=is_loop, parent_name=parent_name
        )
        xmlns = form_information['xmlns']

        # for each member of the form's loop...
        for loop_member in form_information['loops']:
            loop_member['xmlns'] = xmlns
            loop_member['name'] = loop_member['id']
            sheet_child = self.generate_form_spreadsheet(
                form_information=loop_member, is_loop=True, parent_name=spreadsheet_relationships['table_target']
            )
            spreadsheet_relationships['children'].append(sheet_child)

        return spreadsheet_relationships

    def create_form_spreadsheet(self, form_information, is_loop=False, parent_name=''):
        """
        Using the form information provided, generate an XLSX file to be used in the commcare export tool.
        :param form_information:
        :return:
        """
        spreadsheet_relationships = {
            'form_name': form_information['name'],
            'spreadsheet_file': None,
            'table_target': None,
            'columns': [],
            'children': []
        }
        form_name = form_information['name'].replace(' ', '_').replace('/', '_').lower()
        spreadsheet_relationships['spreadsheet_file'] = form_name+'.xlsx'

        output_dir = os.path.join(self.output_directory, 'queries')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        workbook = xlsxwriter.Workbook(os.path.join(output_dir, form_name+'.xlsx'))

        # Name the sheet the same as the form name.  This sets the database table name.
        if parent_name != '':
            parent_name = parent_name+'_'
        sheet_name = parent_name + form_name

        # standard replacements
        sheet_name = sheet_name.replace("(", '').replace(')', '').replace('-', '')
        if len(sheet_name) > 31:
            sheet_name = self.strip_sheet_name(sheet_name)

            if len(sheet_name) > 31:
                # try stripping JUST the form name
                sheet_name = self.strip_sheet_name(form_name)

                # if it's STILL greater than 31...
                if len(sheet_name) > 31:
                    print("The form name can't be stripped!", sheet_name, '['+parent_name + ' '+form_name+']')
                    exit()

        # while this sheet name has already been used, rename the last character with a counter (i hate this)
        current_edition = 0
        while sheet_name in self.database_table_names:
            current_edition += 1
            sheet_name = sheet_name[:-1] + str(current_edition)

        # now add it to the list of database table names
        self.database_table_names.append(sheet_name)

        spreadsheet_relationships['table_target'] = sheet_name
        worksheet = workbook.add_worksheet(name=sheet_name)

        # Write the headers
        worksheet.write('A1', 'Data Source')
        worksheet.write('B1', 'Filter Name')
        worksheet.write('C1', 'Filter Value')
        worksheet.write('D1', 'Field')
        worksheet.write('E1', 'Source Field')

        if is_loop:
            data_source = form_information['id']
            data_source = data_source.replace('/', '.')

            source_structure = data_source.split('.')
            updated_sources = []
            for source in source_structure:
                new_source = source
                if 'loop' in new_source:
                    new_source += '[*]'
                updated_sources.append(new_source)

            data_source = '.'.join(updated_sources)
            worksheet.write('A2', 'form.form.'+data_source+'[*]')
        else:
            worksheet.write('A2', 'form')
        worksheet.write('B2', 'xmlns.exact')
        worksheet.write('C2', form_information['xmlns'])

        # For the fields, always do the id and received on fields (by default)
        worksheet.write('D2', 'id')
        spreadsheet_relationships['columns'].append({
            'question_id': 'id',
            'question_text': '',
            'question_type': 'Hidden',
            'database_column': 'id',
            'calculation': 'auto-generated'
            })

        # if is_loop and not form_name.endswith('_form'):
        #     worksheet.write('E2', 'case.@case_id')
        if not is_loop and not form_name.endswith('_form'):
            worksheet.write('E2', '$.id')
        else:
            worksheet.write('E2', 'id')

        worksheet.write('D3', 'received_on')
        spreadsheet_relationships['columns'].append({
            'question_id': 'recieved_on',
            'question_text': '',
            'question_type': 'Hidden',
            'database_column': 'received_on',
            'calculation': 'auto-generated'
            })

        if is_loop:
            worksheet.write('E3', '$.received_on')
        else:
            worksheet.write('E3', 'received_on')
        worksheet.write('D4', 'case_id')
        spreadsheet_relationships['columns'].append({
            'question_id': 'case_id',
            'question_text': '',
            'question_type': 'Hidden',
            'database_column': 'case_id',
            'calculation': 'auto-generated'
            })

        if is_loop:
            worksheet.write('E4', '$.form.case.@case_id')
        else:
            worksheet.write('E4', 'form.case.@case_id')

        worksheet.write('D5', 'meta_username')
        spreadsheet_relationships['columns'].append({
            'question_id': 'username',
            'question_text': '',
            'question_type': 'Hidden',
            'database_column': 'meta_username',
            'calculation': 'auto-generated'
            })
        worksheet.write('E5', '$.metadata.username')

        worksheet.write('D6', 'meta_app_id')
        spreadsheet_relationships['columns'].append({
            'question_id': 'app_id',
            'question_text': '',
            'question_type': 'Hidden',
            'database_column': 'meta_app_id',
            'calculation': 'auto-generated'
            })
        worksheet.write('E6', 'app_id')

        worksheet.write('D7', 'meta_device_id')
        spreadsheet_relationships['columns'].append({
            'question_id': 'device_id',
            'question_text': '',
            'question_type': 'Hidden',
            'database_column': 'meta_device_id',
            'calculation': 'auto-generated'
            })
        worksheet.write('E7', 'form.meta.deviceID')

        # get meta GPS
        worksheet.write('D8', 'meta_gps')
        spreadsheet_relationships['columns'].append({
            'question_id': 'gps_location',
            'question_text': '',
            'question_type': 'Hidden',
            'database_column': 'meta_gps',
            'calculation': 'auto-generated'
            })
        worksheet.write('E8', 'form.meta.location')

        if not is_loop:
            worksheet.write('D9', 'meta_started_time')
            spreadsheet_relationships['columns'].append({
                'question_id': 'started_time',
                'question_text': '',
                'question_type': 'Hidden',
                'database_column': 'meta_started_time',
                'calculation': 'auto-generated'
                })
            worksheet.write('E9', 'form.meta.timeStart')

            worksheet.write('D10', 'meta_completed_time')
            spreadsheet_relationships['columns'].append({
                'question_id': 'completed_time',
                'question_text': '',
                'question_type': 'Hidden',
                'database_column': 'meta_completed_time',
                'calculation': 'auto-generated'
                })
            worksheet.write('E10', 'form.meta.timeEnd')

            current_row = 10
        else:
            worksheet.write('D9', 'parent_form_id')
            spreadsheet_relationships['columns'].append({
                'question_id': 'parent_form_id',
                'question_text': '',
                'question_type': 'Hidden',
                'database_column': 'parent_form_id',
                'calculation': 'auto-generated'
                })
            worksheet.write('E9', '$.id')
            current_row = 9

        for field in form_information['questions']:
            current_row += 1
            db_column_name = field['id'].lower().replace('/', '.').replace('-', '_')
            if '.' in db_column_name:
                db_column_name = db_column_name.split('.')[-1]

            column_data = {
                'question_id': field['id'],
                'question_text': field['text'],
                'question_type': field['type'],
                'calculation': field['calculation'],
                'database_column': db_column_name
            }

            spreadsheet_relationships['columns'].append(column_data)
            worksheet.write('D'+str(current_row), db_column_name)
            if is_loop:
                worksheet.write('E'+str(current_row), field['id'].lower().split('/')[-1])
            else:
                worksheet.write('E'+str(current_row), 'form.'+field['id'].lower().replace('/', '.'))

        # also for the groups:
        for group in form_information['groups']:
            group['xmlns'] = form_information['xmlns']
            group_info = self.get_group_questions(group_object=group, is_loop=is_loop)
            questions = group_info['questions']
            spreadsheet_relationships['children'].extend(group_info['children'])

            for question in questions:
                current_row += 1
                spreadsheet_relationships['columns'].append(question['db_column'])
                worksheet.write('D'+str(current_row), question['db_column']['database_column'])
                # if 'outcome' in question['db_column']['question_id']:
                #     print(group['value'])
                #     print(question)
                #     print('\n\n------\n\n')
                # group_name = group['value'].split('/')[-1]
                if is_loop:
                    # question_query = group_name + '.' + question['form_column'].replace('/', '.')
                    question_query = question['form_column'].replace('/', '.')
                else:
                    question_query = question['form_column'].replace('/', '.')
                worksheet.write('E'+str(current_row), question_query)

        workbook.close()

        return spreadsheet_relationships

    def get_group_questions(self, group_object, is_loop=False):
        question_list = []
        children = []

        # first loop through all the questions
        for question in group_object['questions']:
            new_question = {}
            db_column_name = question['id'].lower().replace('/', '.').replace('-', '_')
            if '.' in db_column_name:
                db_column_name = db_column_name.split('.')[-1]
            new_question['db_column'] = {
                'question_id': question['id'],
                'question_text': question['text'],
                'question_type': question['type'],
                'database_column': db_column_name,
                'calculation': question['calculation']
                }

            if is_loop:
                # save only the last two names surrounding the last '/'
                qid = '/'.join(question['id'].split('/')[-2:])
                new_question['form_column'] = qid.lower().replace('/', '.')
            else:
                new_question['form_column'] = 'form.'+question['id'].lower().replace('/', '.')
            question_list.append(new_question)

        # then do the same thing for any groups of this group
        for group in group_object['groups']:
            group['xmlns'] = group_object['xmlns']
            group_info = self.get_group_questions(group_object=group, is_loop=is_loop)
            question_list.extend(group_info['questions'])
            children.extend(group_info['children'])

        # for any loops within the group...
        for loop in group_object['loops']:
            loop['xmlns'] = group_object['xmlns']
            loop['name'] = loop['id']
            children.append(self.generate_spreadsheets(form_information=loop, is_loop=True))

        return {
            'questions': question_list,
            'children': children
        }

    def setup_form_controls(self, form, parent=None):
        """
        For the form and it's children, setup a form control object in the Django model.
        """
        # get or create the form based on the form's name
        form_control, created = FormControl.objects.get_or_create(form_name=form['form_name'])

        # update the sheet name
        form_control.sheet_name = form['spreadsheet_file']

        # update the parent, if it exists
        if parent:
            form_control.form_parent = parent

        form_control.save()

        # create a Django model for this form
        self.generate_form_model(form_obj=form_control)

        # loop through it's children
        for child in form['children']:
            self.setup_form_controls(form=child, parent=form_control)

    def generate_case_spreadsheet(self, case_information):
        """
        Generates a case query excel sheet.
        """
        case_name = 'case_'+case_information['name'].lower()
        output_dir = os.path.join(self.output_directory, 'queries')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        workbook = xlsxwriter.Workbook(os.path.join(output_dir, case_name+'.xlsx'))

        sheet_name = case_name

        # standard replacements
        sheet_name = sheet_name.replace("(", '').replace(')', '').replace('-', '')
        if len(sheet_name) > 31:
            sheet_name = self.strip_sheet_name(sheet_name)

            if len(sheet_name) > 31:
                # try stripping JUST the case name
                sheet_name = self.strip_sheet_name(case_name.replace('_', ''))

                # if it's STILL greater than 31...
                if len(sheet_name) > 31:
                    raise ValueError('A case name must be fewer than 26 characters in length!  I attempted to reduce the case name `'+case_name+'` down enough but was unsuccessful.  Please rename your case to something shorter.')

        # now add it to the list of database table names
        self.database_table_names.append(sheet_name)

        worksheet = workbook.add_worksheet(name=sheet_name)

        # Write the headers
        worksheet.write('A1', 'Data Source')
        worksheet.write('B1', 'Filter Name')
        worksheet.write('C1', 'Filter Value')
        worksheet.write('D1', 'Field')
        worksheet.write('E1', 'Source Field')

        # write the default values for case queries
        worksheet.write('A2', 'case')
        worksheet.write('B2', 'type')
        # NOTE: this needs to remained unchanged (no lower case)
        worksheet.write('C2', case_information['name'])

        # add a default "id" and "closed" property
        worksheet.write('D2', 'id')
        worksheet.write('E2', 'id')

        worksheet.write('D3', 'closed')
        worksheet.write('E3', 'closed')

        worksheet.write('D4', 'opened_date')
        worksheet.write('E4', 'properties.date_opened')

        worksheet.write('D5', 'owner_id')
        worksheet.write('E5', 'properties.owner_id')

        current_row = 5

        # now run through all of its properties
        for case_property in case_information['properties']:
            current_row += 1
            worksheet.write('D'+str(current_row), case_property)
            if case_property == 'name':
                worksheet.write('E'+str(current_row), case_property)
            else:
                worksheet.write('E'+str(current_row), 'properties.'+case_property)

        workbook.close()

        # add a case control for this
        case_control, created = CaseControl.objects.get_or_create(
            case_name=case_information['name'],
            sheet_name=case_name+'.xlsx'
        )
        case_control.save()

        # create a model for this case
        self.generate_case_model(case_obj=case_control)

    def find_duplicate_columns(self, form):
        existing_columns = {}
        duplicate_columns = {}

        for column in form['columns']:
            if column['database_column'] in existing_columns:
                if column['database_column'] not in duplicate_columns:
                    duplicate_columns[column['database_column']] = [
                        existing_columns[column['database_column']]
                    ]
                duplicate_columns[column['database_column']].append({
                    'id': column['database_column'],
                    'text': column['question_text']
                })
            else:
                existing_columns[column['database_column']] = {
                    'id': column['database_column'],
                    'text': column['question_text']
                }

        if len(form['children']) > 0:
            for child in form['children']:
                duplicate_columns.update(self.find_duplicate_columns(child))

        return duplicate_columns

    def generate_form_model(self, form_obj):
        """
        Given a form control object, generate a Django model for it.
        """
        # make sure our form models directory exists
        output_dir = os.path.join(self.output_directory, 'models')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # get the query sheet and load its information
        form_file = os.path.join(self.output_directory, 'queries', form_obj.sheet_name)
        table_name = get_table_name_from_excel_file(form_file)
        form_info = get_form_table(
            directory=self.output_directory,
            sheet_name=form_file.replace(self.output_directory, '').replace('queries/', '').replace('/', '')
        )

        form_info['table'] = table_name
        # make sure we have all columns
        database_connections = settings.DATABASES[self.commcare_db]
        confirm_table_schema(form_data=form_info, database_connections=database_connections)
        confirm_table_columns(form_data=form_info, database_connections=database_connections)

        # try to open the output file
        output_file_name = (
            form_obj.form_name.lower()+'.py'
        ).replace('/', '_').replace(' ', '_').replace('-', '_').replace('&', 'and').replace('(', '').replace(')', '')
        output_file = os.path.join(output_dir, output_file_name)
        stringio_obj = StringIO()
        call_command("inspectdb", table_name, database=self.commcare_db, stdout=stringio_obj)
        stringio_obj.seek(0)
        output_as_string = stringio_obj.read()

        output_as_string = output_as_string.replace('from django.contrib.gis.db import models', """from django.db import models
from djang_commcare.models import CommCareBaseAbstractModel""")
        output_as_string = output_as_string.replace('(models.Model)', '(CommCareBaseAbstractModel)')

        with open(output_file, 'w') as f:
            f.write(output_as_string)
            f.close()

        self.init_to_write.append(
            "from ."+output_file_name.replace('.py', '')+" import *\n"
        )
    
    def generate_case_model(self, case_obj):
        """
        Given a form control object, generate a Django model for it.
        """
        # make sure our form models directory exists
        output_dir = os.path.join(self.output_directory, 'models')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # get the query sheet and load its information
        form_file = os.path.join(self.output_directory, 'queries', case_obj.sheet_name)
        table_name = get_table_name_from_excel_file(form_file)

        # open the ALL_CASES.json to load case information
        all_cases_file = open(os.path.join(self.output_directory, 'ALL_CASES.json'), 'r')
        all_cases = json.loads(all_cases_file.read())
        all_cases_file.close()
        case_info = None
        for case_data in all_cases:
            if case_data['name'] == case_obj.case_name:
                case_info = case_data
                break

        case_info['table'] = table_name
        # make sure we have all columns
        database_connections = settings.DATABASES[self.commcare_db]
        confirm_case_table_schema(case_data=case_info, database_connections=database_connections)
        confirm_case_table_columns(case_info=case_info, database_connections=database_connections)

        # try to open the output file
        output_file_name = (
            'case_'+case_obj.case_name.lower()+'.py'
        ).replace('/', '_').replace(' ', '_').replace('-', '_').replace('&', 'and').replace('(', '').replace(')', '')
        output_file = os.path.join(output_dir, output_file_name)
        stringio_obj = StringIO()
        call_command("inspectdb", table_name, database=self.commcare_db, stdout=stringio_obj)
        stringio_obj.seek(0)
        output_as_string = stringio_obj.read()

        output_as_string = output_as_string.replace('from django.contrib.gis.db import models', """from django.db import models
from djang_commcare.models import CommCareBaseAbstractModel""")
        output_as_string = output_as_string.replace('(models.Model)', '(CommCareBaseAbstractModel)')

        with open(output_file, 'w') as f:
            f.write(output_as_string)
            f.close()

        self.init_to_write.append(
            "from ."+output_file_name.replace('.py', '')+" import *\n"
        )

    def handle(self, *args, **options):
        handle_start = datetime.datetime.now()
        # set the default output path
        if hasattr(settings, 'COMMCARE_QUERY_DIR'):
            if settings.COMMCARE_QUERY_DIR[0] == '/':
                directory = settings.COMMCARE_QUERY_DIR
            else:
                directory = os.path.join(os.getcwd(), settings.COMMCARE_QUERY_DIR)
        else:
            # try the environment variable
            if ('COMMCARE_QUERY_DIR' not in os.environ or
                settings.ENV('COMMCARE_QUERY_DIR') == ''
            ):
                directory = os.getcwd()
            else:
                directory = os.path.join(os.getcwd(), settings.ENV('COMMCARE_QUERY_DIR'))

        if not os.path.exists(directory):
            raise IOError("The directory `"+directory+"` does not exist.")
        else:
            self.output_directory = directory
        
        # try to get the login credentials for CommCare HQ
        credentials = get_commcare_credentials()

        # get the JSON structure of all of the cases
        print("Scraping CommCare HQ for fresh case schema information...", end="")
        case_pull_start = datetime.datetime.now()
        self.cases_as_json = self.get_all_cases(credentials=credentials)
        case_pull_end = datetime.datetime.now()
        print("done [ in", (case_pull_end - case_pull_start), "]")

        # get the JSON structure of all of the forms
        print("Scraping CommCare HQ for fresh form schema information...", end="")
        form_pull_start = datetime.datetime.now()
        self.forms_as_json = self.get_all_forms(credentials=credentials)
        form_pull_end = datetime.datetime.now()
        print("done [ in", (form_pull_end - form_pull_start), "]")

        # dump the forms to the ALL_FORMS.json file
        json_out = open(os.path.join(self.output_directory, "ALL_FORMS.json"), 'w')
        json_out.write(json.dumps(self.forms_as_json))
        json_out.close()

        # dump the cases to the ALL_CASES.json file
        json_out = open(os.path.join(self.output_directory, "ALL_CASES.json"), 'w')
        json_out.write(json.dumps(self.cases_as_json))
        json_out.close()

        # for each case, generate a spreadsheet out of it
        print("Generating case query files (spreadsheets) and models...")
        for case_obj in self.cases_as_json:
            self.generate_case_spreadsheet(case_obj)

        # for each form, generate a spreadsheet out of it
        print("Generating form query files (spreadsheets) and models...")
        for form in self.forms_as_json:
            sheet_relationships = self.generate_form_spreadsheet(form_information=form)
            self.form_relations.append(sheet_relationships)

            # check for duplicate columns first!
            duplicate_columns = self.find_duplicate_columns(sheet_relationships)
            if len(duplicate_columns.items()) > 0:
                print(
                    "Error found:  Form `",
                    sheet_relationships['form_name'],
                    "` has the following duplicate columns:\n",
                    json.dumps(duplicate_columns, indent=4, sort_keys=True)
                )
            else:
                # add form documentation
                self.generate_form_documentation(form=sheet_relationships)

        # setup or update form controls in the Django models
        for form in self.form_relations:
            self.setup_form_controls(form=form)

        # dump the sheet relationships to the JSON file
        sheets_out = open(os.path.join(self.output_directory, "SHEET_RELS.json"), 'w')
        sheets_out.write(json.dumps(self.form_relations))
        sheets_out.close()

        # dump a list of all the tables we generate
        tables_out = open(os.path.join(self.output_directory, "DB_TABLES.json"), 'w')
        tables_out.write(json.dumps(self.database_table_names))
        tables_out.close()

        # lastly, dump all of our imports to the init file
        with open(os.path.join(self.output_directory, 'models', '__init__.py'), 'w') as f:
            f.writelines(self.init_to_write)
            f.close()

        handle_end = datetime.datetime.now()
        print("== Completed in", (handle_end - handle_start), "==")
