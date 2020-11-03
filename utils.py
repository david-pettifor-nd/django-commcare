import os
import pandas
import json
import psycopg2

from django.conf import settings

def create_case_table(case_data, cursor):
    """
    Creates a database table using the form data passed in.
    """
    create_sql = """
    CREATE TABLE "%(table)s" (
        id CHARACTER VARYING(255) PRIMARY KEY,
        imported_on TIMESTAMP,
    """ % {'table': case_data['table']}

    for column in case_data['properties'][1:-1]:
        create_sql += """"%(column)s" TEXT,
        """ % {'column': column}

    create_sql += """"%(column)s" TEXT
    );
    """ % {'column': case_data['properties'][-1]}

    cursor.execute(create_sql)
    return True

def confirm_case_table_schema(case_data, database_connections):
    """
    Checks to make sure the table exists.  If it doesn't, it creates it.
    """
    connection = psycopg2.connect(
        host=database_connections['HOST'],
        database=database_connections['NAME'],
        user=database_connections['USER'],
        port=database_connections['PORT'],
        password=database_connections['PASSWORD']
    )

    cursor = connection.cursor()

    # verify the table exists
    cursor.execute("""
    SELECT EXISTS (
    SELECT 1
    FROM   pg_tables
    WHERE  schemaname = 'public'
    AND    tablename = '%(table)s'
    );
    """ % {'table': case_data['table']})
    exists = cursor.fetchone()[0]

    if not exists:
        # then create the table
        if create_case_table(case_data, cursor):
            connection.commit()
            return True

def confirm_case_table_columns(case_info, database_connections):
    """
    Runs through the case properties and ensures each column exists.
    """
    connection = psycopg2.connect(
        host=database_connections['HOST'],
        database=database_connections['NAME'],
        user=database_connections['USER'],
        port=database_connections['PORT'],
        password=database_connections['PASSWORD']
    )

    cursor = connection.cursor()

    # get a list of columns from the database
    cursor.execute("""
    SELECT attname FROM pg_attribute WHERE attrelid =
    (SELECT oid FROM pg_class
    WHERE relname = '%(table)s')
    AND attname != 'imported_on' AND attname != 'id' AND attname != 'closed'
    AND attisdropped = FALSE
    AND attnum > 0;""" % {
        'table': case_info['table']
    })

    existing_columns = cursor.fetchall()
    existing_column_list = []
    for column in existing_columns:
        existing_column_list.append(column[0])

    case_info['properties'].append('imported_on')
    case_info['properties'].append('opened_date')
    case_info['properties'].append('closed')
    case_info['properties'].append('owner_id')

    for case_property in case_info['properties']:
        # if this column is found in the existing columns, we can remove that from the list
        if case_property in existing_column_list:
            existing_column_list.remove(case_property)

        cursor.execute("""
        SELECT attname FROM pg_attribute WHERE attrelid =
        (SELECT oid FROM pg_class
        WHERE relname = '%(table)s') AND attname = '%(column)s';""" % {
            'table': case_info['table'],
            'column': case_property
        })

        column_exists = cursor.fetchone()

        if not column_exists:
            if case_property == 'closed':
                cursor.execute("""ALTER TABLE "%(table)s"
                    ADD COLUMN "%(column)s" boolean DEFAULT FALSE;""" % {
                    'table': case_info['table'],
                    'column': case_property
                })
            else:
                cursor.execute("""ALTER TABLE "%(table)s"
                    ADD COLUMN "%(column)s" TEXT;""" % {
                    'table': case_info['table'],
                    'column': case_property
                })
            connection.commit()

    # at this point, anything left in "existing_column_list" should only be legacy columns that we don't need anymore
    for c in existing_column_list:
        cursor.execute("""ALTER TABLE "%(table)s"
        DROP COLUMN "%(column)s";""" % {
            'column': c,
            'table': case_info['table']
        })
    connection.commit()

def get_table_name_from_excel_file(excel_file):
    """
    Opens the excel file passed in and returns back the name of the first sheet it finds.
    This should be the target table name.
    """
    xl = pandas.ExcelFile(excel_file)
    return xl.sheet_names[0]

def get_form_info(sheet_name, form_data):
    """
    If the current sheet we're working on matches the form data,
    return the form data.  Otherwise, recursively call this function
    for each child of the form.

    If it doesn't find any, return None.
    """
    if form_data['spreadsheet_file'] == sheet_name:
        return form_data

    for child in form_data['children']:
        sheet = get_form_info(sheet_name, child)
        if sheet:
            return sheet

    return None

def get_form_table(directory, sheet_name):
    """
    Load the sheet relationships JSON file and start looking for the form
    that matches the current sheet we're about to ingest.
    """
    # load the JSON file
    form_information_file = os.path.join(directory, 'SHEET_RELS.json')

    if not os.path.exists(form_information_file):
        raise OSError("Could not locate sheet relationship file: `"+form_information_file+"`")

    rels_in = open(form_information_file, 'r')
    rels_json = json.loads(rels_in.read())
    rels_in.close()

    # loop through the relationships looking
    for form in rels_json:
        sheet = get_form_info(sheet_name, form)
        if sheet:
            return sheet

    return None

def create_table(form_data, cursor):
    """
    Creates a database table using the form data passed in.
    """
    create_sql = """
    CREATE TABLE "%(table)s" (
        id CHARACTER VARYING(%(length)s) PRIMARY KEY,
        imported_on TIMESTAMP,
    """ % {'table': form_data['table_target'], 'length': settings.MAX_CHARVAR_LENGTH}

    for column in form_data['columns'][1:-1]:
        create_sql += """"%(column)s" TEXT,
        """ % {'column': column['database_column']}

    create_sql += """"%(column)s" TEXT
    );
    """ % {'column': form_data['columns'][-1]['database_column']}

    cursor.execute(create_sql)
    return True

def confirm_table_schema(form_data, database_connections):
    """
    Checks to make sure the table exists.  If it doesn't, it creates it.
    """
    connection = psycopg2.connect(
        host=database_connections['HOST'],
        database=database_connections['NAME'],
        user=database_connections['USER'],
        port=database_connections['PORT'],
        password=database_connections['PASSWORD']
    )

    cursor = connection.cursor()

    # verify the table exists
    cursor.execute("""
    SELECT EXISTS (
    SELECT 1
    FROM   pg_tables
    WHERE  schemaname = 'public'
    AND    tablename = '%(table)s'
    );
    """ % {'table': form_data['table_target']})
    exists = cursor.fetchone()[0]

    if not exists:
        # then create the table
        if create_table(form_data, cursor):
            connection.commit()
            return True

def confirm_table_columns(form_data, database_connections):
    """
    Looks at the expected columns in the form data and checks to see if the database table
    has all of these columns.
    """
    connection = psycopg2.connect(
        host=database_connections['HOST'],
        database=database_connections['NAME'],
        user=database_connections['USER'],
        port=database_connections['PORT'],
        password=database_connections['PASSWORD']
    )

    cursor = connection.cursor()

    # get a list of columns from the database
    cursor.execute("""
    SELECT attname FROM pg_attribute WHERE attrelid =
    (SELECT oid FROM pg_class
    WHERE relname = '%(table)s')
    AND attname != 'imported_on'
    AND attisdropped = FALSE
    AND attnum > 0;""" % {
        'table': form_data['table_target']
    })

    existing_columns = cursor.fetchall()
    existing_column_list = []
    for column in existing_columns:
        existing_column_list.append(column[0])

    # loop through each column and verify we have data for it
    for column in form_data['columns']:
        # if this column is found in the existing columns, we can remove that from the list
        if column['database_column'] in existing_column_list:
            existing_column_list.remove(column['database_column'])

        # first see if the column exists
        cursor.execute("""
        SELECT attname FROM pg_attribute WHERE attrelid =
        (SELECT oid FROM pg_class
        WHERE relname = '%(table)s') AND attname = '%(column)s';""" % {
            'table': form_data['table_target'],
            'column': column['database_column']
        })

        column_exists = cursor.fetchone()

        if column_exists:
            # if this is the ID column...
            if column['database_column'] == 'id':
                # make sure we have a large enough character varying max length
                cursor.execute("""
                SELECT character_maximum_length FROM information_schema.columns
                WHERE table_name = '%(table)s' and column_name = 'id';""" % {
                    'table': form_data['table_target']
                })
                max_length = cursor.fetchone()[0]

                if max_length < settings.MAX_CHARVAR_LENGTH:
                    # then alter this type
                    cursor.execute("""
                    ALTER TABLE "%(table)s"
                    ALTER COLUMN id TYPE VARCHAR(%(length)s);
                    """ % {
                        'table': form_data['table_target'],
                        'length': settings.MAX_CHARVAR_LENGTH
                    })

            cursor.execute("""
            SELECT COUNT(*) FROM "%(table)s" WHERE "%(column)s" IS NOT NULL AND "%(column)s" != '';
            """ % {'table': form_data['table_target'], 'column': column['database_column']})

            count = cursor.fetchone()

        else:
            # create the column!
            cursor.execute("""ALTER TABLE "%(table)s"
            ADD COLUMN "%(column)s" TEXT;""" % {
                'table': form_data['table_target'],
                'column': column['database_column']
            })
            connection.commit()

    # at this point, anything left in "existing_column_list" should only be legacy columns that we don't need anymore
    for c in existing_column_list:
        cursor.execute("""ALTER TABLE "%(table)s"
        DROP COLUMN "%(column)s";""" % {
            'column': c,
            'table': form_data['table_target']
        })
    connection.commit()
