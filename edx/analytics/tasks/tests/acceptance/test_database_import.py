import os
import logging
import pandas

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available

log = logging.getLogger(__name__)


class DatabaseImportAcceptanceTest(AcceptanceTestCase):

    DATE = '2014-07-01'

    def setUp(self):
        super(DatabaseImportAcceptanceTest, self).setUp()
        #self.execute_sql_fixture_file('load_auth_user_for_internal_reporting_user.sql')
        #self.execute_sql_fixture_file('load_auth_userprofile.sql')
        self.execute_sql_fixture_file('load_database_import_test_table.sql')
        # self.execute_sql_fixture_file('load_course_groups_courseusergroup.sql')
        # self.execute_sql_fixture_file('load_course_groups_courseusergroup_users.sql')
        # self.execute_sql_fixture_file('load_student_courseenrollment.sql')

    @when_vertica_available
    def test_database_import(self):
        self.task.launch([
            'ImportMysqlToVerticaTask',
            '--date', self.DATE,
            '--overwrite'
        ])

        self.validate_output()

    def validate_output(self):
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(
                self.data_dir,
                'output',
                'database_import',
                'expected_database_import_test_table.csv'
            )
            expected = pandas.read_csv(expected_output_csv, parse_dates=[6,7])
            expected.fillna('', inplace=True)

            cursor.execute(
                "SELECT * FROM {schema}.database_import_test_table".format(schema=self.vertica.schema_name)
            )
            response = cursor.fetchall()
            database_import_test_table = pandas.DataFrame(response, columns=list(expected.columns))
            database_import_test_table = database_import_test_table.convert_objects(convert_numeric=True)

            self.assert_data_frames_equal(database_import_test_table, expected)
