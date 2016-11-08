import os
import logging
import datetime
import pandas
import luigi

from luigi.date_interval import Date

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available
from edx.analytics.tasks.url import url_path_join

log = logging.getLogger(__name__)


class DatabaseImportAcceptanceTest(AcceptanceTestCase):

    DATE = '2014-07-01'

    def setUp(self):
        super(DatabaseImportAcceptanceTest, self).setUp()
        #self.execute_sql_fixture_file('load_auth_user_for_internal_reporting_user.sql')
        #self.execute_sql_fixture_file('load_auth_userprofile.sql')
        self.execute_sql_fixture_file('load_certificates_generatedcertificate.sql')
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
            expected_output_csv = os.path.join(self.data_dir, 'output', 'database_import', 'expected_certificates_generatedcertificate.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=[11,12])
            expected.fillna('', inplace=True)

            cursor.execute("SELECT * FROM {schema}.certificates_generatedcertificate".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            certificates_generatedcertificate = pandas.DataFrame(response, columns=list(expected.columns))
            certificates_generatedcertificate=certificates_generatedcertificate.convert_objects(convert_numeric=True)

            self.assert_data_frames_equal(certificates_generatedcertificate, expected)
