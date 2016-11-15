"""
Ensure we can write from MySQL to Hive data sources.
"""

import datetime
import textwrap

from mock import patch, Mock, MagicMock

from edx.analytics.tasks.database_imports import (
    ImportStudentCourseEnrollmentTask, ImportIntoHiveTableTask, LoadMysqlToVerticaTableTask
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.config import with_luigi_config
from edx.analytics.tasks.tests.target import FakeTarget


class LoadMysqlToVerticaTableTaskTest(unittest.TestCase):

    def test_table_schema(self):
        test_input = """
                    id,int(11),NO
                    slug,varchar(255),YES
                    site_id,int(11),NO
                    parent_id,int(11),YES
                    lft,int(10) unsigned,NO
                    rght,int(10) unsigned,NO
                    tree_id,int(10) unsigned,NO
                    level,int(10) unsigned,NO
                    article_id,int(11),NO
                    feedback,longtext,YES
                    value,double,NO
                    """

        def reformat(string):
            """Reformat string to make it like a TSV."""
            return textwrap.dedent(string).strip().replace(',', '\t')

        task = LoadMysqlToVerticaTableTask(table_name='test_table')

        fake_input = {
            'mysql_schema_task': FakeTarget(value=reformat(test_input))
        }
        task.input = MagicMock(return_value=fake_input)

        expected_schema = [
            ('id', 'int NOT NULL'),
            ('slug', 'varchar(255)'),
            ('site_id', 'int NOT NULL'),
            ('parent_id', 'int'),
            ('lft', 'int NOT NULL'),
            ('rght', 'int NOT NULL'),
            ('tree_id', 'int NOT NULL'),
            ('level', 'int NOT NULL'),
            ('article_id', 'int NOT NULL'),
            ('feedback', 'LONG VARCHAR'),
            ('value', 'DOUBLE PRECISION NOT NULL'),
        ]
        self.assertEqual(task.vertica_compliant_schema(), expected_schema)


class ImportStudentCourseEnrollmentTestCase(unittest.TestCase):
    """Tests to validate ImportStudentCourseEnrollmentTask."""

    def test_base_class(self):
        task = ImportIntoHiveTableTask(**{})
        with self.assertRaises(NotImplementedError):
            task.table_name()

    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        kwargs = {'import_date': datetime.datetime.strptime('2014-07-01', '%Y-%m-%d').date()}
        task = ImportStudentCourseEnrollmentTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS student_courseenrollment;
            CREATE EXTERNAL TABLE student_courseenrollment (
                id INT,user_id INT,course_id STRING,created TIMESTAMP,is_active BOOLEAN,mode STRING
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/student_courseenrollment';
            ALTER TABLE student_courseenrollment ADD PARTITION (dt = '2014-07-01');
            """
        )
        self.assertEquals(query, expected_query)

    def test_overwrite(self):
        kwargs = {'overwrite': True}
        task = ImportStudentCourseEnrollmentTask(**kwargs)
        self.assertFalse(task.complete())

    def test_no_overwrite(self):
        # kwargs = {'overwrite': False}
        kwargs = {}
        task = ImportStudentCourseEnrollmentTask(**kwargs)
        with patch('edx.analytics.tasks.database_imports.HivePartitionTarget') as mock_target:
            output = mock_target()
            # Make MagicMock act more like a regular mock, so that flatten() does the right thing.
            del output.__iter__
            del output.__getitem__
            output.exists = Mock(return_value=False)
            self.assertFalse(task.complete())
            self.assertTrue(output.exists.called)
            output.exists = Mock(return_value=True)
            self.assertTrue(task.complete())
            self.assertTrue(output.exists.called)
