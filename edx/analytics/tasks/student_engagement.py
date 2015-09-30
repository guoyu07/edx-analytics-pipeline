"""Calculates per-student engagement reports per course."""

import csv
import datetime
import hashlib
import json
import logging
from itertools import groupby
from operator import itemgetter
import re
import sys

import luigi
try:
    from elasticsearch import Elasticsearch
    from elasticsearch.client import IndicesClient
    from elasticsearch.exceptions import NotFoundError
    from elasticsearch import helpers
except ImportError:
    pass

from edx.analytics.tasks.calendar_task import CalendarTableTask
from edx.analytics.tasks.database_imports import (
    ImportAuthUserTask, ImportAuthUserProfileTask, ImportCourseUserGroupTask, ImportCourseUserGroupUsersTask)
from edx.analytics.tasks.enrollments import CourseEnrollmentTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join, IgnoredTarget
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.mysql_load import MysqlInsertTask

from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveTableFromQueryTask

log = logging.getLogger(__name__)


SUBSECTION_VIEWED_MARKER = 'marker:last_subsection_viewed'


class StudentEngagementTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    Calculate student engagement for a given interval and interval type.

    Calculates separately for each user in each course.
    """

    SUBSECTION_ACCESSED_PATTERN = r'/courses/[^/+]+(/|\+)[^/+]+(/|\+)[^/]+/courseware/[^/]+/[^/]+/.*$'

    output_root = luigi.Parameter()
    interval_type = luigi.Parameter(default="daily")

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')

        entity_id = ''
        info = {}
        if event_type == 'problem_check':
            if event_source != 'server':
                return

            problem_id = event_data.get('problem_id')
            if not problem_id:
                return

            entity_id = problem_id
            if event_data.get('success', 'incorrect').lower() == 'correct':
                info['correct'] = True
        elif event_type == 'play_video':
            encoded_module_id = event_data.get('id')
            if not encoded_module_id:
                return

            entity_id = encoded_module_id
        elif event_type[:9] == '/courses/' and re.match(self.SUBSECTION_ACCESSED_PATTERN, event_type):
            timestamp = eventlog.get_event_time_string(event)
            if timestamp is None:
                return
            info['path'] = event_type
            info['timestamp'] = timestamp
            event_type = SUBSECTION_VIEWED_MARKER

        date_grouping_key = date_string

        if self.interval_type == 'weekly':
            last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
            last_weekday = last_complete_date.isoweekday()

            split_date = date_string.split('-')
            event_date = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
            event_weekday = event_date.isoweekday()

            days_until_end = last_weekday - event_weekday
            if days_until_end < 0:
                days_until_end += 7

            end_of_week_date = event_date + datetime.timedelta(days=days_until_end)
            date_grouping_key = end_of_week_date.isoformat()

        elif self.interval_type == 'all':
            # If gathering all data for a given user, use the last complete day of the interval
            # for joining with enrollment.
            last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
            date_grouping_key = last_complete_date.isoformat()

        yield ((date_grouping_key, course_id, username), (entity_id, event_type, json.dumps(info), date_string))

    def reducer(self, key, events):
        """Calculate counts for events corresponding to user and course in a given time period."""
        date_grouping_key, course_id, username = key

        sort_key = itemgetter(0)
        sorted_events = sorted(events, key=sort_key)
        if len(sorted_events) == 0:
            return

        num_problems_attempted = 0
        num_problem_attempts = 0
        num_problems_correct = 0
        num_videos_played = 0
        num_forum_comments = 0
        num_forum_responses = 0
        num_forum_posts = 0
        num_textbook_pages = 0
        dates_active = set()
        max_timestamp = None
        last_subsection_viewed = ''
        for _entity_id, events in groupby(sorted_events, key=sort_key):
            is_first = True
            is_correct = False

            for _, event_type, info_json, date_string in events:
                info = json.loads(info_json)
                if event_type == 'problem_check':
                    if is_first:
                        num_problems_attempted += 1
                    num_problem_attempts += 1
                    if not is_correct and info.get('correct', False):
                        is_correct = True
                elif event_type == 'play_video':
                    if is_first:
                        num_videos_played += 1
                elif event_type == 'edx.forum.comment.created':
                    num_forum_comments += 1
                elif event_type == 'edx.forum.response.created':
                    num_forum_responses += 1
                elif event_type == 'edx.forum.thread.created':
                    num_forum_posts += 1
                elif event_type == 'book':
                    num_textbook_pages += 1
                elif event_type == SUBSECTION_VIEWED_MARKER:
                    if not max_timestamp or info['timestamp'] > max_timestamp:
                        last_subsection_viewed = info['path']
                        max_timestamp = info['timestamp']

                if is_first:
                    is_first = False

                if date_string not in dates_active:
                    dates_active.add(date_string)

            if is_correct:
                num_problems_correct += 1

        yield (
            # Output to be read by Hive must be encoded as UTF-8.
            date_grouping_key,
            course_id.encode('utf-8'),
            username.encode('utf-8'),
            len(dates_active),
            num_problems_attempted,
            num_problem_attempts,
            num_problems_correct,
            num_videos_played,
            num_forum_posts,
            num_forum_responses,
            num_forum_comments,
            num_textbook_pages,
            last_subsection_viewed.encode('utf-8'),
        )

    def output(self):
        return get_target_from_url(self.output_root)


# After generating the output, need to join with auth_user to get the
# email (joining on username), and would also need to get the user_id
# to be able to join with enrollment data.

# In order to be able to perform this join, we first need to import this
# data into Hive.  Then define the other tables as also being in Hive.

class StudentEngagementTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the StudentEngagementTableTask task."""

    interval_type = luigi.Parameter(default="daily")


class StudentEngagementTableTask(StudentEngagementTableDownstreamMixin, HiveTableTask):
    """Hive table that stores the set of students engaged in each course over time."""

    @property
    def table(self):
        return 'student_engagement_raw_{}'.format(self.interval_type)

    @property
    def columns(self):
        return [
            ('end_date', 'STRING'),
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('days_active', 'INT'),
            ('problems_attempted', 'INT'),
            ('problem_attempts', 'INT'),
            ('problems_correct', 'INT'),
            ('videos_played', 'INT'),
            ('forum_posts', 'INT'),
            ('forum_responses', 'INT'),
            ('forum_comments', 'INT'),
            ('textbook_pages_viewed', 'INT'),
            ('last_subsection_viewed', 'STRING'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return StudentEngagementTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
            interval_type=self.interval_type,
        )


class JoinedStudentEngagementTableTask(StudentEngagementTableDownstreamMixin, HiveTableFromQueryTask):
    """
    Join additional information onto raw student engagement data, but leave information in Hive,
    not in Mysql.

    Just need cohort and email, and to add (zeroed) entries for enrolled users who were not among the active.

    Doesn't look like the base class is ever used, and not sure it's right.  So pulling in a copy
    to work on instead.
    """

    @property
    def table(self):
        return 'student_engagement_summary_{}'.format(self.interval_type)

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def columns(self):
        return [
            ('end_date', 'STRING'),
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('email', 'STRING'),
            ('name', 'STRING'),
            ('enrollment_mode', 'STRING'),
            ('cohort', 'STRING'),
            ('days_active', 'INT'),
            ('problems_attempted', 'INT'),
            ('problem_attempts', 'INT'),
            ('problems_correct', 'INT'),
            ('videos_played', 'INT'),
            ('forum_posts', 'INT'),
            ('forum_responses', 'INT'),
            ('forum_comments', 'INT'),
            ('textbook_pages_viewed', 'INT'),
            ('last_subsection_viewed', 'STRING'),
            ('segments', 'STRING'),
        ]

    def hiveconfs(self):
        jcs = super(JoinedStudentEngagementTableTask, self).hiveconfs()
        jcs['hive.auto.convert.join'] = 'false'
        return jcs

    @property
    def insert_query(self):
        # Join with calendar data only if calculating weekly engagement.
        calendar_join = ""
        if self.interval_type == "daily":
            date_where = "ce.date >= '{start}' AND ce.date < '{end}'".format(
                start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
                end=self.interval.date_b.isoformat()  # pylint: disable=no-member
            )
        elif self.interval_type == "weekly":
            last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
            iso_weekday = last_complete_date.isoweekday()
            calendar_join = "INNER JOIN calendar cal ON (ce.date = cal.date) "
            date_where = "ce.date >= '{start}' AND ce.date < '{end}' AND cal.iso_weekday = {iso_weekday}".format(
                start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
                end=self.interval.date_b.isoformat(),  # pylint: disable=no-member
                iso_weekday=iso_weekday,
            )
        elif self.interval_type == "all":
            last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
            date_where = "ce.date = '{last_complete_date}'".format(last_complete_date=last_complete_date.isoformat())

        return """
        SELECT
            ce.date,
            ce.course_id,
            au.username,
            au.email,
            aup.name,
            ce.mode,
            COALESCE(cohort.name, ''),
            COALESCE(ser.days_active, 0),
            COALESCE(ser.problems_attempted, 0),
            COALESCE(ser.problem_attempts, 0),
            COALESCE(ser.problems_correct, 0),
            COALESCE(ser.videos_played, 0),
            COALESCE(ser.forum_posts, 0),
            COALESCE(ser.forum_responses, 0),
            COALESCE(ser.forum_comments, 0),
            COALESCE(ser.textbook_pages_viewed, 0),
            COALESCE(ser.last_subsection_viewed, ''),
            concat_ws(",",
                CASE WHEN (ser.days_active = 0 OR ser.days_active IS NULL) THEN "inactive" END,
                CASE WHEN (
                    (ser.problem_attempts > 0 AND ser.problems_correct = 0)
                    OR (
                        ser.problems_correct > 0
                        AND ((ser.problem_attempts / ser.problems_correct) > perc.attempts_per_correct_80)
                    )
                ) THEN "struggling" END,
                "enrolled"
            )
        FROM course_enrollment ce
        {calendar_join}
        INNER JOIN auth_user au
            ON (ce.user_id = au.id)
        INNER JOIN auth_userprofile aup
            ON (au.id = aup.user_id)
        LEFT OUTER JOIN student_engagement_raw_{interval_type} ser
            ON (au.username = ser.username AND ce.date = ser.end_date and ce.course_id = ser.course_id)
        LEFT OUTER JOIN (
                SELECT
                    end_date,
                    course_id,
                    percentile_approx(
                        CASE WHEN problems_correct > 0 THEN (problem_attempts / problems_correct) ELSE 0.0 END,
                        0.8
                    ) AS attempts_per_correct_80
                FROM student_engagement_raw_{interval_type}
                GROUP BY end_date, course_id
            ) perc ON (ce.course_id = perc.course_id AND ce.date = perc.end_date)
        LEFT OUTER JOIN (
            SELECT
                cugu.user_id,
                cug.course_id,
                cug.name
            FROM course_groups_courseusergroup_users cugu
            INNER JOIN course_groups_courseusergroup cug
                ON (cugu.courseusergroup_id = cug.id)
        ) cohort
            ON (au.id = cohort.user_id AND ce.course_id = cohort.course_id)
        WHERE ce.at_end = 1 AND {date_where}
        """.format(
            calendar_join=calendar_join,
            interval_type=self.interval_type,
            date_where=date_where
        )

    def requires(self):
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
        }
        kwargs_for_engagement = {
            'mapreduce_engine': self.mapreduce_engine,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'overwrite': self.overwrite,
            'interval_type': self.interval_type,
        }
        # For enrollment, use the default start date and the current
        # interval's end date to calculate. Note that if it's already
        # calculated, this won't check the interval that was used.
        kwargs_for_enrollment = {
            'mapreduce_engine': self.mapreduce_engine,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval_end': self.interval.date_b,  # pylint: disable=no-member
            'pattern': self.pattern,
            'overwrite': self.overwrite,
        }
        yield (
            StudentEngagementTableTask(**kwargs_for_engagement),
            ImportAuthUserTask(**kwargs_for_db_import),
            ImportCourseUserGroupTask(**kwargs_for_db_import),
            ImportCourseUserGroupUsersTask(**kwargs_for_db_import),
            CourseEnrollmentTableTask(**kwargs_for_enrollment),
            ImportAuthUserProfileTask(**kwargs_for_db_import),
        )
        # Only the weekly requires use of the calendar.
        if self.interval_type == "weekly":
            yield (
                CalendarTableTask(
                    warehouse_path=self.warehouse_path,
                )
            )


class StudentEngagementIndexTask(
        StudentEngagementTableDownstreamMixin,
        OverwriteOutputMixin,
        MapReduceJobTask):

    elasticsearch_host = luigi.Parameter(
        is_list=True,
        config_path={'section': 'elasticsearch', 'name': 'host'}
    )
    elasticsearch_index = luigi.Parameter(
        config_path={'section': 'student-engagement', 'name': 'index'}
    )

    def requires(self):
        return JoinedStudentEngagementTableTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            overwrite=self.overwrite,
            interval_type=self.interval_type,
        )

    def init_local(self):
        es = Elasticsearch(hosts=self.elasticsearch_host)
        ix_client = IndicesClient(es)
        if not ix_client.exists(index=self.elasticsearch_index):
            ix_client.create(index=self.elasticsearch_index)

        doc_type = 'roster_entry'
        try:
            ix_client.get_mapping(index=self.elasticsearch_index, doc_type=doc_type)
        except NotFoundError:
            ix_client.put_mapping(index=self.elasticsearch_index, doc_type=doc_type, body={
                'roster_entry': {
                    'properties': {
                        'course_id': {'type': 'string', 'index': 'not_analyzed'},
                        'username': {'type': 'string', 'index': 'not_analyzed'},
                        'email': {'type': 'string', 'index': 'not_analyzed'},
                        'name': {'type': 'string'},
                        'enrollment_mode': {'type': 'string', 'index': 'not_analyzed'},
                        'cohort': {'type': 'string', 'index': 'not_analyzed'},
                        'segments': {'type': 'string', 'index_name': 'segments'}
                    }
                }
            })

    def mapper(self, line):
        hashed_line = hashlib.sha1(line).hexdigest()
        bucket = int(hashed_line[:3], 16) % self.n_reduce_tasks

        yield (bucket, line)

    def reducer(self, _key, records):
        es = Elasticsearch(hosts=self.elasticsearch_host)

        def record_generator():
            for record in records:
                split_record = record.split('\t')
                course_id = split_record[1]
                username = split_record[2]
                yield {
                    '_index': self.elasticsearch_index,
                    '_type': 'roster_entry',
                    '_id': '|'.join([course_id, username]),
                    '_source': {
                        'course_id': course_id,
                        'username': username,
                        'email': split_record[3],
                        'name': split_record[4],
                        'enrollment_mode': split_record[5],
                        'cohort': split_record[6],
                        'segments': split_record[-1].split(',')
                    }
                }

        results = helpers.bulk(es, record_generator())
        sys.stderr.write(str(results))
        sys.stderr.write('\n')

        yield ('', '')

    def extra_modules(self):
        import urllib3
        import elasticsearch
        return [urllib3, elasticsearch]

    def jobconfs(self):
        jcs = super(StudentEngagementIndexTask, self).jobconfs()
        jcs.append('mapred.reduce.tasks.speculative.execution=false')
        return jcs

    def output(self):
        return IgnoredTarget()


class StudentEngagementCsvFileTask(
        StudentEngagementTableDownstreamMixin,
        OverwriteOutputMixin,
        MultiOutputMapReduceJobTask):
    """
    Groups student engagement information by course, producing a different file for each.
    """

    def requires(self):
        return JoinedStudentEngagementTableTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            overwrite=self.overwrite,
            interval_type=self.interval_type,
        )

    def mapper(self, line):
        """
        Groups inputs by date and course_id, writes all records with the same course_id to the same output file.
        """
        # TSV's are assumed to be written (by Hive) in UTF-8 encoding,
        # so we do not have to encode the course_id before outputting.
        date, course_id, content = line.split('\t', 2)
        yield (date, course_id), content

    def output_path_for_key(self, key):
        """
        Match the course folder hierarchy that is expected by the instructor dashboard.

        The instructor dashboard expects the file to be stored in a
        folder named sha1(course_id).  All files in that directory
        will be displayed on the instructor dashboard for that course.
        """
        date, course_id = key
        if self.interval_type == "all":
            date = str(self.interval)
        hashed_course_id = hashlib.sha1(course_id).hexdigest()
        filename = u'student_engagement_{interval_type}_{date}.csv'.format(date=date, interval_type=self.interval_type)
        return url_path_join(self.output_root, hashed_course_id, filename)

    def _get_date_header(self):
        """Gets column header for date, conditional on interval type."""
        return 'Date' if self.interval_type == "daily" else 'End Date'

    def _get_active_header(self):
        """Gets column header for days active, conditional on interval type."""
        if self.interval_type == "daily":
            return 'Was Active'
        elif self.interval_type == "weekly":
            return "Days Active This Week"
        else:
            return 'Days Active'

    def get_column_names(self):
        """
        List names of columns as they should appear in the CSV.

        Apart from the first two entries, these must also be the order
        they are stored in the Hive TSV output.
        """
        return [
            'Course ID',
            self._get_date_header(),
            'Username',
            'Email',
            'Cohort',
            self._get_active_header(),
            'Unique Problems Attempted',
            'Total Problem Attempts',
            'Unique Problems Correct',
            'Unique Videos Played',
            'Discussion Posts',
            'Discussion Responses',
            'Discussion Comments',
            'Textbook Pages Viewed',
            'URL of Last Subsection Viewed',
        ]

    def multi_output_reducer(self, key, values, output_file):
        """
        Each entry should be written to the output file in csv format.

        This output is visible to instructors, so use an excel friendly format (csv).
        """
        end_date, course_id = key
        field_names = self.get_column_names()

        writer = csv.DictWriter(output_file, field_names)
        writer.writerow(dict(
            (k, k) for k in field_names
        ))

        # Collect in memory the list of dicts to be output.  Then sort
        # the list of dicts by their field names before encoding.
        row_data = []
        for content in values:
            fields = content.split('\t')
            # skip the values from the key in field_names, and add values manually.
            row = {field_key: field_value for field_key, field_value in zip(field_names[2:], fields)}
            row[self._get_date_header()] = end_date
            row['Course ID'] = course_id
            row_data.append(row)

        row_data = sorted(row_data, key=itemgetter(*field_names))

        for row_dict in row_data:
            # TSV's are assumed to be written (by Hive) in UTF-8 encoding,
            # so we should not encode the values of row_data before outputting.
            writer.writerow(row_dict)


class StudentModuleEngagementTask(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')

        entity_id = None
        entity_type = None
        if event_type == 'problem_check':
            if event_source != 'server':
                return

            entity_type = 'problem'
            entity_id = event_data.get('problem_id')
        elif event_type == 'play_video':
            entity_type = 'video'
            entity_id = event_data.get('id')
        elif event_type.startswith('edx.forum.'):
            entity_type = 'forum'
            entity_id = event_data.get('commentable_id')

        if not entity_id or not entity_type:
            return

        key = tuple([k.encode('utf8') for k in (date_string, course_id, username, entity_type, entity_id)])

        yield (key, 1)

    def reducer(self, key, values):
        yield ('\t'.join(key + [str(sum(values))]),)

    def output(self):
        return get_target_from_url(self.output_root)

class InsertStudentModuleEngagementIntoMysqlTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, MysqlInsertTask):

    output_root = luigi.Parameter()

    @property
    def table(self):
        return "student_module_engagement"

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('username', 'VARCHAR(30) NOT NULL'),
            ('date', 'DATE NOT NULL'),
            ('module_category', 'VARCHAR(10) NOT NULL'),
            ('encoded_module_id', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT'),
            ('PRIMARY KEY', '(course_id, username, date, module_category, encoded_module_id)')
        ]

    @property
    def insert_source_task(self):
        return StudentModuleEngagementTask(
            n_reduce_tasks=self.n_reduce_tasks,
            interval=self.interval,
            output_root=self.output_root
        )


class StudentEngagementToVerticaTask(
        StudentEngagementTableDownstreamMixin,
        VerticaCopyTask):

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        return (
            JoinedStudentEngagementTableTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                overwrite=self.overwrite,
                interval_type=self.interval_type,
            )
        )

    @property
    def table(self):
        return 'd_student_engagement_{}'.format(self.interval_type)

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def columns(self):
        return [
            ('end_date', 'DATE'),
            ('course_id', 'VARCHAR(255)'),
            ('username', 'VARCHAR(30)'),
            ('email', 'VARCHAR(255)'),
            ('name', 'VARCHAR(255)'),
            ('enrollment_mode', 'VARCHAR(255)'),
            ('cohort', 'STRING'),
            ('days_active', 'INT'),
            ('problems_attempted', 'INT'),
            ('problem_attempts', 'INT'),
            ('problems_correct', 'INT'),
            ('videos_played', 'INT'),
            ('forum_posts', 'INT'),
            ('forum_responses', 'INT'),
            ('forum_comments', 'INT'),
            ('textbook_pages_viewed', 'INT'),
            ('last_subsection_viewed', 'VARCHAR(255)'),
            ('segments', 'VARCHAR(255)'),
        ]


class StudentModuleEngagementToVerticaTask(
        StudentEngagementTableDownstreamMixin,
        VerticaCopyTask):

    output_root = luigi.Parameter()

    @property
    def insert_source_task(self):
        return (
            StudentModuleEngagementTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                output_root=self.output_root,
            )
        )

    @property
    def table(self):
        return 'f_student_module_engagement'

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255)'),
            ('username', 'VARCHAR(30)'),
            ('date', 'DATE'),
            ('module_category', 'VARCHAR(10)'),
            ('encoded_module_id', 'VARCHAR(255)'),
            ('count', 'INT'),
        ]