"""
This Lambda is used discover AWS resources and automatically create cloudwatch alarms for them.
The cloudwatch alarms then can create alerts for service desk.
It is scheduled to run in each account every hour. It is configured using the aws config repo.
For more details, see:
https://tech-docs.shd.pantheon.hedgeservx.com/hs-aws-docs/latest/monitoring/cloud_watch.html#cloudwatch-auto-alarms
"""

import yaml
import logging
import boto3
from dataclasses import dataclass, fields, field
from typing import Optional
import re
import os
from hs_common_utilities.util.aws.lambda_json_log_formatter import capture_lambda_metadata, configure_lambda_logging

log = logging.getLogger('auto-alarms')


def _get_namespace_config():
    NAMESPACE_CONFIG = [
        {
            'name': 'AWS/EC2',
            'dimension': 'InstanceId',
            'resource_type': 'ec2:instance',
            'tag_filters': [{'Key': 'hs:std:app-code'}],
            'identifier_from_arn_parse_char': '/',
        },
        {
            'name': 'AWS/EBS',
            'dimension': 'VolumeId',
            'resource_type': 'ec2:volume',
            'tag_filters': [{'Key': 'hs:std:app-code', 'Values': ['MSSQL']}],
            'identifier_from_arn_parse_char': '/',

        },
        {
            'name': 'AWS/SQS',
            'dimension': 'QueueName',
            'resource_type': 'sqs:queue',
            'tag_filters': [{'Key': 'hs:app:monitored', 'Values': ['true']}],
            'identifier_from_arn_parse_char': ':',
        },
        {
            'name': 'AWS/Lambda',
            'dimension': 'FunctionName',
            'resource_type': 'lambda:function',
            'tag_filters': [{'Key': 'hs:std:app-code'}],
            'identifier_from_arn_parse_char': ':',
        },
        {
            'name': 'AWS/AutoScaling',
            'dimension': 'AutoScalingGroupName',
            'resource_type': 'autoscaling:autoScalingGroup',
            'tag_filters': [{'Key': 'hs:std:app-code'}],
            'identifier_from_arn_parse_char': '/',
            'resource_finder': get_autoscaling_resources_and_tags
        },
        ]
    return NAMESPACE_CONFIG


configure_lambda_logging(service_name='HS CW Auto Alarms')


@capture_lambda_metadata
def handler(event, context):  # Required args for lambda functions. pylint: disable=unused-argument
    """
    Lambda to automatically create CloudWatch alarms.
    Inspired by: https://github.com/aws-samples/amazon-cloudwatch-auto-alarms
    """
    log.info('Generating alarms')
    generate_alarms()
    log.info('Finished successfully')


def generate_alarms():
    s3_alarm_config = get_s3_alarm_config()
    # s3_alarm_config = get_local_alarm_config()
    desired_alarms = get_desired_alarms(s3_alarm_config)
    log.info('Got %s desired alarms', len(desired_alarms))
    save_alarms(desired_alarms)


def get_desired_alarms(s3_alarm_config):
    if not s3_alarm_config.create_alarms:
        return []

    alarms = []
    for namespace_config in _get_namespace_config():
        resource_finder = namespace_config.get('resource_finder', get_resources_and_tags)
        for arn, tags in resource_finder(namespace_config['resource_type'], namespace_config['tag_filters']):
            namespace = namespace_config['name']
            resource_identifier = arn.split(namespace_config['identifier_from_arn_parse_char'])[-1]
            for alarm_config in s3_alarm_config.alarm_configs:
                if alarm_config.namespace != namespace or not alarm_config.is_included(tags):
                    continue

                alarm_data = AlarmData(namespace=namespace,
                                       resource_identifier=resource_identifier,
                                       resource_name=tags.get('Name'),
                                       statistic=alarm_config.statistic,
                                       metric_name=alarm_config.metric_name,
                                       metric_math=alarm_config.metric_math,
                                       comparison_operator=alarm_config.comparison_operator,
                                       threshold=alarm_config.get_threshold(tags),
                                       datapoints_to_alarm=alarm_config.datapoints_to_alarm,
                                       evaluation_periods=alarm_config.evaluation_periods,
                                       period=alarm_config.period,
                                       display_name=alarm_config.display_name,
                                       amdb_number=alarm_config.amdb_number,
                                       sdp_priority=alarm_config.sdp_priority,
                                       maintenance_window=bool(alarm_config.maintenance_window),
                                       software_owner=tags.get('hs:std:svc-software-owner', 'TechOps'),
                                       app_code=tags.get('hs:std:app-code', 'UNKNOWN'),
                                       monitored=alarm_config.create_tickets and s3_alarm_config.create_tickets)
                alarms.append(alarm_data)
    return alarms


def save_alarms(desired_alarms):
    cloudwatch = boto3.client('cloudwatch')

    desired_alarm_map = {alarm.alarm_name: alarm for alarm in desired_alarms}

    for arn, tags in get_resources_and_tags('cloudwatch', [{'Key': 'hs:app:auto-generated', 'Values': ['true']}]):
        existing_alarm = AlarmData.create_from_alarm_tags(tags)
        existing_alarm_name = ':'.join(arn.split(':')[6:])
        desired_alarm = desired_alarm_map.pop(existing_alarm_name, None)
        if not desired_alarm:
            log.info('Deleting existing alarm %s %s', existing_alarm_name, existing_alarm)
            cloudwatch.delete_alarms(AlarmNames=[existing_alarm_name])
        elif existing_alarm == desired_alarm:
            log.debug('Alarm already configured correctly %s', desired_alarm.alarm_name)
        else:
            log.info('Updating existing alarm. Before %s, After %s', existing_alarm, desired_alarm)
            cloudwatch.delete_alarms(AlarmNames=[existing_alarm_name])  # put won't change tags unless we delete first
            cloudwatch.put_metric_alarm(**desired_alarm.get_alarm_json())

    for desired_alarm in desired_alarm_map.values():
        log.info('Adding missing alarm %s', desired_alarm)
        cloudwatch.put_metric_alarm(**desired_alarm.get_alarm_json())


def get_s3_alarm_config():
    s3 = boto3.resource('s3')
    environment = os.getenv('AWS_ENVIRONMENT')
    key = f'configs/{environment}/monitoring/cloudwatch_auto_alarms.yaml'
    log.info('Loading config from %s', key)
    s3_object = s3.Object('hedgeserv-shd-ci-us-east-2-s3-configs', key)
    alarm_config = yaml.full_load(s3_object.get()['Body'])
    return S3AlarmConfig(alarm_config)


def get_local_alarm_config():
    # used when testing this entire thing locally.
    namespace_file_path = r"C:\Dev\hs_aws_applications_configs\configs\uat\monitoring\cloudwatch_auto_alarms.yaml"
    with open(namespace_file_path, "r") as namespace_file_fp:
        alarm_config = yaml.full_load(namespace_file_fp)

    return S3AlarmConfig(alarm_config)


def get_resources_and_tags(resource_type, tag_filters):
    tagging = boto3.client('resourcegroupstaggingapi')

    for results in tagging.get_paginator('get_resources').paginate(ResourceTypeFilters=[resource_type],
                                                                   TagFilters=tag_filters):
        for resource_info in results['ResourceTagMappingList']:
            tags = {row['Key']: row['Value'] for row in resource_info['Tags']}
            yield resource_info['ResourceARN'], tags


def get_autoscaling_resources_and_tags(_, tag_filters):
    asg_client = boto3.client('autoscaling')

    for page in asg_client.get_paginator('describe_auto_scaling_groups').paginate():
        for asg in page['AutoScalingGroups']:
            tags = {tag['Key']: tag['Value'] for tag in asg.get('Tags', {})}
            if _matches_tag_filters(tags, tag_filters):
                yield asg['AutoScalingGroupARN'], tags


def _matches_tag_filters(tags, filters):
    """All filters must match: key present, and if Values given -> tag value ∈ Values."""
    for f in filters:
        key = f['Key']
        if key not in tags:
            return False
        vals = f.get('Values', [])
        if vals and tags.get(key) not in vals:
            return False
    return True


class CustomCalc:

    @classmethod
    def percent_of_tag(cls, tags, threshold_tag_name, threshold_percent):
        return str(int(int(tags.get(threshold_tag_name)) * (threshold_percent / 100)))

    @classmethod
    def ninety_percent_of_iops(cls, tags):
        # Keeping this for reverse compatibility, this will be deprecated in favor of percent_of_tag.
        return cls.percent_of_tag(tags, 'hs:app:iops', 90)


class S3AlarmConfig:

    def __init__(self, config):
        self.config = config

    @property
    def alarm_configs(self):
        return [AlarmConfig(c) for c in self.config['alarms']]

    @property
    def create_alarms(self):
        return self.config.get('config', {}).get('create_alarms', True)

    @property
    def create_tickets(self):
        return self.config.get('config', {}).get('create_tickets', True)

    @property
    def maintenance_window(self):
        """
        If True then we do NOT generate tickets during the weekend blackout window for alarms.
        Ideally we don't want to set this, we would prefer to know as soon as there is some problem.
        Unfortunately some services (like SQL Server or anything that depends on it)
        are not stable during the weekend (e.g. because of DB reorgs).
        This prevents generating false alarms and creating ticket fatigue for those services.
        """
        return self.config.get('config', {}).get('maintenance_window', False)


class AlarmConfig:

    def __init__(self, config):
        self.config = config

    def is_included(self, tags):
        for tag_name, included_values in self.included_tags.items():
            tag_value = tags.get(tag_name)
            if not tag_value or not re.match(included_values, tag_value):
                return False

        for tag_name, excluded_values in self.excluded_tags.items():
            tag_value = tags.get(tag_name)
            if tag_value and re.match(excluded_values, tag_value):
                return False

        return True

    @property
    def amdb_number(self):
        return str(self.config['amdb_number'])

    @property
    def sdp_priority(self):
        return str(self.config.get('sdp_priority', '3 - Moderate'))

    @property
    def namespace(self):
        return self.config['namespace']

    def get_threshold(self, tags):
        threshold = str(self.config['threshold'])
        if not threshold.startswith('CUSTOM:'):
            return threshold
        return getattr(CustomCalc, threshold.rsplit(':', maxsplit=1)[-1])(tags, self.threshold_tag_name, self.threshold_percent)

    @property
    def threshold_tag_name(self):
        return self.config.get('threshold_tag_name', 'hs:app:iops')

    @property
    def threshold_percent(self):
        return self.config.get('threshold_percent', 90)

    @property
    def comparison_operator(self):
        return self.config.get('comparison_operator', 'GreaterThanThreshold')

    @property
    def metric_name(self):
        if self.metric_math:
            # This is converted to a AWS Tag so we are limited with what characters we can use
            return f'{self.metric_math["operator"]}-{"+".join(self.metric_math["operands"])}'

        return self.config['metric_name']

    @property
    def metric_math(self):
        return self.config.get('metric_math')

    @property
    def statistic(self):
        return self.config.get('statistic', 'Average')

    @property
    def period(self):
        return str(self.config.get('period', 60))

    @property
    def datapoints_to_alarm(self):
        return str(self.config.get('datapoints_to_alarm', 1))

    @property
    def evaluation_periods(self):
        return str(self.config.get('evaluation_periods', self.datapoints_to_alarm))

    @property
    def included_tags(self):
        return self.config.get('included_tags', {})

    @property
    def excluded_tags(self):
        return self.config.get('excluded_tags', {})

    @property
    def display_name(self):
        return self.config.get('display_name')

    @property
    def maintenance_window(self):
        return self.config.get('maintenance_window')

    @property
    def create_tickets(self):
        return self.config.get('create_tickets', True)


DIMENSION_MAP = {ns_config['name']: ns_config['dimension'] for ns_config in _get_namespace_config()}

@dataclass
class AlarmData:
    """ Data that is needed to create an alerm """
    namespace: str  # 'AWS/EC2'
    resource_identifier: str
    resource_name: str
    statistic: str
    metric_name: str
    comparison_operator: str
    threshold: str
    datapoints_to_alarm: str  # datapoints_to_alarm is how many checks have to fail within the evaluation_periods
    evaluation_periods: str  # evaluation_periods is how many checks we look at in a row
    period: str  # period is how often to check
    software_owner: str
    app_code: str
    amdb_number: str
    sdp_priority: str
    display_name: str
    maintenance_window: bool
    monitored: bool
    metric_math: Optional[dict] = field(compare=False, default=None)  # Can compare metric_name instead

    @property
    def dimension(self):
        return DIMENSION_MAP[self.namespace]

    @property
    def alarm_name(self):
        """
        We include the resource identifier because if the alarm name is not unique it will update the other alarm.
        For example with EC2 instances created by an ASGs, we are not able to create unique Name tags for the underlying EC2 instances.

        For some use cases the logic in the alarm_name_tag includes the resource_identifier, see the alarm_name_tag property, So the name ends up as
        'AUTO-ALARM <Some string that has resource_identifier and other stuff> (<resource_identifier>)
        """
        if self.resource_identifier in self.alarm_name_tag:
            return f'AUTO-ALARM {self.alarm_name_tag}'
        return f'AUTO-ALARM {self.alarm_name_tag} ({self.resource_identifier})'

    @property
    def alarm_name_tag(self):
        """ Without the auto alarm to keep it shorter/cleaner """
        if self.display_name:
            resource_name = self.resource_name or self.resource_identifier
            return f'{self.display_name} for {resource_name}'
        return f'{self.resource_identifier} ({self.namespace}) {self.statistic}-{self.metric_name} is {self.comparison_operator} {self.threshold} ({self.datapoints_to_alarm}/{self.evaluation_periods} periods of {self.period}s)'

    @classmethod
    def create_from_alarm_tags(cls, tags):
        field_2_tag = {f.name: f'hs:alarm:{f.name}' for f in fields(AlarmData)}
        # Custom field mappings
        field_2_tag['sdp_priority'] = 'hs:app:sdp-priority'
        field_2_tag['maintenance_window'] = 'hs:app:maintenance-window'
        field_2_tag['app_code'] = 'hs:std:app-code'
        field_2_tag['software_owner'] = 'hs:std:svc-software-owner'
        field_2_tag['monitored'] = 'hs:app:monitored'
        field_2_tag['amdb_number'] = 'hs:app:amdb'

        values = {field_name: tags.get(tag_name) for field_name, tag_name in field_2_tag.items()}

        # Custom value parsing
        values['maintenance_window'] = str_to_bool(values.get('maintenance_window', False))
        values['monitored'] = str_to_bool(values.get('monitored', False))
        values['amdb_number'] = values['amdb_number'].split('_')[-1] if values.get('amdb_number') else ''

        return AlarmData(**values)

    def get_alarm_json(self):
        """ The actual JSON used to created the alarm"""
        dimensions = [{'Name': self.dimension, 'Value': self.resource_identifier}]

        tags = [
            # Tags that are set using pre-existing or external naming conventions
            {'Key': 'Name', 'Value': self.alarm_name_tag},
            {'Key': 'hs:app:monitored', 'Value': bool_to_str(self.monitored)},
            {'Key': 'hs:app:auto-generated', 'Value': 'true'},
            {'Key': 'hs:app:amdb', 'Value': f'sdpmt_{self.amdb_number}'},
            {'Key': 'hs:app:sdp-priority', 'Value': self.sdp_priority},
            {'Key': 'hs:app:maintenance-window', 'Value': bool_to_str(self.maintenance_window)},
            {'Key': 'hs:std:svc-software-owner', 'Value': self.software_owner},
            {'Key': 'hs:std:app-code', 'Value': self.app_code},

            # Tags that are only used for lambda function
            {'Key': 'hs:alarm:namespace', 'Value': self.namespace},
            {'Key': 'hs:alarm:resource_identifier', 'Value': self.resource_identifier},
            {'Key': 'hs:alarm:resource_name', 'Value': self.resource_name},
            {'Key': 'hs:alarm:statistic', 'Value': self.statistic},
            {'Key': 'hs:alarm:metric_name', 'Value': self.metric_name},
            {'Key': 'hs:alarm:comparison_operator', 'Value': self.comparison_operator},
            {'Key': 'hs:alarm:threshold', 'Value': self.threshold},
            {'Key': 'hs:alarm:datapoints_to_alarm', 'Value': self.datapoints_to_alarm},
            {'Key': 'hs:alarm:evaluation_periods', 'Value': self.evaluation_periods},
            {'Key': 'hs:alarm:period', 'Value': self.period},
            {'Key': 'hs:alarm:display_name', 'Value': self.display_name},
        ]

        tags = [tag for tag in tags if tag['Value'] is not None]

        result = {
            'AlarmName': self.alarm_name,
            'AlarmDescription': 'Created by cloudwatch-auto-alarms',
            'DatapointsToAlarm': int(self.datapoints_to_alarm),
            'EvaluationPeriods': int(self.evaluation_periods),
            'ComparisonOperator': self.comparison_operator,
            'Threshold': float(self.threshold),
            'Tags': tags}

        if self.metric_math:
            metrics = []
            operand_names = []
            for operand_index, operand in enumerate(self.metric_math['operands']):
                operand_name = f'm{operand_index + 1}'
                operand_names.append(operand_name)
                metrics.append({
                    'Id': operand_name,
                    'MetricStat': {
                        'Metric': {
                            'MetricName': operand,
                            'Namespace': self.namespace,
                            'Dimensions': dimensions
                        },
                        'Stat': self.statistic,
                        'Period': int(self.period),
                    },
                    'ReturnData': False
                })

            if self.metric_math["operator"] == "SUM":
                expression = f'{self.metric_math["operator"]}([{",".join(operand_names)}])'
            elif self.metric_math["operator"] == "SUBTRACTION":
                expression = f'{"-".join(operand_names)}'

            if self.metric_math.get('divisor'):
                expression = f'{expression}/{self.metric_math["divisor"]}'

            metrics.append({'Id': 'e1',
                            'Expression': expression,
                            'ReturnData': True})
            result['Metrics'] = metrics
        else:
            # Simple format for metrics that will also show up with resource (e.g. on the EC2 Console for EC2 Alarms)
            result['Namespace'] = self.namespace
            result['MetricName'] = self.metric_name
            result['Dimensions'] = dimensions
            result['Statistic'] = self.statistic
            result['Period'] = int(self.period)

        return result


def bool_to_str(value):
    return str(bool(value)).lower()


def str_to_bool(value):
    return str(value).lower() == 'true'

# used for testing locally
# if __name__ == '__main__':
#     handler("", "")

#
#generate_alarms()
