import boto.swf.layer2 as swf
from boto.dynamodb2.table import Table
import boto.ec2, redis, boto.cloudformation
from boto.utils import get_instance_metadata

region = get_instance_metadata()['placement']['availability-zone'][:-1]
ec2_conn = boto.ec2.connect_to_region(region)
cf_conn=boto.cloudformation.connect_to_region(region)

iid = get_instance_metadata()['instance-id']
reservations = ec2_conn.get_all_reservations(instance_ids='%s' %iid)
instance = reservations[0].instances[0]
tags= instance.tags

DOMAIN = 'demo'
VERSION = '1.0'
TASKLIST = 'default'

r = redis.StrictRedis(host='tes-pu-3jpt4w8eifgu.qpeias.0001.usw2.cache.amazonaws.com', port = 6379, db=0)
dynamo_config=cf_conn.describe_stack_resource(tags['aws:cloudformation:stack-name'],'configTable1')
tbl=dynamo_config['DescribeStackResourceResponse']['DescribeStackResourceResult']['StackResourceDetail']['PhysicalResourceId']
table_obj = Table(tbl)

class HelloWorker(swf.ActivityWorker):

    domain = DOMAIN
    version = VERSION
    task_list = TASKLIST

    def run(self):
        while True:
            activity_task = self.poll()
            if 'activityId' in activity_task:
                requestid = activity_task['workflowExecution']['workflowId']
                result = table_obj.get_item(env=tags['environment'], option='demo')
                data = result['data']
                r.set(requestid, data)
                print 'Hello, World!'
                self.complete()
            if activity_task is False:
                activity_task = "Nothing to do"

if __name__ == '__main__':
    HelloWorker().run()