import boto.swf.layer2 as swf
from boto.dynamodb2.table import Table
import boto.ec2, redis, boto.cloudformation, logging, time
from boto.utils import get_instance_metadata
logging.basicConfig(filename='/var/log/app/app.log',level=logging.INFO)

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
dynamo_config=cf_conn.describe_stack_resource(tags['aws:cloudformation:stack-name'],'ServiceConfigTable')
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
                result = table_obj.get_item(Key='demo')
                data = result['data']
                r.set(requestid, data)
                print 'Hello, World!'
                logging.info(str(time.time()))
                self.complete()
            if activity_task is False:
                activity_task = "Nothing to do"

if __name__ == '__main__':
    HelloWorker().run()