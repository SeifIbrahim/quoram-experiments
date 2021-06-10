import boto3
import sys

regions = ['us-west-1', 'us-east-1', 'us-east-2']
cockroach_types = ['oram-client', 'oram-proxy', 'oram-server']
roram_types = ['roram-client', 'roram-proxy', 'roram-server']
uoram_types = ['uoram-client', 'uoram-proxy', 'uoram-server']
lynch_types = ['lynch-client', 'lynch-server']
types_map = {
    'cockroach': cockroach_types,
    'roram': roram_types,
    'uoram': uoram_types,
    'lynch': lynch_types
}


# start/stop the specified instance types
def instance_state(start, instance_types):
    for region in regions:
        instances = []
        ec2_resource = boto3.resource('ec2', region_name=region)
        for instance in ec2_resource.instances.all():
            if not instance.tags:
                print(f'Instance {instance} has no tags')
                continue
            for tag in instance.tags:
                if tag['Key'] == 'Type' and tag['Value'] in instance_types:
                    instances.append(instance.instance_id)
                    break
        if not instances:
            continue
        ec2_client = boto3.client('ec2', region_name=region)
        if start:
            ec2_client.start_instances(InstanceIds=instances)
            print(f'Started instances {instances} in region {region}')
        else:
            ec2_client.stop_instances(InstanceIds=instances)
            print(f'Stopped instances {instances} in region {region}')


if __name__ == '__main__':
    instance_types = types_map.keys()
    if (len(sys.argv) < 3 or sys.argv[1] not in ['start', 'stop']
            or sys.argv[2] not in instance_types):
        print(f'Usage: {sys.argv[0]} start|stop {"|".join(instance_types)}')
        sys.exit(1)
    instance_state(sys.argv[1] == 'start', types_map[sys.argv[2]])
