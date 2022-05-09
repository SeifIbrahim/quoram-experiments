import boto3
import paramiko
from collections import defaultdict
import time
from pathlib import Path

REGIONS = ['us-west-1', 'us-east-1', 'us-east-2', 'us-west-2']
COCKROACH_TYPES = ['cockroach-client', 'cockroach-proxy', 'cockroach-server']
RORAM_TYPES = ['roram-client', 'roram-proxy', 'roram-server']
UORAM_TYPES = ['uoram-client', 'uoram-proxy', 'uoram-server']
LYNCH_TYPES = ['lynch-client', 'lynch-server']
INSTANCE_TYPES = {
    'cockroach': COCKROACH_TYPES,
    'roram': RORAM_TYPES,
    'uoram': UORAM_TYPES,
    'lynch': LYNCH_TYPES
}


class InstanceSession:
    def __init__(self, instance_types):
        self.type_instances = defaultdict(list)
        self.ssh_clients = {}
        self.instance_region = {}
        self.region_instances = defaultdict(list)

        # populate the instances based on their types
        for region in REGIONS:
            ec2 = boto3.resource('ec2', region_name=region)
            for instance in ec2.instances.all():
                if not instance.tags:
                    continue
                for tag in instance.tags:
                    if tag['Key'] == 'Type' and tag['Value'] in instance_types:
                        self.type_instances[tag['Value']].append(instance)
                        self.instance_region[instance] = region
                        self.region_instances[region].append(instance)
                        break

    def start_instances(self):
        for region, instances in self.region_instances.items():
            ec2_client = boto3.client('ec2', region_name=region)
            ec2_client.start_instances(
                InstanceIds=[instance.instance_id for instance in instances])
            print(f'Started instances {instances} in region {region}')

        # wait for them to start running
        for instance in self.instance_region:
            instance.wait_until_running()

        time.sleep(10)
        print('Instances are running')

    def stop_instances(self):
        for region, instances in self.region_instances.items():
            ec2_client = boto3.client('ec2', region_name=region)
            ec2_client.stop_instances(
                InstanceIds=[instance.instance_id for instance in instances])
            print(f'Stopped instances {instances} in region {region}')

        for instance in self.instance_region:
            instance.wait_until_stopped()

        print('Instances have stopped')

    def connect_ssh(self):
        # establish an ssh connection
        for region, instances in self.region_instances.items():
            key = paramiko.RSAKey.from_private_key_file(
                f'{Path.home()}.ssh/r-oram-{region}.pem')
            for instance in instances:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                try:
                    client.connect(hostname=instance.public_ip_address,
                                   username='ubuntu',
                                   pkey=key)
                except paramiko.ssh_exception.AuthenticationException:
                    print(f'Authentication Failed. \n \
                            ip: {instance.public_ip_address}\n \
                            region: {region}')

                self.ssh_clients[instance] = client

    def all_run(self, command):
        channels = []
        for client in self.ssh_clients.values():
            channel = client.exec_command(command)
            channels.append(channel)
        return channels

    def all_run_wait(self, command):
        channels = self.all_run(command)
        for channel in channels:
            _, stdout, _ = channel
            stdout.channel.recv_exit_status()

    def teardown(self, command):
        for client in self.ssh_clients.values():
            # close the client connection once the job is done
            _, stdout, _ = client.exec_command(command)
            stdout.channel.recv_exit_status()
            client.close()
