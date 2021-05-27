import boto3
import paramiko
import instance_state


class InstanceSession:
    def __init__(self, instance_types):
        self.ssh_clients = {}
        self.instances = {}  # keyed by type

        for region in instance_state.regions:
            ec2 = boto3.resource('ec2', region_name=region)
            for instance in ec2.instances.all():
                if not instance.tags:
                    continue
                for tag in instance.tags:
                    if tag['Key'] == 'Type' and tag['Value'] in instance_types:
                        # establish an ssh connection
                        key = paramiko.RSAKey.from_private_key_file(
                            f'/home/seif/.ssh/r-oram-{region}.pem')
                        client = paramiko.SSHClient()
                        client.set_missing_host_key_policy(
                            paramiko.AutoAddPolicy())
                        try:
                            client.connect(hostname=instance.public_ip_address,
                                           username='ubuntu',
                                           pkey=key)
                        except paramiko.ssh_exception.AuthenticationException:
                            print(f'Authentication Failed. \n \
                                    ip: {instance.public_ip_address}\n \
                                    region: {region}')

                        self.ssh_clients[instance] = client

                        # append to list of instances
                        if tag['Value'] not in self.instances:
                            self.instances[tag['Value']] = []
                        self.instances[tag['Value']].append(instance)
                        break

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
