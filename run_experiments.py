from instance_session import InstanceSession
import instance_state
import subprocess
import time
import sys


def uoram_experiment(clients, load_size, rw_ratio, zipf_exp, warmup_ops,
                     initialize):
    session = InstanceSession(instance_state.uoram_types)
    server = session.instances['uoram-server'][0]
    proxy = session.instances['uoram-proxy'][0]
    config_file = ('oram_file=oram.txt\n'
                   'proxy_thread_count=10\n'
                   'write_back_threshold=10\n'
                   f'proxy_hostname={proxy.public_ip_address}\n'
                   'proxy_port=6000\n'
                   'block_size=4096\n'
                   'blocks_in_bucket=4\n'
                   'block_meta_data_size=8\n'
                   'iv_size=16\n'
                   'min_server_size=1000\n'
                   'num_storage_servers=1\n'
                   'server_port=26257\n'
                   f'storage_hostname1={server.public_ip_address}\n'
                   'max_client_id=2000')
    if initialize:
        session.all_run_wait('killall java')
        print('Killed java')
        # launch server
        session.ssh_clients[server].exec_command(f'cd TaoStore/ && \
                    echo "{config_file}" > config.properties && \
                    nohup ./scripts/run-server.sh')

        # launch proxy
        session.ssh_clients[proxy].exec_command(f'cd TaoStore/ && \
                    echo "{config_file}" > config.properties && \
                    nohup ./scripts/run-proxy.sh')
    else:
        # launch client
        client = session.instances['oram-client'][0]
        stdin, stdout, stderr = session.ssh_clients[client].exec_command(
            'cd TaoStore/'
            f' && echo "{config_file}" > config.properties'
            ' && nohup ./scripts/run-client.sh'
            f' {clients} {load_size} {rw_ratio} {zipf_exp} {warmup_ops}')

        print('Launched client')

        print(stdout.read())
        print(stderr.read())

    session.teardown('pkill -f TaoClient')


# cockroach experiments
def cockroach_experiment(clients, load_size, rw_ratio, zipf_exp, warmup_ops,
                         initialize):
    session = InstanceSession(instance_state.cockroach_types)

    server_ips = [
        server.public_ip_address for server in session.instances['oram-server']
    ]
    proxy = session.instances['oram-proxy'][0]
    config_file = ('oram_file=oram.txt\n'
                   'proxy_thread_count=10\n'
                   'write_back_threshold=10\n'
                   f'proxy_hostname={proxy.public_ip_address}\n'
                   'proxy_port=6000\n'
                   'block_size=4096\n'
                   'blocks_in_bucket=4\n'
                   'block_meta_data_size=8\n'
                   'iv_size=16\n'
                   'min_server_size=1000\n'
                   'num_storage_servers=1\n'
                   'server_port=26257\n'
                   f'storage_hostname1={server_ips[0]}\n'
                   'max_client_id=2000')

    print(f'Proxy will talk to cockroach node at {server_ips[0]}')

    if initialize:
        session.all_run_wait('pkill -9 -f cockroach')
        session.all_run_wait('killall java')
        session.all_run_wait('rm -rf cockroach-data/')
        print('Killed cockroach and java and cleared cockroach data')

        # setup cluster
        for server in session.instances['oram-server']:
            start_cmd = 'nohup cockroach start'\
                ' --insecure'\
                ' --store=type=mem,size=30GB'\
                f' --advertise-addr={server.public_ip_address}'\
                f' --join={",".join(server_ips)}'\
                ' --background'\
                '  > /dev/null 2>&1'

            # print(start_cmd)
            session.ssh_clients[server].exec_command(start_cmd)

        print('Ran initial cluster command on all servers')

        time.sleep(5)

        # send init command and make sure it succeeds
        init_proc = subprocess.Popen(
            ['cockroach', 'init', '--insecure', f'--host={server_ips[0]}'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        try:
            outs, errs = init_proc.communicate(timeout=2)
            print('Initialized cluster:\n'
                  f'returncode: {init_proc.returncode}\n'
                  f'outs: {outs}\n'
                  f'errs: {errs}')
        except subprocess.TimeoutExpired:
            init_proc.kill()
            outs, errs = init_proc.communicate()
            assert False, \
                f'Failed to init cluster. \n outs: {outs} \n errs: {errs}'

        # create table
        cockroach_proc = subprocess.Popen(
            ['cockroach', 'sql', '--insecure', f'--host={server_ips[0]}'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True)
        # uncomment next three lines on the first run
        cockroach_proc.stdin.write(
            'set cluster setting server.remote_debugging.mode=\'any\';'
            'set cluster setting sql.trace.txn.enable_threshold=\'1s\';'
            'create database taostore;'
            'create user seif;'
            'grant all on database taostore to seif;\n')
        cockroach_proc.stdin.write('quit\n')
        try:
            outs, errs = cockroach_proc.communicate(timeout=10)
            print('Created table:\n'
                  f'returncode: {cockroach_proc.returncode}\n'
                  f'outs: {outs}\n'
                  f'errs: {errs}')
        except subprocess.TimeoutExpired:
            cockroach_proc.kill()
            outs, errs = cockroach_proc.communicate()
            assert False, \
                f'Failed to create table. \n outs: {outs} \n errs: {errs}'

        # launch proxy
        session.ssh_clients[proxy].exec_command(f'cd TaoStore/ && \
                    echo "{config_file}" > config.properties && \
                    nohup ./scripts/run-cockroach-proxy.sh')

    else:
        # launch client
        client = session.instances['oram-client'][0]
        stdin, stdout, stderr = session.ssh_clients[client].exec_command(
            'cd TaoStore/'
            f' && echo "{config_file}" > config.properties'
            ' && nohup ./scripts/run-client.sh'
            f' {clients} {load_size} {rw_ratio} {zipf_exp} {warmup_ops}')

        print('Launched client')

        print(stdout.read())
        print(stderr.read())

    session.teardown('pkill -f TaoClient')


# distributed taostore experiments
def roram_experiment(clients, load_size, rw_ratio, zipf_exp, warmup_ops,
                     initialize):
    session = InstanceSession(instance_state.roram_types)

    server_ips = [
        server.public_ip_address
        for server in session.instances['roram-server']
    ]
    proxy_ips = [
        proxy.public_ip_address for proxy in session.instances['roram-proxy']
    ]

    config_file = (f'server_hostname0={server_ips[0]}\n'
                   'server_port0=7000\n'
                   f'server_hostname1={server_ips[1]}\n'
                   'server_port1=7001\n'
                   f'server_hostname2={server_ips[2]}\n'
                   'server_port2=7002\n'
                   f'proxy_hostname0={proxy_ips[0]}\n'
                   'proxy_port0=7100\n'
                   f'proxy_hostname1={proxy_ips[1]}\n'
                   'proxy_port1=7101\n'
                   f'proxy_hostname2={proxy_ips[2]}\n'
                   'proxy_port2=7102\n'
                   'client_port=7200\n'
                   'oram_file=oram.txt\n'
                   'proxy_thread_count=10\n'
                   'write_back_threshold=10\n'
                   'block_size=4096\n'
                   'blocks_in_bucket=4\n'
                   'block_meta_data_size=18\n'
                   'iv_size=16\n'
                   'min_server_size=1000\n'
                   'num_storage_servers=1\n'
                   'num_oram_units=3\n'
                   'incomplete_cache_limit=10000\n'
                   'max_client_id=2000')

    if initialize:
        session.all_run_wait('killall java')
        print('Killed java')

        # launch servers
        server_id = 0
        for server in session.instances['roram-server']:
            session.ssh_clients[server].exec_command(
                f'cd distributed-taostore/ && \
                    echo "{config_file}" > target/config.properties && \
                    nohup ./scripts/run-server.sh {server_id}')
            server_id += 1

        # launch proxies
        proxy_id = 0
        for proxy in session.instances['roram-proxy']:
            session.ssh_clients[proxy].exec_command(
                f'cd distributed-taostore/ && \
                    echo "{config_file}" > target/config.properties && \
                    nohup ./scripts/run-proxy.sh {proxy_id}')
            proxy_id += 1

    else:
        # launch client
        client = session.instances['roram-client'][0]
        stdin, stdout, stderr = session.ssh_clients[client].exec_command(
            'cd distributed-taostore/'
            f' && echo "{config_file}" > target/config.properties'
            ' && ./scripts/run-client.sh '
            f'{clients} {load_size} {rw_ratio} {zipf_exp} {warmup_ops}')

        print('Launched client')

        print(stdout.read())
        print(stderr.read())

    session.teardown('pkill -f TaoClient')


if __name__ == '__main__':
    instance_types = instance_state.types_map.keys()
    if ((len(sys.argv) < 4) or (sys.argv[1] not in instance_types)
            or (sys.argv[2] not in ['init', 'cont'])):
        print(
            f'Usage: {sys.argv[0]} {"|".join(instance_types)} init|cont num_clients'
        )
        sys.exit(1)

    if sys.argv[1] == 'cockroach':
        cockroach_experiment(sys.argv[3], 3 * 60 * 1000, 0.5, 0.9, 100,
                             sys.argv[2] == 'init')
    elif sys.argv[1] == 'cockroach':
        roram_experiment(sys.argv[3], 3 * 60 * 1000, 0.5, 0.9, 1000,
                         sys.argv[2] == 'init')
    elif sys.argv[1] == 'uoram':
        uoram_experiment(sys.argv[3], 3 * 60 * 1000, 0.5, 0.9, 1000,
                         sys.argv[2] == 'init')
