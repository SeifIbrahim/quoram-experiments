import functools
import signal
import os
import errno
from instance_session import (InstanceSession, COCKROACH_TYPES, RORAM_TYPES,
                              UORAM_TYPES, LYNCH_TYPES)
import subprocess
import time

DEFAULT_CLIENTS = 100
DEFAULT_DURATION = 3 * 60 * 1000
DEFAULT_RW_RATIO = 0.5
DEFAULT_ZIPF = 0.9
DEFAULT_WARMUP = 100
DEFAULT_K = 40
DEFAULT_REPLICAS = 3


def timeout(seconds=10 * 60, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wrapper

    return decorator


@timeout()
def lynch_experiment(clients, load_size, rw_ratio, zipf_exp, warmup_ops,
                     initialize):
    results = ''

    session = InstanceSession(LYNCH_TYPES)
    session.connect_ssh()

    server_ips = [
        server.public_ip_address
        for server in session.type_instances['lynch-server']
    ]
    client_ips = [
        client.public_ip_address
        for client in session.type_instances['lynch-client']
    ]
    config_file = (f'server_hostname0={server_ips[0]}\n'
                   'server_port0=7000\n'
                   f'server_hostname1={server_ips[1]}\n'
                   'server_port1=7001\n'
                   f'server_hostname2={server_ips[2]}\n'
                   'server_port2=7002\n'
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
                   'max_client_id=2000\n'
                   f'proxy_service_threads={clients}\n'
                   'client_timeout=100000')

    if initialize:
        print('Starting Lynch Experiment')
        print(f"Client IPs:\n{client_ips}")
        print(config_file)

        session.all_run_wait('killall java')
        print('Killed java')

        # launch servers
        server_id = 0
        for server in session.type_instances['lynch-server']:
            session.ssh_clients[server].exec_command(
                f'cd distributed-taostore/ && \
                    echo "{config_file}" > target/config.properties && \
                    nohup ./scripts/run-insecure-server.sh {server_id}')
            server_id += 1

    else:
        print('Continuing Lynch Experiment')

        # launch client
        client_outputs = [
            session.ssh_clients[client].exec_command(
                'cd distributed-taostore/'
                f' && echo "{config_file}" > target/config.properties'
                ' && ./scripts/run-insecure-client.sh '
                f'{clients} {load_size} {rw_ratio} {zipf_exp} {warmup_ops}')
            for client in session.type_instances['lynch-client']
        ]

        print('Launched clients')

        for i, output in enumerate(client_outputs):
            results += f'---------- CLIENT {i} OUTPUT ----------\n'
            results += output[1].read().decode() + '\n'
            results += output[2].read().decode() + '\n'

        print(results)

        session.teardown('pkill -f InsecureTaoClient')

        return results


@timeout()
def uoram_experiment(clients, load_size, rw_ratio, zipf_exp, warmup_ops, k,
                     initialize):
    results = ''

    session = InstanceSession(UORAM_TYPES)
    session.connect_ssh()

    server = session.type_instances['uoram-server'][0]
    proxy = session.type_instances['uoram-proxy'][0]
    config_file = ('oram_file=oram.txt\n'
                   'proxy_thread_count=10\n'
                   f'write_back_threshold={k}\n'
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
                   'max_client_id=2000\n'
                   f'connection_pool_size={clients}\n'
                   f'proxy_service_threads={clients}')

    if initialize:
        print('Starting Unreplicated TaoStore Experiment')
        print(f"Proxy IP:\n{proxy.public_ip_address}")
        print(config_file)

        session.all_run_wait('killall java')
        print('Killed java')
        # launch server
        session.ssh_clients[server].exec_command(f'cd TaoStore/ && \
                    echo "{config_file}" > config.properties && \
                    nohup ./scripts/run-server.sh > server.log')

        # launch proxy
        session.ssh_clients[proxy].exec_command(f'cd TaoStore/ && \
                    echo "{config_file}" > config.properties && \
                    nohup ./scripts/run-proxy.sh > proxy.log')
    else:
        print('Continuing Unreplicated TaoStore Experiment')

        # launch client
        client_outputs = [
            session.ssh_clients[client].exec_command(
                'cd TaoStore/'
                f' && echo "{config_file}" > config.properties'
                ' && nohup ./scripts/run-client.sh'
                f' {clients} {load_size} {rw_ratio}'
                f' {zipf_exp} {warmup_ops} {i}')
            for i, client in enumerate(session.type_instances['uoram-client'])
        ]

        print('Launched clients')

        for i, output in enumerate(client_outputs):
            results += f'---------- CLIENT {i} OUTPUT ----------\n'
            results += output[1].read().decode() + '\n'
            results += output[2].read().decode() + '\n'

        print(results)

        session.teardown('pkill -f TaoClient')

        return results


# cockroach experiments
@timeout()
def cockroach_experiment(clients, test_duration, rw_ratio, zipf_exp,
                         warmup_ops, num_replicas, initialize, test_crash):
    results = ''

    session = InstanceSession(COCKROACH_TYPES)
    session.connect_ssh()

    server_ips = [
        server.public_ip_address
        for server in session.type_instances['cockroach-server']
    ]

    # The CockroachDB replica that TaoProxy talks to
    server_to_contact = server_ips[0]

    proxy = session.type_instances['cockroach-proxy'][0]
    config_file = ('oram_file=oram.txt\n'
                   'proxy_thread_count=30\n'
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
                   f'storage_hostname1={server_to_contact}\n'
                   'max_client_id=2000\n'
                   f'proxy_service_threads={clients}\n'
                   f'connection_pool_size={clients}')

    if initialize:
        print('Starting CockroachDB Experiment')
        print(f'Proxy will talk to cockroach node at {server_to_contact}')
        print(f"Proxy IP:\n{proxy.public_ip_address}")

        session.all_run_wait('pkill -9 -f cockroach')
        session.all_run_wait('killall java')
        session.all_run_wait('rm -rf cockroach-data/')
        print('Killed cockroach and java and cleared cockroach data')

        # setup cluster
        for server in session.type_instances['cockroach-server']:
            start_cmd = 'nohup cockroach start'\
                ' --cache=.35 --max-sql-memory=.35'\
                ' --insecure'\
                f' --advertise-addr={server.public_ip_address}'\
                f' --join={",".join(server_ips)}'\
                ' --background'\
                '  > /dev/null 2>&1'
            # ' --store=type=mem,size=30GB'\

            # print(start_cmd)
            session.ssh_clients[server].exec_command(start_cmd)

        print('Ran initial cluster command on all servers')

        time.sleep(5)

        # send init command and make sure it succeeds
        init_proc = subprocess.Popen(
            ['cockroach', 'init', '--insecure', f'--host={server_to_contact}'],
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
            ['cockroach', 'sql', '--insecure', f'--host={server_to_contact}'],
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
                    nohup ./scripts/run-cockroach-proxy.sh 2>&1 > proxy.log')

    else:
        print('Continuing CockroachDB Experiment')

        # launch client
        client_outputs = [
            session.ssh_clients[client].exec_command(
                'cd TaoStore/'
                f' && echo "{config_file}" > config.properties'
                ' && nohup ./scripts/run-client.sh'
                f' {clients} {test_duration} {rw_ratio}'
                f' {zipf_exp} {warmup_ops} {i}') for i, client in enumerate(
                    session.type_instances['cockroach-client'])
        ]

        print('Launched clients')

        if test_crash:
            num_to_crash = num_replicas - (num_replicas // 2 + 1)
            # crash_delay = (test_duration // 2) // 1000
            crash_delay = 150
            print(f'Crashing {num_to_crash} units after {crash_delay} seconds')
            time.sleep(crash_delay)
            for i in range(num_to_crash):
                server = session.type_instances['cockroach-server'][i]
                session.ssh_clients[server].exec_command(
                    'pkill -9 -f cockroach')
            print('Finished crashing units')

        for i, output in enumerate(client_outputs):
            results += f'---------- CLIENT {i} OUTPUT ----------\n'
            results += output[1].read().decode() + '\n'
            results += output[2].read().decode() + '\n'

        print(results)

        session.teardown('pkill -f TaoClient')

        return results


# replicated oram experiments
@timeout()
def roram_experiment(clients, test_duration, rw_ratio, zipf_exp, warmup_ops, k,
                     quorum_type, num_replicas, initialize, test_crash):
    results = ''

    session = InstanceSession(RORAM_TYPES)
    session.connect_ssh()

    server_ips = [
        server.public_ip_address
        for server in session.type_instances['roram-server']
    ]
    proxy_ips = [
        proxy.public_ip_address
        for proxy in session.type_instances['roram-proxy']
    ]
    client_ips = [
        client.public_ip_address
        for client in session.type_instances['roram-client']
    ]
    server_and_proxy_config = ''.join([
        f'server_hostname{i}={server_ips[i]}\n'
        f'server_port{i}={7000 + i}\n'
        f'proxy_hostname{i}={proxy_ips[i]}\n'
        f'proxy_port{i}={7100 + i}\n' for i in range(num_replicas)
    ])

    config_file = (server_and_proxy_config + 'client_port=7200\n'
                   'oram_file=oram.txt\n'
                   'proxy_thread_count=10\n'
                   f'write_back_threshold={k}\n'
                   'block_size=4096\n'
                   'blocks_in_bucket=4\n'
                   'block_meta_data_size=18\n'
                   'iv_size=16\n'
                   'min_server_size=1000\n'
                   'num_storage_servers=1\n'
                   f'num_oram_units={num_replicas}\n'
                   'incomplete_cache_limit=100000\n'
                   'max_client_id=2000\n'
                   f'proxy_service_threads={clients}\n'
                   'access_daemon_delay=0\n'
                   'client_timeout=100000')

    if initialize:
        print('Starting Quoram Experiment')
        print(f"Client IPs:\n{client_ips}")
        print(config_file)

        session.all_run_wait('killall java')
        print('Killed java')

        # launch servers
        server_id = 0
        for server in session.type_instances['roram-server']:
            session.ssh_clients[server].exec_command(
                f'cd distributed-taostore/ && \
                    echo "{config_file}" > target/config.properties && \
                    nohup ./scripts/run-server.sh {server_id} \
                    2>&1 > server{server_id}.log')
            server_id += 1

        # launch proxies
        proxy_id = 0
        for proxy in session.type_instances['roram-proxy']:
            session.ssh_clients[proxy].exec_command(
                f'cd distributed-taostore/ && \
                    echo "{config_file}" > target/config.properties && \
                    nohup ./scripts/run-proxy.sh {proxy_id} \
                    2>&1 > proxy{proxy_id}.log')
            proxy_id += 1

    else:
        print('Continuing Quoram Experiment')

        # launch clients
        client_outputs = [
            session.ssh_clients[client].exec_command(
                f'cd distributed-taostore/ \
                 && echo "{config_file}" > target/config.properties \
                 && ./scripts/run-client.sh \
                {clients} {test_duration} {rw_ratio} {zipf_exp} {warmup_ops} \
                        {quorum_type} {i}')
            for i, client in enumerate(session.type_instances['roram-client'])
        ]

        print('Launched clients')

        if test_crash:
            num_to_crash = num_replicas - (num_replicas // 2 + 1)
            # crash_delay = (test_duration // 2) // 1000
            crash_delay = 150
            print(f'Crashing {num_to_crash} units after {crash_delay} seconds')
            time.sleep(crash_delay)
            for i in range(num_to_crash):
                proxy = session.type_instances['roram-proxy'][i]
                server = session.type_instances['roram-server'][i]
                session.ssh_clients[proxy].exec_command('killall java')
                session.ssh_clients[server].exec_command('killall java')
            print('Finished crashing units')

        for i, output in enumerate(client_outputs):
            results += f'---------- CLIENT {i} OUTPUT ----------\n'
            results += output[1].read().decode() + '\n'
            results += output[2].read().decode() + '\n'

        session.teardown('pkill -f TaoClient')

        print(results)

        return results
