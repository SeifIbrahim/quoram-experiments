from experiments import (roram_experiment, cockroach_experiment,
                         uoram_experiment, lynch_experiment, DEFAULT_DURATION,
                         DEFAULT_RW_RATIO, DEFAULT_ZIPF, DEFAULT_WARMUP,
                         DEFAULT_K, DEFAULT_REPLICAS)
from instance_session import (InstanceSession, RORAM_TYPES, UORAM_TYPES,
                              COCKROACH_TYPES, LYNCH_TYPES)
import threading

NUM_DATA_POINTS = 3
NUM_CLIENTS = [1, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]


def roram_throughput_latency():
    # start roram
    session = InstanceSession(RORAM_TYPES)
    session.start_instances()
    i = 0
    while i < NUM_DATA_POINTS:
        j = 0
        while j < len(NUM_CLIENTS):
            num_clients = NUM_CLIENTS[j]
            # initialize
            roram_experiment(num_clients,
                             DEFAULT_DURATION,
                             DEFAULT_RW_RATIO,
                             DEFAULT_ZIPF,
                             DEFAULT_WARMUP,
                             DEFAULT_K,
                             'random',
                             DEFAULT_REPLICAS,
                             initialize=True,
                             test_crash=False)

            # start experiment
            results = roram_experiment(num_clients,
                                       DEFAULT_DURATION,
                                       DEFAULT_RW_RATIO,
                                       DEFAULT_ZIPF,
                                       DEFAULT_WARMUP,
                                       DEFAULT_K,
                                       'random',
                                       DEFAULT_REPLICAS,
                                       initialize=False,
                                       test_crash=False)

            with open(f'roram_{num_clients}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()


def cockroach_throughput_latency():
    # start cockroach
    session = InstanceSession(COCKROACH_TYPES)
    session.start_instances()
    i = 0
    while i < NUM_DATA_POINTS:
        j = 0
        while j < len(NUM_CLIENTS):
            num_clients = NUM_CLIENTS[j]
            # initialize
            cockroach_experiment(num_clients,
                                 DEFAULT_DURATION,
                                 DEFAULT_RW_RATIO,
                                 DEFAULT_ZIPF,
                                 DEFAULT_WARMUP,
                                 DEFAULT_REPLICAS,
                                 initialize=True,
                                 test_crash=False)

            # start experiment
            results = cockroach_experiment(num_clients,
                                           DEFAULT_DURATION,
                                           DEFAULT_RW_RATIO,
                                           DEFAULT_ZIPF,
                                           DEFAULT_WARMUP,
                                           DEFAULT_REPLICAS,
                                           initialize=False,
                                           test_crash=False)

            with open(f'cockroach_{num_clients}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()


def uoram_throughput_latency():
    # start uoram
    session = InstanceSession(UORAM_TYPES)
    session.start_instances()
    i = 0
    while i < NUM_DATA_POINTS:
        j = 0
        while j < len(NUM_CLIENTS):
            num_clients = NUM_CLIENTS[j]
            # initialize
            uoram_experiment(num_clients,
                             DEFAULT_DURATION,
                             DEFAULT_RW_RATIO,
                             DEFAULT_ZIPF,
                             DEFAULT_WARMUP,
                             DEFAULT_K,
                             initialize=True,
                             test_crash=False)

            # start experiment
            results = uoram_experiment(num_clients,
                                       DEFAULT_DURATION,
                                       DEFAULT_RW_RATIO,
                                       DEFAULT_ZIPF,
                                       DEFAULT_WARMUP,
                                       DEFAULT_K,
                                       initialize=False,
                                       test_crash=False)

            with open(f'uoram_{num_clients}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()


def lynch_throughput_latency():
    # start lynch
    session = InstanceSession(LYNCH_TYPES)
    session.start_instances()
    i = 0
    while i < NUM_DATA_POINTS:
        j = 0
        while j < len(NUM_CLIENTS):
            num_clients = NUM_CLIENTS[j]
            # initialize
            lynch_experiment(num_clients,
                             DEFAULT_DURATION,
                             DEFAULT_RW_RATIO,
                             DEFAULT_ZIPF,
                             DEFAULT_WARMUP,
                             initialize=True)

            # start experiment
            results = lynch_experiment(num_clients,
                                       DEFAULT_DURATION,
                                       DEFAULT_RW_RATIO,
                                       DEFAULT_ZIPF,
                                       DEFAULT_WARMUP,
                                       initialize=False)

            with open(f'lynch_{num_clients}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()


threading.Thread(target=roram_throughput_latency).start()
threading.Thread(target=uoram_throughput_latency).start()
threading.Thread(target=cockroach_throughput_latency).start()
threading.Thread(target=lynch_throughput_latency).start()
