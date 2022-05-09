from experiments import (roram_experiment, cockroach_experiment,
                         uoram_experiment, lynch_experiment, DEFAULT_DURATION,
                         DEFAULT_RW_RATIO, DEFAULT_ZIPF, DEFAULT_WARMUP,
                         DEFAULT_K, DEFAULT_REPLICAS)
from instance_session import (InstanceSession, INSTANCE_TYPES, RORAM_TYPES,
                              UORAM_TYPES, COCKROACH_TYPES, LYNCH_TYPES)
import sys

NUM_DATA_POINTS = 3
NUM_CLIENTS = [3, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]


def roram_throughput_latency():
    # start roram
    session = InstanceSession(RORAM_TYPES)
    session.start_instances()
    for i in range(NUM_DATA_POINTS):
        for num_clients in NUM_CLIENTS:
            try:
                # initialize
                roram_experiment(num_clients // 3,
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
                results = roram_experiment(num_clients // 3,
                                           DEFAULT_DURATION,
                                           DEFAULT_RW_RATIO,
                                           DEFAULT_ZIPF,
                                           DEFAULT_WARMUP,
                                           DEFAULT_K,
                                           'random',
                                           DEFAULT_REPLICAS,
                                           initialize=False,
                                           test_crash=False)
            except TimeoutError:
                print('Experiment timed out')
                continue

            with open(f'roram_{num_clients}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()


def cockroach_throughput_latency():
    # start cockroach
    session = InstanceSession(COCKROACH_TYPES)
    session.start_instances()

    # initialize
    cockroach_experiment(100,
                         DEFAULT_DURATION,
                         DEFAULT_RW_RATIO,
                         DEFAULT_ZIPF,
                         DEFAULT_WARMUP,
                         DEFAULT_REPLICAS,
                         initialize=True,
                         test_crash=False)

    for i in range(NUM_DATA_POINTS):
        for num_clients in NUM_CLIENTS:
            try:
                # start experiment
                results = cockroach_experiment(num_clients // 3,
                                               DEFAULT_DURATION,
                                               DEFAULT_RW_RATIO,
                                               DEFAULT_ZIPF,
                                               DEFAULT_WARMUP,
                                               DEFAULT_REPLICAS,
                                               initialize=False,
                                               test_crash=False)
            except TimeoutError:
                print('Experiment timed out')
                continue

            with open(f'cockroach_{num_clients}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()


def uoram_throughput_latency():
    # start uoram
    session = InstanceSession(UORAM_TYPES)
    session.start_instances()
    for i in range(NUM_DATA_POINTS):
        for num_clients in NUM_CLIENTS:
            try:
                # initialize
                uoram_experiment(num_clients // 3,
                                 DEFAULT_DURATION,
                                 DEFAULT_RW_RATIO,
                                 DEFAULT_ZIPF,
                                 DEFAULT_WARMUP,
                                 DEFAULT_K,
                                 initialize=True)

                # start experiment
                results = uoram_experiment(num_clients // 3,
                                           DEFAULT_DURATION,
                                           DEFAULT_RW_RATIO,
                                           DEFAULT_ZIPF,
                                           DEFAULT_WARMUP,
                                           DEFAULT_K,
                                           initialize=False)
            except TimeoutError:
                print('Experiment timed out')
                continue

            with open(f'uoram_{num_clients}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()


def lynch_throughput_latency():
    # start lynch
    session = InstanceSession(LYNCH_TYPES)
    session.start_instances()
    for i in range(NUM_DATA_POINTS):
        for num_clients in NUM_CLIENTS:
            try:
                # initialize
                lynch_experiment(num_clients // 3,
                                 DEFAULT_DURATION,
                                 DEFAULT_RW_RATIO,
                                 DEFAULT_ZIPF,
                                 DEFAULT_WARMUP,
                                 initialize=True)

                # start experiment
                results = lynch_experiment(num_clients // 3,
                                           DEFAULT_DURATION,
                                           DEFAULT_RW_RATIO,
                                           DEFAULT_ZIPF,
                                           DEFAULT_WARMUP,
                                           initialize=False)
            except TimeoutError:
                print('Experiment timed out')
                continue

            with open(f'lynch_{num_clients}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()


if __name__ == '__main__':
    instance_names = INSTANCE_TYPES.keys()
    if (len(sys.argv) < 2 or sys.argv[1] not in instance_names):
        print(f'Usage: {sys.argv[0]} {"|".join(instance_names)}')
        sys.exit(1)

    if sys.argv[1] == 'roram':
        roram_throughput_latency()
    elif sys.argv[1] == 'uoram':
        uoram_throughput_latency()
    elif sys.argv[1] == 'cockroach':
        cockroach_throughput_latency()
    elif sys.argv[1] == 'lynch':
        lynch_throughput_latency()
