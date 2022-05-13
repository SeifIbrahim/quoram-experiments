from experiments import (roram_experiment, DEFAULT_CLIENTS, DEFAULT_DURATION,
                         DEFAULT_RW_RATIO, DEFAULT_K, DEFAULT_WARMUP,
                         DEFAULT_REPLICAS)

from instance_session import InstanceSession, RORAM_TYPES

NUM_DATA_POINTS = 3
ZIPF_VALUES = [0.00001, 0.1, 0.3, 0.5, 0.7, 0.9]

if __name__ == '__main__':
    # start roram
    session = InstanceSession(RORAM_TYPES)
    session.start_instances()

    for i in range(NUM_DATA_POINTS):
        for z in ZIPF_VALUES:
            # initialize
            roram_experiment(DEFAULT_CLIENTS,
                             DEFAULT_DURATION,
                             DEFAULT_RW_RATIO,
                             z,
                             DEFAULT_WARMUP,
                             DEFAULT_K,
                             'random',
                             DEFAULT_REPLICAS,
                             initialize=True,
                             test_crash=False)
            # start experiment
            results = roram_experiment(DEFAULT_CLIENTS,
                                       DEFAULT_DURATION,
                                       DEFAULT_RW_RATIO,
                                       z,
                                       DEFAULT_WARMUP,
                                       DEFAULT_K,
                                       'random',
                                       DEFAULT_REPLICAS,
                                       initialize=False,
                                       test_crash=False)

            with open(f'zipf_{z}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()
