from experiments import (roram_experiment, DEFAULT_CLIENTS, DEFAULT_DURATION,
                         DEFAULT_RW_RATIO, DEFAULT_ZIPF, DEFAULT_WARMUP,
                         DEFAULT_REPLICAS)

from instance_session import InstanceSession, RORAM_TYPES

NUM_DATA_POINTS = 3
K_VALUES = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

if __name__ == '__main__':
    # start roram
    session = InstanceSession(RORAM_TYPES)
    session.start_instances()

    i = 0
    while i < NUM_DATA_POINTS:
        j = 7
        while j < len(K_VALUES):
            k = K_VALUES[j]
            # initialize
            roram_experiment(DEFAULT_CLIENTS,
                             DEFAULT_DURATION,
                             DEFAULT_RW_RATIO,
                             DEFAULT_ZIPF,
                             DEFAULT_WARMUP,
                             k,
                             'random',
                             DEFAULT_REPLICAS,
                             initialize=True,
                             test_crash=False)
            # start experiment
            results = roram_experiment(DEFAULT_CLIENTS,
                                       DEFAULT_DURATION,
                                       DEFAULT_RW_RATIO,
                                       DEFAULT_ZIPF,
                                       DEFAULT_WARMUP,
                                       k,
                                       'random',
                                       DEFAULT_REPLICAS,
                                       initialize=False,
                                       test_crash=False)

            with open(f'k_{k}_{i}.log', 'w') as f:
                f.write(results)

    # stop roram
    session.stop_instances()
