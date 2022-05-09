import sys
from experiments import (cockroach_experiment, roram_experiment,
                         uoram_experiment, lynch_experiment, DEFAULT_DURATION,
                         DEFAULT_RW_RATIO, DEFAULT_ZIPF, DEFAULT_WARMUP,
                         DEFAULT_K, DEFAULT_REPLICAS)

from instance_session import INSTANCE_TYPES

if __name__ == '__main__':
    instance_types = INSTANCE_TYPES.keys()
    if ((len(sys.argv) < 4) or (sys.argv[1] not in instance_types)
            or (sys.argv[2] not in ['init', 'cont'])):
        print(f'Usage: {sys.argv[0]} {"|".join(instance_types)} \
                    init|cont num_clients')
        sys.exit(1)

    if sys.argv[1] == 'cockroach':
        cockroach_experiment(int(sys.argv[3]), DEFAULT_DURATION,
                             DEFAULT_RW_RATIO, DEFAULT_ZIPF, DEFAULT_WARMUP,
                             DEFAULT_REPLICAS, sys.argv[2] == 'init', False)
    elif sys.argv[1] == 'roram':
        roram_experiment(int(sys.argv[3]), DEFAULT_DURATION, DEFAULT_RW_RATIO,
                         DEFAULT_ZIPF, DEFAULT_WARMUP, DEFAULT_K, 'random',
                         DEFAULT_REPLICAS, sys.argv[2] == 'init', False)
    elif sys.argv[1] == 'uoram':
        uoram_experiment(int(sys.argv[3]), DEFAULT_DURATION, DEFAULT_RW_RATIO,
                         DEFAULT_ZIPF, DEFAULT_WARMUP, DEFAULT_K,
                         sys.argv[2] == 'init')
    elif sys.argv[1] == 'lynch':
        lynch_experiment(int(sys.argv[3]), DEFAULT_DURATION, DEFAULT_RW_RATIO,
                         DEFAULT_ZIPF, DEFAULT_WARMUP, sys.argv[2] == 'init')
    else:
        print(f'Unknown experiment: {sys.argv[1]}')
