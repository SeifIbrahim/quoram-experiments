import sys
import glob
import re
from collections import defaultdict
import os

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print(f'Usage: {sys.argv[0]} <file_prefix> <directory> <y/n>')
        sys.exit(1)

    try:
        os.chdir(sys.argv[2])
    except OSError as e:
        print(e)
        print(f'Usage: {sys.argv[0]} <file_prefix> <directory>')
        sys.exit(1)

    file_list = glob.glob(f'{sys.argv[1]}*.log')

    latencies = defaultdict(dict)
    throughputs = defaultdict(dict)

    for file_name in file_list:
        name_match = re.match(
            r'(\w+)_(-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)_(\d+).log',
            file_name).groups()
        config = float(name_match[1])
        run = int(name_match[2])
        with open(file_name, 'r') as f:
            total_latency = 0
            total_throughput = 0
            for line in f:
                latency_match = re.search(
                    r'Average response time was (\d+.\d+) ms', line)
                if latency_match:
                    total_latency += float(latency_match.group(1))

                throughput_match = re.search(r'Thr?oughput: (\d+.\d+)', line)
                if throughput_match:
                    total_throughput += float(throughput_match.group(1))

            latencies[run][config] = total_latency / 3
            throughputs[run][config] = total_throughput

    print('Throughputs:')
    for i, run in throughputs.items():
        print(f'Run {i}')
        print(','.join(
            str(kv[1] if sys.argv[3] == 'n' else kv)
            for kv in sorted(run.items())))

    print('Latencies:')
    for i, run in latencies.items():
        print(f'Run {i}')
        print(','.join(
            str(kv[1] if sys.argv[3] == 'n' else kv)
            for kv in sorted(run.items())))
