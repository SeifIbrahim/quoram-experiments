import sys
import glob
import re
from collections import defaultdict

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f'Usage: {sys.argv[0]} <file_prefix>')

    file_list = glob.glob(f'{sys.argv[1]}*.log')

    latencies = defaultdict(dict)
    throughputs = defaultdict(dict)

    for file_name in file_list:
        _, config, run = re.match(r'(\w+)_(\d+)_(\d+).log', file_name).groups()
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
    for run in throughputs.values():
        print(','.join(str(kv[1]) for kv in sorted(run.items())))

    print('Latencies:')
    for run in latencies.values():
        print(','.join(str(kv[1]) for kv in sorted(run.items())))
