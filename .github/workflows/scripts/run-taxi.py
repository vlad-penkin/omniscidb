#!/usr/bin/python3

import subprocess
import statistics, json, os.path
import matplotlib.pyplot as plt

NUM_RUNS = 3
NUM_TEST = 4
JSON_NAME = 'perf.json'


def do_runs():
    time = [[] for _ in range(NUM_TEST)]
    cpu = [[] for _ in range(NUM_TEST)]

    for i in range(NUM_RUNS):
        cmd = ['build/bin/taxi_reduced', '--data', 'data']
        r = subprocess.run(cmd, stdout = subprocess.PIPE)
        lines = r.stdout.decode('utf-8').splitlines()

        for j in range(NUM_TEST):
            r = lines[j - NUM_TEST].split()
            time[j].append(int(r[1]))
            cpu[j].append(int(r[3]))

    time = [statistics.median(r) for r in time]
    cpu = [statistics.median(r) for r in cpu]
    return time, cpu


def load_json():
    if not os.path.exists(JSON_NAME): return [[], []]

    with open(JSON_NAME) as f:
        data = json.load(f)

    return data


def draw_graphs(name, data):
    for i in range(NUM_TEST):
        d = [x[i] for x in data]
        n = name + str(i)
        plt.plot(d)
        plt.title(n)
        plt.xticks([])
        plt.savefig(n + '.pdf')
        plt.close()


def process_runs(perf_json, time, cpu):
    perf_json[0].append(time)
    perf_json[1].append(cpu)

    with open(JSON_NAME, 'w') as f:
        json.dump(perf_json, f)
    return perf_json


def draw_all(perf_json):
    draw_graphs('time', perf_json[0])
    draw_graphs('cpu', perf_json[1])


time, cpu = do_runs()
perf_json = load_json()
perf_json = process_runs(perf_json, time, cpu)
draw_all(perf_json)

