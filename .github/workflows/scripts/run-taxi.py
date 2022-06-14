#!/usr/bin/python3

import subprocess

cmd = ['build/bin/taxi_reduced', '--data', 'data']
result = subprocess.run(cmd, stdout = subprocess.PIPE)
print(result.stdout.decode('utf-8'))

