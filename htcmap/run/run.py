import os
import sys
import socket
import datetime
from pathlib import Path

import cloudpickle

print(f'Landed on execute node {socket.getfqdn()} ({socket.gethostbyname(socket.gethostname())}) at {datetime.datetime.utcnow()}.')
print(f'Execute node operating system: {os.uname()}')

dir_contents = ", ".join(str(x) for x in Path.cwd().iterdir())
print(f'Local directory contents: {dir_contents}')

with open('fn.pkl', mode = 'rb') as file:
    fn = cloudpickle.load(file)

with open(f'{sys.argv[1]}.in', mode = 'rb') as file:
    args, kwargs = cloudpickle.load(file)

output = fn(*args, **kwargs)
print(output)

with open(f'{sys.argv[1]}.out', mode = 'wb') as file:
    cloudpickle.dump(output, file)
