from pathlib import Path
import time

import htmap


@htmap.mapped
def counter(num_steps):
    checkpoint_path = Path('checkpoint')
    try:
        step = int(checkpoint_path.read_text())
        print('loaded checkpoint!')
    except FileNotFoundError:
        step = 0
        print('starting from scratch')

    while True:
        time.sleep(1)
        step += 1
        print(f'completed step {step}')

        if step >= num_steps:
            break

        checkpoint_path.write_text(str(step))
        htmap.checkpoint(checkpoint_path)

    return True


map = counter.map('chk', [30])

while map.component_statuses[0] is not htmap.ComponentStatus.RUNNING:
    print(map.component_statuses[0])
    time.sleep(1)

print('component has started, letting it run...')
time.sleep(10)
map.vacate()
print('vacated map')

while map.component_statuses[0] is not htmap.ComponentStatus.COMPLETED:
    print(map.component_statuses[0])
    time.sleep(1)

print(map[0])
print(map.stdout(0))
