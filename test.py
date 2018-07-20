from htcmap import htcmap
import sys


@htcmap
def double(x):
    return 2 * x


print(double)
inputs = list((i,) for i in range(int(sys.argv[1]), int(sys.argv[2])))
j = double.map(inputs)
print(j)
for i, result in zip(inputs, j):
    print(f'{i} -> {result}')
