def double(x):
    return 2 * x


import htmap

result = htmap.map('dbl', double, range(10))

print(list(result))

print(htmap.map_ids())

print(htmap.status())

result = htmap.map('dbl', double, range(10))
