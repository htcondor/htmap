import htmap


@htmap.mapped
def smoke(x):
    return str(x)


map = smoke.map('smoke', range(10))
