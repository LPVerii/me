import numpy as np
# from sklearn.datasets import make_moons, make_blobs

MAX_DB_SIZE = 10000000

def gen_datasets():
    n = 10
    while n<=MAX_DB_SIZE:
        yield np.random.rand(n, 2)
        n*=10
