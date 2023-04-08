from core import core
from sklearn.neighbors import LocalOutlierFactor
from pyod.models.loda import LODA
from sklearn.cluster import Birch, DBSCAN, SpectralClustering
from sklearn.ensemble import IsolationForest
from pyclustering.cluster.clique import clique # 3 init

algs = [
    (Birch(), 'birtch'),
    # (SpectralClustering(), 'spectral'),
    (DBSCAN(), 'dbscan'),
    (LODA(), 'loda')
]

for alg, alg_name in algs:
    core(alg, alg_name)
