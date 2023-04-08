from core import core
from sklearn.neighbors import LocalOutlierFactor

core(LocalOutlierFactor(novelty=True), "lofv2")
