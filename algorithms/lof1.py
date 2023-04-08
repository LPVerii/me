# Author: Alexandre Gramfort <alexandre.gramfort@inria.fr>
#         Albert Thomas <albert.thomas@telecom-paristech.fr>
# License: BSD 3 clause
# https://scikit-learn.org/stable/auto_examples/miscellaneous/plot_anomaly_comparison.html#sphx-glr-auto-examples-miscellaneous-plot-anomaly-comparison-py

import time
import threading
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import psutil
import datetime
# from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from datasets import gen_datasets, MAX_DB_SIZE
from monitor import Monitor
from helpers import make_fig, save_summary


# matplotlib.rcParams["contour.negative_linestyle"] = "solid"

alg = LocalOutlierFactor(novelty=True)
alg_name = 'LocalOutlierFactor'

START = False
RESET = False
STOP = False
cpu_values = []
mem_values = []
learning_times = []
interfere_times = []
mean_learning_ram_usage = []
mean_learning_cpu_usage = []
mean_interfere_ram_usage = []
mean_interfere_cpu_usage = []
#MAX_DB_SIZE = 10000000
FIG_FOR = [100, 1000, 100000, MAX_DB_SIZE]
# config = {
#     'cpu_values': cpu_values,
#     'mem_values': mem_values
# }
# class Monitor(threading.Thread):
    
#     def __init__(self):
#         threading.Thread.__init__(self)

#     def run(self):
#         global START
#         global RESET
#         global cpu_values
#         global mem_values
#         while True:
#             if START:
#                 cpu_percent = psutil.cpu_percent()
#                 mem_usage = psutil.virtual_memory().percent
#                 cpu_values.append(cpu_percent)
#                 mem_values.append(mem_usage)
#                 # display_usage(cpu_percent, mem_usage, 30)
#                 time.sleep(0.5)
#             if STOP:
#                 break
          

# def gen_datasets():
#     n = 10
#     while n<=MAX_DB_SIZE:
#         yield np.random.rand(n, 2)
#         n*=10
    

monitor = Monitor()
# monitor.run.__globals__.update(config)
monitor.start()
time.sleep(5)
mem = monitor.run.__globals__['mem_values']
cpu = monitor.run.__globals__['cpu_values']
for X in gen_datasets():
    print(len(X))
    
    monitor.run.__globals__['START'] = True
    time.sleep(5)
    t1 = time.time()
    alg.fit(X)
    t2 = time.time()
    time.sleep(5)
    monitor.run.__globals__['START'] = False
    # print(f"po pierwszym runie {mem}")
    if len(X) in FIG_FOR:
        make_fig(mem,cpu,t1,t2,f"{alg_name}_learn_stage_for_{len(X)}_samples",f"{alg_name}")
    learning_times.append(t2 - t1)
    mean_learning_cpu_usage.append(np.mean(cpu))
    mean_learning_ram_usage.append(np.mean(mem))
    cpu.clear()
    mem.clear()

    monitor.run.__globals__['START'] = True
    time.sleep(5)
    t1 = time.time()
    y_pred = alg.predict(X)
    t2 = time.time()
    time.sleep(5)
    monitor.run.__globals__['START'] = False
    if len(X) in FIG_FOR:
        make_fig(mem,cpu,t1,t2,f"{alg_name}_interfere_stage_for_{len(X)}_samples",f"{alg_name}")
    print("upload global lists")
    interfere_times.append(t2 - t1)
    mean_interfere_cpu_usage.append(np.mean(cpu))
    mean_interfere_ram_usage.append(np.mean(mem))
    cpu.clear()
    mem.clear()
monitor.run.__globals__['STOP'] = True
print("STOP")

save_summary(mean_learning_ram_usage, mean_learning_cpu_usage, learning_times,["RAM","CPU","TIME"], f"{alg_name}_learnging_summary",f"{alg_name}")
save_summary(mean_interfere_ram_usage, mean_interfere_cpu_usage, interfere_times,["RAM","CPU","TIME"], f"{alg_name}_interfere_summary",f"{alg_name}")



