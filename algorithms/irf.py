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
from sklearn.ensemble import IsolationForest

# matplotlib.rcParams["contour.negative_linestyle"] = "solid"

irf = IsolationForest()

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
MAX_DB_SIZE = 10000000
FIG_FOR = [100, 1000, 100000, MAX_DB_SIZE]

class Monitor(threading.Thread):
    
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        global START
        global RESET
        global cpu_values
        global mem_values
        while True:
            if START:
                cpu_percent = psutil.cpu_percent()
                mem_usage = psutil.virtual_memory().percent
                cpu_values.append(cpu_percent)
                mem_values.append(mem_usage)
                # display_usage(cpu_percent, mem_usage, 30)
                time.sleep(0.5)
            if STOP:
                break
            
def make_fig(x1, x2, t1,t2,title, folder="."):
        if len(x1)> 1000000:
            x1 = x1[::3]
            x2 = x2[::3]
        fig = plt.figure()
        fig, (ax1, ax2) = plt.subplots(2, 1)
        ax1.plot(x1, color='blue')
        ax1.plot(x2, color='green')
        ax1.legend(["RAM", "CPU"])
        ax1.set_title(title)
        ax1.set_xlabel(f"Time (taken time {t2-t1:.2f} secs)")
        ax1.set_ylabel("%   ", rotation=0)
        fig.subplots_adjust(hspace=0.5)

        ax2.set_title("Usage")
        ax2.set_ylabel("%   ", rotation=0)
        ax2.bar([1,2,3], [max(x1), np.mean(x1), min(x1)], color='blue')
        ax2.bar([4,5,6],[max(x2), np.mean(x2), min(x2)], color='green')
        ax2.set_xticks([i for i in range(1,7)])
        ax2.set_xticklabels(["max", "mean", "min","max", "mean", "min"], rotation=65)
        
        plt.savefig(f"{folder}/{title}")

def gen_datasets():
    n = 10
    while n<=MAX_DB_SIZE:
        yield np.random.rand(n, 2)
        n*=10
    

monitor = Monitor()
monitor.start()
time.sleep(5)
for X in gen_datasets():
    print(len(X))

    START = True
    time.sleep(5)
    t1 = time.time()
    irf.fit(X)
    t2 = time.time()
    time.sleep(5)
    START = False
    if len(X) in FIG_FOR:
        make_fig(mem_values,cpu_values,t1,t2,f"irf_learn_stage_for_{len(X)}_samples","irf")
    learning_times.append(t2 - t1)
    mean_learning_cpu_usage.append(np.mean(cpu_values))
    mean_learning_ram_usage.append(np.mean(mem_values))
    cpu_values = []
    mem_values = []
    

    START = True
    time.sleep(5)
    t1 = time.time()
    y_pred = irf.fit_predict(X)
    t2 = time.time()
    time.sleep(5)
    START = False
    if len(X) in FIG_FOR:
        make_fig(mem_values,cpu_values,t1,t2,f"irf_interfere_stage_for_{len(X)}_samples","irf")
    print("upload global lists")
    interfere_times.append(t2 - t1)
    mean_interfere_cpu_usage.append(np.mean(cpu_values))
    mean_interfere_ram_usage.append(np.mean(mem_values))
    cpu_values = []
    mem_values = []
STOP = True

def save_summary(x1,x2,x3,legend,title,folder):
    plt.figure()
    plt.title(title)
    plt.plot(x1)
    plt.plot(x2)
    plt.plot(x3)
    plt.legend(legend)
    plt.savefig(f"{folder}/{title}")

save_summary(mean_learning_ram_usage, mean_learning_cpu_usage, learning_times,["RAM","CPU","TIME"], "IRF_learnging_summary","irf")
save_summary(mean_interfere_ram_usage, mean_interfere_cpu_usage, interfere_times,["RAM","CPU","TIME"], "IRF_interfere_summary","irf")



