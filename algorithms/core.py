from monitor import Monitor
from datasets import gen_datasets, MAX_DB_SIZE
from helpers import make_fig, save_summary
import time
import numpy as np

FIG_FOR = [100, 1000, 100000, MAX_DB_SIZE]


def core(alg, alg_name):
    cpu_values = []
    mem_values = []
    learning_times = []
    interfere_times = []
    mean_learning_ram_usage = []
    mean_learning_cpu_usage = []
    mean_interfere_ram_usage = []
    mean_interfere_cpu_usage = []


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
        try:
            alg.predict(X)
        except AttributeError:
            alg.fit_predict(X)
        t2 = time.time()
        time.sleep(5)
        monitor.run.__globals__['START'] = False
        if len(X) in FIG_FOR:
            make_fig(mem,cpu,t1,t2,f"{alg_name}_interfere_stage_for_{len(X)}_samples",f"{alg_name}")
        interfere_times.append(t2 - t1)
        mean_interfere_cpu_usage.append(np.mean(cpu))
        mean_interfere_ram_usage.append(np.mean(mem))
        cpu.clear()
        mem.clear()
    monitor.run.__globals__['STOP'] = True
    print("STOP")

    save_summary(mean_learning_ram_usage, mean_learning_cpu_usage, learning_times,["RAM","CPU","TIME"], f"{alg_name}_learnging_summary",f"{alg_name}")
    save_summary(mean_interfere_ram_usage, mean_interfere_cpu_usage, interfere_times,["RAM","CPU","TIME"], f"{alg_name}_interfere_summary",f"{alg_name}")
