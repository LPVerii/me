import time
import psutil
import matplotlib.pyplot as plt
import datetime 
import threading

cpu_values = []
mem_values = []
START = None
STOP = None
RESET = None

# def display_usage(cpu_percent, mem_usage, bars=50):
#     cpu_per = (cpu_percent/100)
#     cpu_bar = '❚' * int(cpu_per*bars) + '-' * (bars - int(cpu_per *bars))
#     mem_per = (mem_usage/100)
#     mem_bar = '❚' * int(mem_per*bars) + '-' * (bars - int(mem_per *bars))

#     print(f"\rCPU |{cpu_bar}| {cpu_percent:.2f}%  ", end='')
#     print(f"RAM |{mem_bar}| {mem_usage:.2f}%  ", end='\r')


class Monitor(threading.Thread):
    
    def __init__(self):
        print("Monitor init")
        threading.Thread.__init__(self)

    def run(self):
        print("Monitor run")
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
                time.sleep(0.5)
            if STOP:
                break