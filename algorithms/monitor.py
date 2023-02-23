import time
import psutil
import matplotlib.pyplot as plt
import datetime 

cpu_values = []
mem_values = []

class Monitor:
    def __init__(self, cpu_probe=1, mem_probe=1):
        self.spu_probe = cpu_probe
        self.mem_probe = mem_probe
    def cpu_monitor():
        pass
    def mem_monitor():
        pass
    def show_results():
        pass

def display_usage(cpu_percent, mem_usage, bars=50):
    cpu_per = (cpu_percent/100)
    cpu_bar = '❚' * int(cpu_per*bars) + '-' * (bars - int(cpu_per *bars))
    mem_per = (mem_usage/100)
    mem_bar = '❚' * int(mem_per*bars) + '-' * (bars - int(mem_per *bars))

    print(f"\rCPU |{cpu_bar}| {cpu_percent:.2f}%  ", end='')
    print(f"RAM |{mem_bar}| {mem_usage:.2f}%  ", end='\r')

while True:
    try:
        cpu_percent = psutil.cpu_percent()
        mem_usage = psutil.virtual_memory().percent
        cpu_values.append(cpu_percent)
        mem_values.append(mem_usage)
        display_usage(cpu_percent, mem_usage, 30)
        time.sleep(0.5)
    except KeyboardInterrupt:
        print("shuting down")
        plt.plot(cpu_values)
        plt.plot(mem_values)
        plt.legend(["CPU", "RAM"])
        # plt.show()
        plt.savefig(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
        break