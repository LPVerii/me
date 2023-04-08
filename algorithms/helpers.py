from matplotlib import pyplot as plt
import numpy as np

def save_summary(x1,x2,x3,legend,title,folder):
    plt.figure()
    plt.title(title)
    plt.plot(x1)
    plt.plot(x2)
    plt.plot(x3)
    plt.legend(legend)
    plt.savefig(f"{folder}/{title}")

      
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
