import threading 

G = "f1"
lis = []

class T(threading.Thread):
    def __init__(self):
        print("Monitor init")
        threading.Thread.__init__(self)
    def run():
        global lis
        lis.append(G)


if __name__=="__main__":
    func()
    print(func.__globals__)
    print(lis)