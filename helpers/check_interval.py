import sys

def main():
    counter = 0
    n = float(sys.stdin.readline())
    m = float(sys.stdin.readlmeine())
    # print(f"{n} - {m}")
    highest = m-n
    while m: 
        # counter += 1
        if m < n:
            raise Exception("Values not sorted")     
        n = m
        try:
            m = float(sys.stdin.readline()) 
        except ValueError as e:
            print(f"Interrupted by: {e} {sys.stdin.readline()}")
            if sys.stdin.readline() is None:
                print("None value")
            if sys.stdin.readline()=='':
                print("None string")
            break
        # print(f"{m} - {n} = {m-n}")
        tmp = m-n
        # print(f"{tmp}")
        if highest < tmp:
            highest = tmp
            print(f"{tmp}")
        # print(f"counter {counter}")
    print(f"Highest interval: {highest} secs")

if __name__=='__main__':
    main()
    