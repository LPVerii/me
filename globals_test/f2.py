from f1 import func, G

lis1 = []
# global G
# G = "f2"
print(G)
print(f"func {func.__globals__['G']}")
G="asdsad"
print(G)
print(f"func {func.__globals__['G']}")


print("Cccccccc")
NEW_GLOBAL = "this is override globalasdasdasds"
config={'G': NEW_GLOBAL}
func.__globals__.update(config)
config = {'lis':lis1}
func.__globals__.update(config)
print(G)

if __name__=="__main__":
    # from f1 import G
    print(G)
    func()
    print(lis1)
    lis1.clear()
    print(G)
    NEW_GLOBAL = "overide again"
    print(G)
    func()
    print(G)
    print(lis1)