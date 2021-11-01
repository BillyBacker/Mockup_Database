import multiprocessing
import os

def test(n, a, Bin):
    Bin = n**2+a
    os.mkdir(f"{n}")

if __name__ == '__main__':
    n = [i for i in range(2)]
    Bin = [0 for i in range(2)]
    threads = []
    for i in range(len(n)):
        threads.append(multiprocessing.Process(target=test, args=(n[i],Bin[i])))
        threads[-1].start()
    for thread in threads:
        thread.join()
    print(Bin)