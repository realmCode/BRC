import mmap
from time import perf_counter
# from statistics import mean
import math

def round_to_infinity(x):
    return math.ceil(x * 10) / 10
dic:dict = {}
output_arr:list = []


def read(filename: str):
    with open(filename, "rb") as file:
        file_descriptor = file.fileno()
        file_map = mmap.mmap(file_descriptor, 0, access=mmap.ACCESS_READ)
        file_data = bytes(file_map)
        file_map.close()

    return file_data


def processline(line: str) -> None:
    global dic
    arg = line.split(";")
    val = float(arg[1])
    if dic.get(arg[0]):
        dic[arg[0]]['min'] = min(dic[arg[0]]['min'], val)
        dic[arg[0]]['max'] = max(dic[arg[0]]['max'], val)
        dic[arg[0]]['count']+=1
        dic[arg[0]]['mean'] += (val - dic[arg[0]]['mean']) / dic[arg[0]]['count']
    else:
        dic[arg[0]] = {
            'min': val,
            'max': val,
            'count': 1,
            'mean': val
        }
    return


def processline1(key, val) -> str:
    global output_arr
    output_arr.append(f"{key}={round_to_infinity(val['min'])}/{round_to_infinity(val['mean'])}/{round_to_infinity(val['max'])}\n")
    return

def sort_arr() -> None:
    global dic
    dic = dict(sorted(dic.items()))
def main(input_file_name: str = "testcase.txt", output_file_name: str = "output.txt"):

    a = perf_counter()
    print("timer started")
    input_dat = read(input_file_name).decode().strip()
    for x in input_dat.splitlines():
        processline(x)
    sort_arr()
    for x,y in dic.items():
        processline1(x,y)
        
    with open(output_file_name, 'w') as f:
        f.write("".join(output_arr))
    return
if __name__ == "__main__":
    a = perf_counter()
    main()
    print(perf_counter()-a)
