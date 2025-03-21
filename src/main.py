import mmap
from time import perf_counter
import math
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count 

# threading lock takes fucking more time, breaking problem into subproblmem

# lets divide and rule like colonal

def round_to_infinity(x):
    return math.ceil(x * 10) / 10

def read(filename: str):
    with open(filename, "rb") as file:
        file_map = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        file_data = bytes(file_map)
        file_map.close()
    return file_data

def process_chunk(lines):
    local_dic = {}
    for line in lines:
        arg = line.split(";")
        key = arg[0]
        val = float(arg[1])
        if key in local_dic:
            local_dic[key]['min'] = min(local_dic[key]['min'], val)
            local_dic[key]['max'] = max(local_dic[key]['max'], val)
            local_dic[key]['count'] += 1
            local_dic[key]['mean'] += (val - local_dic[key]['mean']) / local_dic[key]['count']
        else:
            local_dic[key] = {
                'min': val,
                'max': val,
                'count': 1,
                'mean': val
            }
    return local_dic

def merge_dicts(dict_list):
    merged = {}
    for d in dict_list:
        for key, val in d.items():
            if key in merged:
                temp = merged[key]
                temp['min'] = min(temp['min'], val['min'])
                temp['max'] = max(temp['max'], val['max'])
                total_count = temp['count'] + val['count']
                temp['mean'] = (temp['mean'] * temp['count'] + val['mean'] * val['count']) / total_count
                temp['count'] = total_count
            else:
                merged[key] = val.copy()
    return merged

def write_output(dic, output_file):
    with open(output_file, "w") as f:
        for key in sorted(dic.keys()):
            temp = dic[key]
            f.write(f"{key}={round_to_infinity(temp['min'])}/{round_to_infinity(temp['mean'])}/{round_to_infinity(temp['max'])}\n")

def main(input_file_name: str = "testcase.txt", output_file_name: str = "output.txt", workers: int = 8):
    lines = read(input_file_name).decode().strip().splitlines() # mmap we read in binary, encoding to make it fuck
    chunk_size = len(lines) // workers + 1
    chunks = [lines[i:i+chunk_size] for i in range(0, len(lines), chunk_size)]
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        dic_arrays = list(executor.map(process_chunk, chunks))
    
    final_dic = merge_dicts(dic_arrays)
    write_output(final_dic, output_file_name)

if __name__ == "__main__":
    a = perf_counter()
    main()
    print(perf_counter() - a)
