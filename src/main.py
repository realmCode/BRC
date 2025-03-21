import mmap
from time import perf_counter
import math
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count

def round_to_infinity(x):
    return math.ceil(x * 10) / 10

def read(filename: str):
    with open(filename, "rb") as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as file_map:
            file_data = file_map.read()
    return file_data

def process_chunk(lines):
    local_dic = {}
    for line in lines:
        parts = line.split(";")
        key = parts[0]
        val = float(parts[1])
        if key in local_dic:
            rec = local_dic[key]
            rec['min'] = min(rec['min'], val)
            rec['max'] = max(rec['max'], val)
            rec['count'] += 1
            rec['sum'] += val
        else:
            local_dic[key] = {'min': val, 'max': val, 'count': 1, 'sum': val}
    return local_dic

def merge_dicts(dict_list):
    merged = {}
    for d in dict_list:
        for key, rec in d.items():
            if key in merged:
                m = merged[key]
                m['min'] = min(m['min'], rec['min'])
                m['max'] = max(m['max'], rec['max'])
                m['count'] += rec['count']
                m['sum'] += rec['sum']
            else:
                merged[key] = rec.copy()
    return merged

def write_output(dic, output_file):
    with open(output_file, "w") as f:
        for key in sorted(dic.keys()):
            rec = dic[key]
            mean_val = rec['sum'] / rec['count']
            f.write(f"{key}={round_to_infinity(rec['min'])}/{round_to_infinity(mean_val)}/{round_to_infinity(rec['max'])}\n")

def main(input_file="testcase.txt", output_file="output.txt", workers=8):
    data = read(input_file).decode().strip().splitlines()
    #lets fuckinh  Divide the file into chunks for each process like britisher
    chunk_size = len(data) // workers + 1
    chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        partial_results = list(executor.map(process_chunk, chunks))
    
    final_dic = merge_dicts(partial_results)
    write_output(final_dic, output_file)

if __name__ == "__main__":
    a = perf_counter()
    main()
    print(perf_counter() - a)
