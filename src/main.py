import mmap
import math
import os
from time import perf_counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

def round_to_infinity(x):
    return math.ceil(x * 10) / 10

def merge_dicts(target, source):
    """
    Merge source dictionary into target. Each dictionary maps keys to [min, max, count, sum].
    """
    for key, rec in source.items():
        if key in target:
            t_rec = target[key]
            t_rec[0] = min(t_rec[0], rec[0])
            t_rec[1] = max(t_rec[1], rec[1])
            t_rec[2] += rec[2]
            t_rec[3] += rec[3]
        else:
            target[key] = rec[:]  # copy the list
    return target

def chunked_reader(filename, chunk_size=100000):
    """
    Generator that memory-maps the file in binary mode and yields chunks of lines.
    Each chunk is a list of byte strings.
    """
    with open(filename, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            chunk = []
            line = mm.readline()
            while line:
                # Remove trailing newline(s)
                chunk.append(line.rstrip(b'\n'))
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
                line = mm.readline()
            if chunk:
                yield chunk

def process_subchunk(lines):
    """
    Process a small sub-chunk (list of byte strings) and return a dict mapping keys to
    [min, max, count, sum].
    """
    local_dic = {}
    for line in lines:
        if not line:
            continue
        # Partition at first occurrence of b';'
        key, sep, val_bytes = line.partition(b';')
        val = float(val_bytes)
        if key in local_dic:
            rec = local_dic[key]
            rec[0] = min(val, rec[0])
            rec[1] = max(val, rec[1])
            rec[2] += 1
            rec[3] += val
        else:
            local_dic[key] = [val, val, 1, val]
    return local_dic

def process_chunk_threaded(lines, thread_workers=8):
    """
    Within a process, split the list of lines into sub-chunks and use threads to process them concurrently.
    """
    subchunk_size = len(lines) // thread_workers + 1
    subchunks = [lines[i:i + subchunk_size] for i in range(0, len(lines), subchunk_size)]
    merged_local = {}
    with ThreadPoolExecutor(max_workers=thread_workers) as executor:
        futures = [executor.submit(process_subchunk, sc) for sc in subchunks]
        for future in as_completed(futures):
            partial = future.result()
            merge_dicts(merged_local, partial)
    return merged_local

def write_output(merged, output_file):
    if os.path.exists("output.txt"):
        os.remove("output.txt")
    with open(output_file, "a+") as f:
        for key in sorted(merged.keys(), key=lambda k: k.decode('utf-8')):
            rec = merged[key]
            mean_val = rec[3] / rec[2]
            f.write(f"{key.decode('utf-8')}={round_to_infinity(rec[0])}/"
                    f"{round_to_infinity(mean_val)}/"
                    f"{round_to_infinity(rec[1])}\n")

def main(input_file="testcase.txt", output_file="output.txt", proc_workers=None, chunk_size=100000, thread_workers=16):
    if proc_workers is None:
        proc_workers = os.cpu_count() or 4

    start_total = perf_counter()
    merged_results = {}

    # fucking nested threading to fuck/ this skibidi
    with ProcessPoolExecutor(max_workers=proc_workers) as proc_executor:
        futures = []
        for chunk in chunked_reader(input_file, chunk_size):
            # Each process uses threads to further parallelize its chunk.
            futures.append(proc_executor.submit(process_chunk_threaded, chunk, thread_workers))
        
        for future in as_completed(futures):
            partial_result = future.result()
            merge_dicts(merged_results, partial_result)

    write_output(merged_results, output_file)
    print("Total processing time:", perf_counter() - start_total)

if __name__ == '__main__':
    main()
