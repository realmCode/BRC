import math
import os
from time import perf_counter
from concurrent.futures import ProcessPoolExecutor, as_completed
import mmap

def round_to_infinity(x):
    return math.ceil(x * 10) / 10

def process_chunk(lines):
    """
    Processes a list of lines (each a bytestring) and returns a dictionary mapping keys to
    [min, max, count, sum] for the associated values.
    """
    local_dic = {}
    for line in lines:
        if not line:  # skip empty lines
            continue
        # Partition the line at the first occurrence of b';'
        key, sep, val_bytes = line.partition(b';')
        try:
            val = float(val_bytes)
        except ValueError:
            continue  # skip if the value cannot be parsed
        if key in local_dic:
            rec = local_dic[key]
            rec[0] = min(rec[0], val)  # update min
            rec[1] = max(rec[1], val)  # update max
            rec[2] += 1              # increment count
            rec[3] += val            # add to sum
        else:
            local_dic[key] = [val, val, 1, val]
    return local_dic

def merge_dicts(target, source):
    """
    Merge the source dictionary into the target dictionary.
    Each dictionary maps keys to [min, max, count, sum].
    """
    for key, rec in source.items():
        if key in target:
            m = target[key]
            m[0] = min(m[0], rec[0])
            m[1] = max(m[1], rec[1])
            m[2] += rec[2]
            m[3] += rec[3]
        else:
            target[key] = rec
    return target

def chunked_reader(filename, chunk_size=100000):
    with open(filename, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            chunk = []
            line = mm.readline()
            while line:
                # Remove trailing newline characters
                chunk.append(line.rstrip(b'\n'))
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
                line = mm.readline()
            if chunk:
                yield chunk

def write_output(merged, output_file):
    """
    Writes the final merged results to the output file.
    The keys (stored as bytes) are decoded to UTF-8.
    """
    with open(output_file, "w") as f:
        for key in sorted(merged.keys(), key=lambda k: k.decode('utf-8')):
            rec = merged[key]
            mean_val = rec[3] / rec[2]
            f.write(f"{key.decode('utf-8')}={round_to_infinity(rec[0])}/"
                    f"{round_to_infinity(mean_val)}/"
                    f"{round_to_infinity(rec[1])}\n")

def main(input_file="testcase.txt", output_file="output.txt", workers=None, chunk_size=4**6):
    if workers is None:
        workers = os.cpu_count() or 4

    start_total = perf_counter()
    merged_results = {}

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = []
        for chunk in chunked_reader(input_file, chunk_size):
            futures.append(executor.submit(process_chunk, chunk))
        
        for future in as_completed(futures):
            partial_result = future.result()
            merge_dicts(merged_results, partial_result)

    write_output(merged_results, output_file)
    print("Total processing time:", perf_counter() - start_total)

if __name__ == '__main__':
    main()
