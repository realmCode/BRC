import os
import mmap
import math
import concurrent.futures


# colonal : divinde and rule
def process_chunk(filename, start, end):
    local_stats = {}
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        filesize = mm.size()

        # If not at the beginning, adjust start to the next newline.
        if start != 0:
            newline_pos = mm.find(b'\n', start, end)
            if newline_pos == -1:
                mm.close()
                return local_stats  # no complete line in this segment
            start = newline_pos + 1

        # If not at the end of file, adjust end to the last newline.
        if end < filesize:
            last_newline = mm.rfind(b'\n', start, end)
            if last_newline != -1:
                end = last_newline

        # Decode the chunk once.
        chunk = mm[start:end].decode('utf-8', errors='replace')
        mm.close()

    for line in chunk.splitlines():
        if not line:
            continue
        
        city, sep, num_str = line.partition(';')
        value = float(num_str)
        if city in local_stats:
            curr_min, curr_max, curr_sum, curr_count = local_stats[city]
            local_stats[city] = (value if value < curr_min else curr_min,
                                 value if value > curr_max else curr_max,
                                 curr_sum + value,
                                 curr_count + 1)
        else:
            local_stats[city] = (value, value, value, 1)
    return local_stats

def merge_dicts(dicts):
    """
    (min, max, sum, count).
    """
    global_stats = {}
    for d in dicts:
        for city, (lmin, lmax, lsum, lcount) in d.items():
            if city in global_stats:
                gmin, gmax, gsum, gcount = global_stats[city]
                global_stats[city] = (min(gmin, lmin),
                                      max(gmax, lmax),
                                      gsum + lsum,
                                      gcount + lcount)
            else:
                global_stats[city] = (lmin, lmax, lsum, lcount)
    return global_stats

def round_to_infinity(x):
    return math.ceil(x * 10) / 10

def main():
    input_filename = "testcase.txt"
    output_filename = "output.txt"
    
    filesize = os.path.getsize(input_filename)
    num_workers = os.cpu_count() or 4
    chunk_size = filesize // num_workers

    # Compute chunk offsets.
    # list comphrehesion is father of for loop
    offsets = [( i * chunk_size, (i + 1) * chunk_size if i < num_workers - 1 else filesize) for i in range(num_workers)]

    
    # CORPORATE RULE: if one woman can give birth in 9 months, then 9 woman can give birth in 1 month
    # multi fucking skibidi
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_chunk, input_filename, start, end)for start, end in offsets]
        global_stats = merge_dicts( [future.result() for future in concurrent.futures.as_completed(futures)])

    # out_lines = []
    # for city in sorted(global_stats.keys()):
    #     gmin, gmax, gsum, gcount = global_stats[city]
    #     mean = gsum / gcount
    #     # out_line = f"{city}={round_to_infinity(gmin):.1f}/{round_to_infinity(mean):.1f}/{round_to_infinity(gmax):.1f}"
    #     # out_lines.append(out_line)
    #     # let not store in var
    #     out_lines.append(f"{city}={round_to_infinity(gmin):.1f}/{round_to_infinity(mean):.1f}/{round_to_infinity(gmax):.1f}")

    #again fucking list comphrehension
    if os.path.exists("output.txt"):
        os.remove("output.txt")
    with open(output_filename, "a+") as outf:
        for city in sorted(global_stats.keys()):
            gmin, gmax, gsum, gcount = global_stats[city]
            outf.write(f"{city}={round_to_infinity(gmin):.1f}/{round_to_infinity(gsum/gcount):.1f}/{round_to_infinity(gmax):.1f}\n")


if __name__ == "__main__":
    # from time import perf_counter
    # a = perf_counter()
    # print("started")
    main()
    # print(perf_counter()-a)