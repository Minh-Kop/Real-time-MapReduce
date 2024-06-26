import time

from jaccard_hadoop import run_jaccard
from custom_util import (
    env_dict,
    get_txt_filename,
    put_files_to_hdfs,
)

HADOOP_PATH = env_dict["hadoop_path"]

if __name__ == "__main__":
    ## Start timer
    start = time.time()

    input_file_path = "./input/input_file_test.txt"
    hdfs_input_file_path = f"{HADOOP_PATH}/input/{get_txt_filename(input_file_path)}"

    ## Put input file to HDFS
    put_files_to_hdfs(input_file_path, hdfs_input_file_path)

    ## Run Jaccard
    run_jaccard(hdfs_input_file_path, output_file_path="hadoop_output/jaccard.txt")

    ## End timer
    end = time.time()
    print(f"MapReduce Jaccard run time: {end - start}s")
