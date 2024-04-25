import os
import sys
import time

import numpy as np

sys.path.append(os.path.abspath("./util"))

from custom_util import run_mr_job, write_data_to_file, env_dict
from split_input import SplitInput

# from selectKBest_chi2.main_hadoop import run_clustering_chi2
from selectKBest_chi2 import run_clustering_chi2

HADOOP_PATH = env_dict["hadoop_path"]


if __name__ == "__main__":
    # run_clustering(6)

    # Split input using mapreduce
    # centroids = []
    # with open(create_path('./clustering/output/centroids.txt')) as file:
    #     for line in file:
    #         id, _ = line.strip().split('\t')
    #         centroids.append(id)

    # for index, id in enumerate(centroids):
    #     result_data = run_mr_job(SplitInput, [create_path(
    #         './input/input_file.txt'), create_path('./clustering/output/user_item_matrix.txt'), '--cid', id])
    #     write_data_to_file(f'test/output/output_file{index}.txt', result_data)

    # # Split new input into new user
    # for index, cluster in enumerate(centroids):
    #     start_mr = time.time()

    #     with open(create_path(f'./test/output/output_file_{index}.txt'), 'r') as fread, open(create_path(f'./test/user_split/user_{index}.txt'), 'a') as fwrite:
    #         L = []
    #         for line in fread:
    #             user, _, _ = line.strip().split(';')
    #             new_line = user + '\n'

    #             if new_line not in L:
    #                 L.append(user+'\n')

    #         fwrite.writelines(L)

    #     # run mfps
    #     run_mfps(create_path(f'./test/output/output_file_{index}.txt'),
    #              create_path(f'./test/user_split/user_{index}.txt'), cluster)

    #     end_mr = time.time()

    #     print("time: " + str(end_mr - start_mr))

    hdfs_input_file_path = f"{HADOOP_PATH}/input/input_file_copy.txt"
    run_clustering_chi2(hdfs_input_file_path)
