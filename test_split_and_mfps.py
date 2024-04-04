from split_input import SplitInput
import os
import time
import numpy as np
from mfps.main import run_mfps
from clustering.main import run_clustering


def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


def run_mr_job(mr_job_class, input_args):
    mr_job = mr_job_class(args=input_args)
    with mr_job.make_runner() as runner:
        runner.run()
        data = []
        for key, value in mr_job.parse_output(runner.cat_output()):
            data.append(f"{key}\t{value}")
        return data


def write_data_to_file(filename, data, mode="w"):
    output_file = open(filename, mode)
    for el in data:
        output_file.writelines(el)
    output_file.close()


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

    run_mfps(
        "hdfs://localhost:9000/user/mackop/input/input_file_copy.txt",
        "hdfs://localhost:9000/user/mackop/input/avg_ratings.txt",
        "./output/mfps.txt",
    )
