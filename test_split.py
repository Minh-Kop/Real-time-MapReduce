from split_input import SplitInput
import os
import time
import numpy as np


def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


def run_mr_job(mr_job_class, input_args):
    mr_job = mr_job_class(args=input_args)
    with mr_job.make_runner() as runner:
        runner.run()
        data = []
        for key, value in mr_job.parse_output(runner.cat_output()):
            data.append(f'{key}\t{value}')
        return data


def write_data_to_file(filename, data, mode='w'):
    output_file = open(filename, mode)
    for el in data:
        output_file.writelines(el)
    output_file.close()


if __name__ == '__main__':
    start_mr = time.time()

    centoids = []
    with open(create_path('./clustering/output/centroids.txt')) as file:
        for line in file:
            id, _ = line.strip().split('\t')
            centoids.append(id)

    for index, id in enumerate(centoids):
        result_data = run_mr_job(SplitInput, [create_path(
            './input_file.txt'), create_path('./clustering/output/labels.txt'), '--cid', id])
        write_data_to_file(f'test/output_file{index}.txt', result_data)

    end_mr = time.time()

    print("time: " + str(end_mr - start_mr))
