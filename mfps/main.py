import os

from mfps.create_combinations import create_combinations
from mfps.rating_commodity import rating_commodity
from mfps.rating_usefulness import rating_usefulness
from mfps.rating_details import rating_details
from mfps.rating_time import rating_time
from mfps.calculate_mfps import CalculateMFPS


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


def run_mfps(input_path, user_path):
    # Create combinations
    result_data = run_mr_job(create_combinations, [
                             create_path(create_path(input_path))])
    write_data_to_file(
        (create_path('./output/create_combinations.txt')), result_data)

    # Calculate rating commodity
    result_data = run_mr_job(rating_commodity, ['--users-path', create_path(user_path),
                                                create_path(input_path)])
    write_data_to_file(create_path(
        './output/rating_commodity.txt'), result_data)

    # Calculate rating usefulness
    result_data = run_mr_job(
        rating_usefulness, [create_path(input_path),
                            '--rating-commodity-path', create_path('./output/rating_commodity.txt')])
    write_data_to_file(create_path(
        './output/rating_usefulness.txt'), result_data)

    # Calculate rating details
    result_data = run_mr_job(rating_details, ['--combinations-path', create_path('./output/create_combinations.txt'),
                                              create_path('./output/rating_usefulness.txt')])
    write_data_to_file(create_path('./output/rating_details.txt'), result_data)

    # Calculate rating time
    result_data = run_mr_job(
        rating_time, [create_path('./output/create_combinations.txt')])
    write_data_to_file(create_path('./output/rating_time.txt'), result_data)

    # Calculate MFPS
    result_data = run_mr_job(CalculateMFPS, [create_path('./output/rating_commodity.txt'),
                                             create_path(
                                                 './output/rating_usefulness.txt'),
                                             create_path(
                                                 './output/rating_details.txt'),
                                             create_path('./output/rating_time.txt')])
    write_data_to_file(create_path('./output/mfps.txt'), result_data)
