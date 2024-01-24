import numpy as np

from calculate_distance_between_users_centroid import DistanceBetweenUsersCentroid

if __name__ == '__main__':
    # Calculate M nearest points
    mr_job = DistanceBetweenUsersCentroid(args=[
        './output/user_item_matrix.txt',
        '--init-centroid', './output/first_centroid.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/D.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()
