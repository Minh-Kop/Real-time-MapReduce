import numpy as np

from create_item_list import ItemList
from calculate_avg_rating import AvgRating
from create_user_item_matrix import UserItemMatrix
from create_importance import Importance
from get_max import GetMax
from create_first_centroid import FirstCentroid
from calculate_distance_between_users_centroid import DistanceBetweenUsersCentroid
from calculate_M_nearest_points import MNearestPoints
from discard_nearest_points import DiscardNearestPoints
from calculate_scaling import Scaling

number_of_clusters = 3

if __name__ == '__main__':
    # Create item list
    mr_job = ItemList(args=['../input_file.txt'])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/items.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Calculate average rating
    mr_job = AvgRating(args=['../input_file.txt'])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/avg_ratings.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Create user-item matrix
    mr_job = UserItemMatrix(args=[
        '../input_file.txt',
        '--items-path', './output/items.txt',
        '--avg-ratings-path', './output/avg_ratings.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/user_item_matrix.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Calculate importance
    mr_job = Importance(args=[
        '../input_file.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/F.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Find most importance
    mr_job = GetMax(args=[
        './output/F.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/max_F.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Create first centroid
    mr_job = FirstCentroid(args=[
        './output/user_item_matrix.txt',
        './output/max_F.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/centroids.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Calculate distance between users and first centroid
    mr_job = DistanceBetweenUsersCentroid(args=[
        './output/user_item_matrix.txt',
        '--first-centroid-path', './output/centroids.txt'
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/D.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Calculate number of discard points
    users_file = open('../users.txt', 'r')
    for number_of_users, line in enumerate(users_file, start=1):
        pass
    users_file.close()

    M = int(number_of_users/4/1.5) + 1

    output_file = open('./output/number_of_discard_points.txt', 'w')
    output_file.writelines(f'{M}')
    output_file.close()

    # Calculate M nearest points
    mr_job = MNearestPoints(args=[
        './output/D.txt',
        '--m-path', './output/number_of_discard_points.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/M_nearest_points.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Discard nearest points in user-item matrix
    mr_job = DiscardNearestPoints(args=[
        './output/user_item_matrix.txt',
        '--nearest-points-path', './output/M_nearest_points.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/user_item_matrix.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Discard nearest points in F
    mr_job = DiscardNearestPoints(args=[
        './output/F.txt',
        '--nearest-points-path', './output/M_nearest_points.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/F.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    for i in range(number_of_clusters - 1):
        print(i)

        # Calculate distance between users and centroids
        mr_job = DistanceBetweenUsersCentroid(args=[
            './output/user_item_matrix.txt',
            '--first-centroid-path', './output/centroids.txt'
        ])
        with mr_job.make_runner() as runner:
            runner.run()
            output_file = open('./output/D.txt', 'w')
            for key, value in mr_job.parse_output(runner.cat_output()):
                output_file.writelines(f'{key}\t{value}')
            output_file.close()

        # Get max F
        mr_job = GetMax(args=[
            './output/F.txt',
        ])
        with mr_job.make_runner() as runner:
            runner.run()
            output_file = open('./output/max_F.txt', 'w')
            for key, value in mr_job.parse_output(runner.cat_output()):
                output_file.writelines(f'{key}\t{value}')
            output_file.close()

        # Scaling F
        mr_job = Scaling(args=[
            './output/F.txt',
            '--max-value-path', './output/max_F.txt',
        ])
        with mr_job.make_runner() as runner:
            runner.run()
            output_file = open('./output/F.txt', 'w')
            for key, value in mr_job.parse_output(runner.cat_output()):
                output_file.writelines(f'{key}\t{value}')
            output_file.close()

        # Get max min_D
        mr_job = GetMax(args=[
            './output/D.txt',
        ])
        with mr_job.make_runner() as runner:
            runner.run()
            output_file = open('./output/max_D.txt', 'w')
            for key, value in mr_job.parse_output(runner.cat_output()):
                output_file.writelines(f'{key}\t{value}')
            output_file.close()

        # Scaling min_D
        mr_job = Scaling(args=[
            './output/D.txt',
            '--max-value-path', './output/max_D.txt',
        ])
        with mr_job.make_runner() as runner:
            runner.run()
            output_file = open('./output/D.txt', 'w')
            for key, value in mr_job.parse_output(runner.cat_output()):
                output_file.writelines(f'{key}\t{value}')
            output_file.close()
