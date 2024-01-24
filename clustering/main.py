import numpy as np

from create_item_list import ItemList
from calculate_avg_rating import AvgRating
from create_user_item_matrix import UserItemMatrix
from create_importance import Importance
from find_most_importance import MostImportance
from create_first_centroid import FirstCentroid
from calculate_M_nearest_points import MNearestPoints

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
        output_file = open('./output/importances.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Find most importance
    mr_job = MostImportance(args=[
        './output/importances.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/most_importance.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
        output_file.close()

    # Calculate number of discard points
    users_file = open('../users.txt', 'r')
    for number_of_users, line in enumerate(users_file, start=1):
        pass
    users_file.close()

    M = int(number_of_users/4/1.5)

    output_file = open('./output/number_of_discard_points.txt', 'w')
    output_file.writelines(f'{M}')
    output_file.close()

    # Create first centroid
    mr_job = FirstCentroid(args=[
        './output/user_item_matrix.txt',
        './output/most_importance.txt',
    ])
    with mr_job.make_runner() as runner:
        runner.run()
        output_file = open('./output/first_centroid.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            output_file.writelines(f'{key}\t{value}')
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
