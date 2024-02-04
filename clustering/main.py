import numpy as np

from create_item_list import ItemList
from calculate_avg_rating import AvgRating
from create_user_item_matrix import UserItemMatrix
from create_importance import Importance
from get_max import GetMax
from create_centroid import CreateCentroid
from calculate_distance_between_users_centroid import DistanceBetweenUsersCentroid
from calculate_M_nearest_points import MNearestPoints
from discard_nearest_points import DiscardNearestPoints
from calculate_scaling import Scaling
from calculate_sum_F_D import SumFD
from update_centroids import UpdateCentroids
from label import Label

number_of_clusters = 3


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
    # # Create item list
    # result_data = run_mr_job(ItemList, ['../input_file.txt'])
    # write_data_to_file('./output/items.txt', result_data)

    # Calculate average rating
    result_data = run_mr_job(AvgRating, ['../input_file.txt'])
    write_data_to_file('./output/avg_ratings.txt', result_data)

    # Create user-item matrix
    result_data = run_mr_job(UserItemMatrix, ['../input_file.txt',
                                              './output/avg_ratings.txt',
                                              '--items-path', './output/items.txt'])
    write_data_to_file('./output/user_item_matrix.txt', result_data)
    write_data_to_file('../user_item_matrix.txt', result_data)

    # Calculate importance
    result_data = run_mr_job(Importance, ['../input_file.txt'])
    write_data_to_file('./output/F.txt', result_data)

    # Find most importance
    result_data = run_mr_job(GetMax, ['./output/F.txt'])
    write_data_to_file('./output/max_F.txt', result_data)

    # Create first centroid
    result_data = run_mr_job(CreateCentroid, ['./output/user_item_matrix.txt',
                                              './output/max_F.txt'])
    write_data_to_file('./output/centroids.txt', result_data)

    # Calculate number of discarded points
    users_file = open('../users.txt', 'r')
    for number_of_users, line in enumerate(users_file, start=1):
        pass
    users_file.close()

    M = int(number_of_users/4/1.5) + 1

    # Calculate distance between users and first centroid
    result_data = run_mr_job(DistanceBetweenUsersCentroid, ['./output/user_item_matrix.txt',
                                                            '--first-centroid-path', './output/centroids.txt'])
    write_data_to_file('./output/D.txt', result_data)

    # Calculate M nearest points
    result_data = run_mr_job(MNearestPoints, ['./output/D.txt',
                                              '--m', str(M)])
    write_data_to_file('./output/M_nearest_points.txt', result_data)

    # Discard nearest points in user-item matrix
    result_data = run_mr_job(DiscardNearestPoints, ['./output/user_item_matrix.txt',
                                                    './output/M_nearest_points.txt'])
    write_data_to_file('./output/user_item_matrix.txt', result_data)

    # Discard nearest points in F
    result_data = run_mr_job(DiscardNearestPoints, ['./output/F.txt',
                                                    './output/M_nearest_points.txt'])
    write_data_to_file('./output/F.txt', result_data)

    # Loop
    for i in range(number_of_clusters - 1):
        print(i)

        # Calculate distance between users and centroids
        result_data = run_mr_job(DistanceBetweenUsersCentroid, ['./output/user_item_matrix.txt',
                                                                '--first-centroid-path', './output/centroids.txt'])
        write_data_to_file('./output/D.txt', result_data)

        # Get max F
        result_data = run_mr_job(GetMax, ['./output/F.txt'])
        write_data_to_file('./output/max_F.txt', result_data)

        # Scaling F
        result_data = run_mr_job(
            Scaling, ['./output/F.txt', '--max-value-path', './output/max_F.txt'])
        write_data_to_file('./output/F.txt', result_data)

        # Get max min_D
        result_data = run_mr_job(GetMax, ['./output/D.txt'])
        write_data_to_file('./output/max_D.txt', result_data)

        # Scaling D
        result_data = run_mr_job(Scaling, ['./output/D.txt',
                                           '--max-value-path', './output/max_D.txt'])
        write_data_to_file('./output/D.txt', result_data)

        # Calculate sum F, D
        result_data = run_mr_job(SumFD, ['./output/F.txt', './output/D.txt'])
        write_data_to_file('./output/F_D.txt', result_data)

        # Calculate max F_D
        result_data = run_mr_job(
            GetMax, ['./output/F_D.txt'])
        write_data_to_file('./output/max_F_D.txt', result_data)

        # Create another centroid
        result_data = run_mr_job(CreateCentroid, ['./output/user_item_matrix.txt',
                                                  './output/max_F_D.txt'])
        write_data_to_file('./output/new_centroid.txt', result_data)
        write_data_to_file('./output/centroids.txt', result_data, mode='a')

        # Calculate distance between new centroid and other users
        result_data = run_mr_job(DistanceBetweenUsersCentroid, ['./output/user_item_matrix.txt',
                                                                '--first-centroid-path', './output/new_centroid.txt'])
        write_data_to_file('./output/D_.txt', result_data)

        # Calculate M nearest points
        result_data = run_mr_job(MNearestPoints, ['./output/D_.txt',
                                                  '--m', str(M)])
        write_data_to_file('./output/M_nearest_points.txt', result_data)

        # Discard nearest points in user-item matrix
        result_data = run_mr_job(DiscardNearestPoints, ['./output/user_item_matrix.txt',
                                                        './output/M_nearest_points.txt'])
        if result_data == []:
            print('Break')
            break
        write_data_to_file('./output/user_item_matrix.txt', result_data)

        # Discard nearest points in F
        result_data = run_mr_job(DiscardNearestPoints, ['./output/F.txt',
                                                        './output/M_nearest_points.txt'])
        write_data_to_file('./output/F.txt', result_data)

    # KMeans
    count = 1
    while True:
        print(f'\nLoop {count}')
        count += 1

        # Calculate distance between users and centroids
        result_data = run_mr_job(DistanceBetweenUsersCentroid, ['../user_item_matrix.txt',
                                                                '--first-centroid-path', './output/centroids.txt',
                                                                '--return-centroid-id', 'True'])
        write_data_to_file('./output/user_item_matrix.txt', result_data)

        # Update centroids
        result_data = run_mr_job(UpdateCentroids, [
                                 './output/user_item_matrix.txt'])
        write_data_to_file('./output/new_centroids.txt', result_data)

        # Check if has converged
        with open('./output/new_centroids.txt', 'r') as new_centroids, open('./output/centroids.txt', 'r') as old_centroids:
            for line in new_centroids:
                key, value = line.strip().split('\t')
                new_centroids_tuples = [tuple(value.strip().split('|'))]
            for line in old_centroids:
                key, value = line.strip().split('\t')
                old_centroids_tuples = [tuple(value.strip().split('|'))]

            if set(new_centroids_tuples) == set(old_centroids_tuples):
                break

        # Save new centroids to file
        with open('./output/new_centroids.txt', 'r') as new_centroids, open('./output/centroids.txt', 'w') as old_centroids:
            for line in new_centroids:
                old_centroids.write(line)

    # Assign labels
    result_data = run_mr_job(Label, ['./output/user_item_matrix.txt'])
    write_data_to_file('./output/labels.txt', result_data)
