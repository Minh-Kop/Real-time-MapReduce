import os
import numpy as np

from clustering.calculate_avg_rating import AvgRating
from clustering.create_user_item_matrix import UserItemMatrix
from clustering.create_importance import Importance
# from clustering.get_max import GetMax
from clustering.create_centroid import CreateCentroid
from clustering.calculate_distance_between_users_centroid import DistanceBetweenUsersCentroid
from clustering.calculate_M_nearest_points import MNearestPoints
from clustering.discard_nearest_points import DiscardNearestPoints
from clustering.calculate_scaling import Scaling
from clustering.calculate_sum_F_D import SumFD
from clustering.update_centroids import UpdateCentroids
from clustering.label import Label

number_of_clusters = 3


def M_nearest_points(input_path, M, output_path):
    with open(create_path(input_path), 'r') as file:
        lines = file.readlines()

        pairs = []

        for line in lines:
            user, distance = map(float, line.strip().split('\t'))
            pairs.append([user, distance])

        data_arr = np.array(pairs)
        indices = np.argsort(data_arr[:, 1])
        sorted_data_arr = data_arr[indices]

        if (M == 0):
            return

        data_str = []
        M_nearest = sorted_data_arr[:M]
        for i in M_nearest:
            data_str.append(f"{i[0]}\t{i[1]}\n")

        write_data_to_file(create_path(output_path), data_str)


def get_max(input_path, output_path):
    with open(create_path(input_path), 'r') as file:
        lines = file.readlines()

        pairs = []

        for line in lines:
            a, b = map(str, line.strip().split('\t'))
            pairs.append([a, b])

        data_arr = np.array(pairs)

        index_of_max_b = np.argmax(data_arr[:, 1].astype(float))
        element_with_max_b = data_arr[index_of_max_b]

        write_data_to_file(create_path(output_path),
                           f"{element_with_max_b[0]}\t{element_with_max_b[1]}")


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


def run_clustering(number_of_clusters=3):
    # Calculate average rating
    result_data = run_mr_job(AvgRating, [create_path('../input/input_file.txt')])
    write_data_to_file(create_path('./output/avg_ratings.txt'), result_data)

    # Create user-item matrix
    result_data = run_mr_job(UserItemMatrix, [create_path('../input/input_file.txt'),
                                              create_path(
                                                  './output/avg_ratings.txt'),
                                              '--items-path', create_path('../input/items.txt')])
    write_data_to_file(create_path(
        './output/user_item_matrix.txt'), result_data)
    write_data_to_file(create_path('../input/user_item_matrix.txt'), result_data)

    # Calculate importance
    result_data = run_mr_job(Importance, [create_path('../input/input_file.txt')])
    write_data_to_file(create_path('./output/F.txt'), result_data)

    # # Find most importance
    # result_data = run_mr_job(GetMax, [create_path('./output/F.txt')])
    # write_data_to_file(create_path('./output/max_F.txt'), result_data)
    get_max('./output/F.txt', './output/max_F.txt')

    # Create first centroid
    result_data = run_mr_job(CreateCentroid, [create_path('./output/user_item_matrix.txt'),
                                              create_path('./output/max_F.txt')])
    write_data_to_file(create_path('./output/centroids.txt'), result_data)

    # Calculate number of discarded points
    users_file = open(create_path('../input/users.txt'), 'r')
    for number_of_users, line in enumerate(users_file, start=1):
        pass
    users_file.close()

    M = int(number_of_users / 4 / 1.5) + 1

    # Calculate distance between users and first centroid
    result_data = run_mr_job(DistanceBetweenUsersCentroid, [create_path('./output/user_item_matrix.txt'),
                                                            '--first-centroid-path', create_path('./output/centroids.txt')])
    write_data_to_file(create_path('./output/D.txt'), result_data)

    # Calculate M nearest points
    # result_data = run_mr_job(MNearestPoints, [create_path('./output/D.txt'),
    #                                           '--m', str(M)])
    # write_data_to_file(create_path(
    #     './output/M_nearest_points.txt'), result_data)
    M_nearest_points('./output/D.txt', M, './output/M_nearest_points.txt')

    # Discard nearest points in user-item matrix
    result_data = run_mr_job(DiscardNearestPoints, [create_path('./output/user_item_matrix.txt'),
                                                    create_path('./output/M_nearest_points.txt')])
    write_data_to_file(create_path(
        './output/user_item_matrix.txt'), result_data)

    # Discard nearest points in F
    result_data = run_mr_job(DiscardNearestPoints, [create_path('./output/F.txt'),
                                                    create_path('./output/M_nearest_points.txt')])
    write_data_to_file(create_path('./output/F.txt'), result_data)

    # Loop
    for i in range(number_of_clusters - 1):
        print(i)

        # Calculate distance between users and centroids
        result_data = run_mr_job(DistanceBetweenUsersCentroid, [create_path('./output/user_item_matrix.txt'),
                                                                '--first-centroid-path', create_path('./output/centroids.txt')])
        write_data_to_file(create_path('./output/D.txt'), result_data)

        # Get max F
        # result_data = run_mr_job(GetMax, [create_path('./output/F.txt')])
        # write_data_to_file(create_path('./output/max_F.txt'), result_data)
        get_max('./output/F.txt', './output/max_F.txt')

        # Scaling F
        result_data = run_mr_job(
            Scaling, [create_path('./output/F.txt'), '--max-value-path', create_path('./output/max_F.txt')])
        write_data_to_file(create_path('./output/F.txt'), result_data)

        # Get max min_D
        # result_data = run_mr_job(GetMax, [create_path('./output/D.txt')])
        # write_data_to_file(create_path('./output/max_D.txt'), result_data)
        get_max('./output/D.txt', './output/max_D.txt')

        # Scaling D
        result_data = run_mr_job(Scaling, [create_path('./output/D.txt'),
                                           '--max-value-path', create_path('./output/max_D.txt')])
        write_data_to_file(create_path('./output/D.txt'), result_data)

        # Calculate sum F, D
        result_data = run_mr_job(
            SumFD, [create_path('./output/F.txt'), create_path('./output/D.txt')])
        write_data_to_file(create_path('./output/F_D.txt'), result_data)

        # Calculate max F_D
        # result_data = run_mr_job(
        #     GetMax, [create_path('./output/F_D.txt')])
        # write_data_to_file(create_path('./output/max_F_D.txt'), result_data)
        get_max('./output/F_D.txt', './output/max_F_D.txt')

        # Create another centroid
        result_data = run_mr_job(CreateCentroid, [create_path('./output/user_item_matrix.txt'),
                                                  create_path('./output/max_F_D.txt')])
        write_data_to_file(create_path(
            './output/new_centroid.txt'), result_data)
        write_data_to_file(create_path(
            './output/centroids.txt'), result_data, mode='a')

        # Calculate distance between new centroid and other users
        result_data = run_mr_job(DistanceBetweenUsersCentroid, [create_path('./output/user_item_matrix.txt'),
                                                                '--first-centroid-path', create_path('./output/new_centroid.txt')])
        write_data_to_file(create_path('./output/D_.txt'), result_data)

        # Calculate M nearest points
        # result_data = run_mr_job(MNearestPoints, [create_path('./output/D_.txt'),
        #                                           '--m', str(M)])
        # write_data_to_file(create_path(
        #     './output/M_nearest_points.txt'), result_data)
        M_nearest_points('./output/D_.txt', M, './output/M_nearest_points.txt')

        # Discard nearest points in user-item matrix
        result_data = run_mr_job(DiscardNearestPoints, [create_path('./output/user_item_matrix.txt'),
                                                        create_path('./output/M_nearest_points.txt')])
        if result_data == []:
            print('Break')
            break
        write_data_to_file(create_path(
            './output/user_item_matrix.txt'), result_data)

        # Discard nearest points in F
        result_data = run_mr_job(DiscardNearestPoints, [create_path('./output/F.txt'),
                                                        create_path('./output/M_nearest_points.txt')])
        write_data_to_file(create_path('./output/F.txt'), result_data)

    # KMeans
    count = 1
    while True:
        print(f'\nLoop {count}')
        count += 1

        # Calculate distance between users and centroids
        result_data = run_mr_job(DistanceBetweenUsersCentroid, [create_path('../input/user_item_matrix.txt'),
                                                                '--first-centroid-path', create_path(
                                                                    './output/centroids.txt'),
                                                                '--return-centroid-id', 'True'])
        write_data_to_file(create_path(
            './output/user_item_matrix.txt'), result_data)

        # Update centroids
        result_data = run_mr_job(
            UpdateCentroids, [create_path('./output/user_item_matrix.txt')])
        write_data_to_file(create_path(
            './output/new_centroids.txt'), result_data)

        # Check if has converged
        with open(create_path('./output/new_centroids.txt'), 'r') as new_centroids, open(create_path('./output/centroids.txt'), 'r') as old_centroids:
            new_centroids_tuples = []
            old_centroids_tuples = []
            for line in new_centroids:
                key, value = line.strip().split('\t')
                new_centroids_tuples.append(tuple(value.strip().split('|')))
            for line in old_centroids:
                key, value = line.strip().split('\t')
                old_centroids_tuples.append(tuple(value.strip().split('|')))

            new_centroids_tuples = tuple(new_centroids_tuples)
            old_centroids_tuples = tuple(old_centroids_tuples)
            if set(new_centroids_tuples) == set(old_centroids_tuples):
                break

        # Save new centroids to file
        with open(create_path('./output/new_centroids.txt'), 'r') as new_centroids, open(create_path('./output/centroids.txt'), 'w') as old_centroids:
            for line in new_centroids:
                old_centroids.write(line)

    # Assign labels
    result_data = run_mr_job(
        Label, [create_path('./output/user_item_matrix.txt')])
    write_data_to_file(create_path('../output/labels.txt'), result_data)
