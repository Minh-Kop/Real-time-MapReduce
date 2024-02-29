import os
import pandas as pd

# from clustering.main import run_clustering
from clustering import run_clustering


def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


if __name__ == '__main__':
    # Create input file
    df = pd.read_csv("./test/u.data", sep='\t',
                     names=['user_id', 'item_id', 'rating', 'timestamp'], dtype='str')
    input_file = pd.DataFrame({'key': df['user_id'] + ';' + df['item_id'],
                               'value': df['rating'] + ';' + df['timestamp']})
    input_file.to_csv(create_path('./input/input_file.txt'),
                      sep='\t', index=False, header=False)

    # Create users, items files
    input_file = input_file['key'].str.split(';', expand=True)
    users = input_file[0]
    items = input_file[1]

    users.drop_duplicates().to_csv(create_path(
        './input/users.txt'), index=False, header=False)
    items.drop_duplicates().to_csv(create_path(
        './input/items.txt'), index=False, header=False)

    # Clustering
    run_clustering(6)

    # Split input file
    df = pd.read_csv('./input/input_file.txt', sep='\t',
                     dtype='str', names=['key', 'value'])
    df['user'] = df['key'].str.split(';', expand=True)[0]

    labels = pd.read_csv('./clustering/output/labels.txt',
                         sep='\t', dtype='str', names=['user', 'label'])

    r = pd.merge(df, labels, on="user").drop(columns='user')
    r = r.set_index('label')

    centroids = pd.read_csv('./clustering/output/centroids.txt',
                            sep='\t', dtype='str', names=['key'], usecols=[0])
    print(centroids)
    for index, value in enumerate(centroids['key']):
        result = r.loc[value]
        result.to_csv((f'./input/input_file_{index}.txt'),
                      sep='\t', index=False, header=False)
