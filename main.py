import os
import pandas as pd

# from clustering.main import run_clustering
from clustering import run_clustering


def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


def create_input_file(input_path, output_path):
    df = pd.read_csv(input_path, sep='\t',
                     names=['user_id', 'item_id', 'rating', 'timestamp'], dtype='str')
    input_df = pd.DataFrame({'key': df['user_id'] + ';' + df['item_id'],
                             'value': df['rating'] + ';' + df['timestamp']})
    input_df.to_csv(create_path(output_path),
                    sep='\t', index=False, header=False)


def create_users_items_file(input_path):
    input_df = pd.read_csv(input_path, sep="\t", names=["key", "value"])
    input_df = input_df['key'].str.split(';', expand=True)

    users = input_df[0].astype('int64').drop_duplicates().sort_values()
    items = input_df[1].astype('int64').drop_duplicates().sort_values()

    users.to_csv(create_path(
        './input/users.txt'), index=False, header=False)
    items.to_csv(create_path(
        './input/items.txt'), index=False, header=False)


def split_files_by_label(input_file_path):
    input_file = pd.read_csv(input_file_path, sep='\t',
                             dtype='str', names=['key', 'value'])
    input_file[['user', 'item']] = input_file['key'].str.split(
        ';', expand=True)

    labels = pd.read_csv('./clustering/output/labels.txt',
                         sep='\t', dtype='str', names=['user', 'label'])
    labels['label'] = labels['label'].str.split('|', expand=True)[0]

    joined_df = pd.merge(input_file, labels, on="user").drop(columns='user')
    joined_df = joined_df.set_index('label')
    print(joined_df)

    avg_ratings = pd.read_csv('./clustering/output/avg_ratings.txt',
                              sep='\t', dtype='str', names=['user', 'avg_rating'])
    avg_ratings = avg_ratings.merge(labels, on='user').set_index('label')

    centroids = pd.read_csv('./clustering/output/centroids.txt',
                            sep='\t', dtype='str', names=['key'], usecols=[0])
    for index, value in enumerate(centroids['key']):
        input_file_i = joined_df.loc[[value]]

        # Export items
        input_file_i['item'].drop_duplicates().to_csv((f'./input/items_{index}.txt'),
                                                      sep='\t', index=False, header=False)

        # Export input file
        input_file_i.drop(columns='item').to_csv((f'./input/input_file_{index}.txt'),
                                                 sep='\t', index=False, header=False)

        avg_ratings_i = avg_ratings.loc[[value]]

        # Export average ratings
        avg_ratings_i.to_csv((f'./input/avg_ratings_{index}.txt'),
                             sep='\t', index=False, header=False)

        # Export users
        avg_ratings_i.iloc[:, 0].to_csv(
            (f'./input/users_{index}.txt'), index=False, header=False)


if __name__ == '__main__':
    source_file_path = "./input/u.data"
    input_file_path = './input/input_file copy.txt'
    # create_input_file(input_path=source_file_path, output_path=input_file_path)
    create_users_items_file(input_file_path)

    # Clustering
    run_clustering(create_path(input_file_path), 3)

    # Split input file
    split_files_by_label(input_file_path)
