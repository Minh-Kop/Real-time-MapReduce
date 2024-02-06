import os
import pandas as pd


def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


read_file = pd.read_csv(create_path("u.data"), sep='\t')
read_file.to_csv(create_path("u.csv"), sep=';', index=None, header=[
                 'user_id', 'item_id', 'rating', 'timestamp'])
