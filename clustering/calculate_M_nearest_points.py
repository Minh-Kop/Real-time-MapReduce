import pandas as pd
import os


def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


def M_nearest_points_pandas(input_path, M, output_path):
    if (M == 0):
        return

    df = pd.read_csv(create_path(input_path), sep='\t', names=['user', 'distance'],
                     dtype={'user': "Int64", "distance": 'Float64'})
    df = df.sort_values(["distance"], ascending=False)
    df = df.iloc[:M]
    df.to_csv(create_path(output_path), sep='\t', index=False, header=False)
