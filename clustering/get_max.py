import pandas as pd


def get_max(input_path, output_path):
    df = pd.read_csv(input_path, sep='\t', names=['user', 'distance'],
                     dtype={'user': "Int64", "distance": 'Float64'})
    max_idx = df["distance"].idxmax()
    max_row = df.loc[[max_idx]]
    max_row.to_csv(output_path, sep='\t', index=False, header=False)
