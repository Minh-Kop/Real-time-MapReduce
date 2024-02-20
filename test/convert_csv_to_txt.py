import os
import pandas as pd


def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


df = pd.read_csv(create_path('u.csv'), sep=';')
new_df = pd.DataFrame({'key': df['user_id'].astype(str) + ';' + df['item_id'].astype(str),
                       'value': df['rating'].astype(str) + ';' + df['timestamp'].astype(str)})
new_df.to_csv(create_path('u.txt'), sep='\t', index=False, header=False)
