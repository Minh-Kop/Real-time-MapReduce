import os
import pandas as pd


def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


df = pd.read_csv(create_path('u.txt'), sep='\t',
                 header=None, names=['key', 'value'], dtype='str')
key = df['key'].str.split(';')

users = pd.Series([row[0] for row in key],
                  dtype='Int64').drop_duplicates()
items = pd.Series([row[1] for row in key],
                  dtype='Int64').drop_duplicates()

items.to_csv(create_path('../items.txt'), index=False, header=False)
users.to_csv(create_path('../users.txt'), index=False, header=False)
