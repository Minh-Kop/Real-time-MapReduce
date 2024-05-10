import os
import pandas as pd
import math
pd.options.mode.chained_assignment = None  # default='warn'

def split_test_train(source_path, train_path, test_path, ratio=0.8):
    if(not os.path.exists(source_path)):
        return
    
    if(os.path.exists(test_path) or os.path.exists(train_path)):
        with open(train_path, "w"), open(test_path, "w"):
            pass

    with open(source_path, 'r') as src_file, open(test_path, "a") as test_file, open(train_path, "a") as train_file:
        lines = src_file.readlines()
        ratio_index = int(len(lines) * ratio)
        train_file.writelines(lines[:ratio_index])
        test_file.writelines(lines[ratio_index:])

def evaluate(sim_path, train_path, test_path, avg_path, k):
    # Read data from files
    train_df = pd.read_csv(train_path, delimiter='\t', names=['user_item', 'rating_time'])
    test_df = pd.read_csv(test_path, delimiter='\t', names=['user_item', 'rating_time'])
    sim_df = pd.read_csv(sim_path, delimiter='\t', names=['user_user', 'sim'])
    avg_df = pd.read_csv(avg_path, delimiter='\t', names=['user', 'avg'])
    avg_df['user'] = avg_df['user'].astype(str)

    # Split user_item and rating_time columns into separate columns
    train_df[['user', 'item']] = train_df['user_item'].str.split(';', expand=True)
    train_df[['rating', 'time']] = train_df['rating_time'].str.split(';', expand=True)

    test_df[['user', 'item']] = test_df['user_item'].str.split(';', expand=True)
    test_df[['rating', 'time']] = test_df['rating_time'].str.split(';', expand=True)

    sim_df[['curr_user', 'user']] = sim_df['user_user'].str.split(';', expand=True)

    # Drop unnecessary columns
    train_df = train_df.drop(['user_item', 'rating_time'], axis=1)
    test_df = test_df.drop(['user_item', 'rating_time', 'time'], axis=1)
    sim_df = sim_df.drop('user_user', axis=1)

    # Merge average ratings into train_df and user_item_list
    train_df = pd.merge(train_df, avg_df, on='user', how='inner')
    user_item_list = test_df.groupby('user')['item'].agg(list).reset_index()
    user_item_list = pd.merge(user_item_list, avg_df, on='user', how='inner')

    # Create a dictionary to store similarities
    sim_dict = {(u, v): s for u, v, s in zip(sim_df['curr_user'], sim_df['user'], sim_df['sim'])}

    # Create a dataFrame to sace predicted, observed value
    val_df = pd.DataFrame(columns=['user','item','pre_rating'])

    # Iterate over user_item_list
    for index, row in user_item_list.iterrows():
        u_user = row['user']
        items = row['item']
        avg = row['avg']

        user_ratings = train_df[train_df['item'].isin(items)]

        # Calculate similarities between the current user and all other users who have rated the items
        user_ratings.loc[:, 'similarity'] = user_ratings['user'].map(lambda v_user: sim_dict.get((u_user, v_user)))
        user_ratings['similarity'] = user_ratings['similarity'].astype(float)

        # Get the top k similar users
        top_k_similar_users = user_ratings.nlargest(k, 'similarity')

        # Calculate the weighted sum of ratings and similarities
        user_ratings.loc[:, 'weighted_sum'] = (user_ratings['rating'].astype(float) - user_ratings['avg'].astype(float)) * user_ratings['similarity'].astype(float)
        top_k_similar_users.loc[:, 'weighted_sum'] = (top_k_similar_users['rating'].astype(float) - top_k_similar_users['avg'].astype(float)) * top_k_similar_users['similarity'].astype(float)

        # Sum of similarities for normalization
        sum_df = user_ratings.groupby('item').agg({'similarity':'sum', 'weighted_sum':'sum'}).reset_index()
        sum_df['pre_rating'] = (avg + (sum_df['weighted_sum']/sum_df['similarity']))
        sum_df['user'] = u_user

        sum_df_top_k = top_k_similar_users.groupby('item').agg({'similarity':'sum', 'weighted_sum':'sum'}).reset_index()
        sum_df_top_k['pre_rating'] = (avg + (sum_df_top_k['weighted_sum']/sum_df_top_k['similarity']))
        sum_df_top_k['user'] = u_user
        
        val_df = pd.concat([val_df, sum_df]).drop(['similarity','weighted_sum'], axis=1)
        val_df_top_k = pd.concat([val_df, sum_df]).drop(['similarity','weighted_sum'], axis=1)
    
    val_df = pd.merge(val_df, test_df, on=['user','item'], how='left')
    val_df.to_csv('./output/pre_obs_val', sep=',', index=False)

    dif = (val_df['rating'].astype(float) - val_df['pre_rating'].astype(float))**2
    rmse = math.sqrt((dif/len(val_df)).sum())

    val_df = pd.merge(val_df, avg_df, on='user').astype(float)

    tp_df = val_df[val_df['pre_rating'] > val_df['avg']].astype(float).drop('rating', axis=1)
    tp_df = pd.merge(tp_df, test_df.astype(float), on=['user', 'item'])

    tp = len(tp_df[tp_df['rating'] > tp_df['avg']])
    test_df = pd.merge(test_df, avg_df, on='user')

    precision = tp/len(val_df[val_df['pre_rating'] > val_df['avg']])
    recall = tp/len(test_df[test_df['rating'].astype(float) > test_df['avg']])

    f1 = 2*(precision*recall)/(precision + recall)

    return rmse, f1