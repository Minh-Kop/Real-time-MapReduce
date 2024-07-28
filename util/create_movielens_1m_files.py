import pandas as pd


def create_input_file(input_path, output_path):
    rating_df = pd.read_csv(
        input_path,
        sep="::",
        names=["user", "item", "rating", "timestamp"],
        dtype="str",
    )
    pd.DataFrame(
        {
            "key": rating_df["user"] + ";" + rating_df["item"],
            "value": rating_df["rating"] + ";" + rating_df["timestamp"],
        }
    ).to_csv(output_path, sep="\t", index=False, header=False)


# Function to convert list of categories to binary string
def categories_to_binary_str(categories, full_categories):
    binary_list = [1 if category in categories else 0 for category in full_categories]
    return "|".join(map(str, binary_list))


def create_item_file(full_categories, input_path, output_path=""):
    item_df = pd.read_csv(
        input_path,
        sep="::",
        usecols=[0, 2],
        names=["item", "categories"],
        dtype="str",
        encoding="latin1",
    )

    item_df["categories"] = item_df["categories"].str.split("|")

    item_df["binary_categories"] = item_df["categories"].apply(
        lambda x: categories_to_binary_str(x, full_categories)
    )

    item_df.drop(columns=["categories"]).to_csv(
        output_path, sep="\t", index=False, header=False
    )


def create_user_file(input_path, output_path=""):
    user_df = pd.read_csv(
        input_path,
        sep="::",
        usecols=[0],
        names=["user"],
        dtype="str",
    ).drop_duplicates("user")
    user_df.to_csv(output_path, sep="\t", index=False, header=False)


if __name__ == "__main__":
    rating_path = "/home/mackop/Downloads/ml-1m/ratings.dat"
    # create_input_file(rating_path, output_path="ml_1m_input.txt")

    item_path = "/home/mackop/Downloads/ml-1m/movies.dat"
    full_categories = [
        "Action",
        "Adventure",
        "Animation",
        "Children's",
        "Comedy",
        "Crime",
        "Documentary",
        "Drama",
        "Fantasy",
        "Film - Noir",
        "Horror",
        "Musical",
        "Mystery",
        "Romance",
        "Sci-Fi",
        "Thriller",
        "War",
        "Western",
    ]
    # create_item_file(full_categories, item_path, output_path="ml_1m_item.txt")

    create_user_file(rating_path, "input/users_1m.txt")
