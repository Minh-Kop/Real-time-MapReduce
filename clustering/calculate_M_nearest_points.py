import pandas as pd


def M_nearest_points_pandas(input_path, M, output_path):
    if M == 0:
        return

    df = pd.read_csv(
        input_path,
        sep="\t",
        names=["user", "distance"],
        dtype={"user": "Int64", "distance": "Float64"},
    )
    df = df.sort_values(["distance"], ascending=False)
    df = df.iloc[:M]
    df.to_csv(output_path, sep="\t", index=False, header=False)


if __name__ == "__main__":
    M = 3
    M_nearest_points_pandas(
        "./clustering/output/D.txt", M, "./clustering/output/M_nearest_points.txt"
    )
