import time

import spark


if __name__ == "__main__":
    NUMBER_OF_CLUSTERS = 3
    MULTIPLIER = 1

    # input_file_path = "input/input_file.txt"
    # item_file_path = "input/items.txt"
    input_file_path = "spark/input/input_file_copy.txt"
    item_file_path = "spark/input/items_copy.txt"
    label_file_path = "spark/output/labels"
    centroid_file_path = "spark/output/centroids"

    mfps_output_path = f"spark/output/mfps"

    # Start timer
    start_time = time.perf_counter()

    # Clustering
    spark.run_spark_clustering(
        input_file_path=input_file_path,
        item_file_path=item_file_path,
        label_output_path=label_file_path,
        centroid_output_path=centroid_file_path,
        number_of_clusters=NUMBER_OF_CLUSTERS,
        multiplier=MULTIPLIER,
    )

    # Split files
    input_filename = spark.split_files_by_label(
        input_file_path=input_file_path,
        label_file_path=label_file_path,
        centroid_file_path=centroid_file_path,
    )

    # Calculate sim
    for index in range(NUMBER_OF_CLUSTERS):
        input_path = f"{input_filename}_{index}"

        spark.run_mfps(input_file_path=input_file_path, output_path=mfps_output_path)

    # End timer
    end_time = time.perf_counter()

    # Calculate elapsed time
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time}s")
