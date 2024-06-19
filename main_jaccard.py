from Jaccard_hadoop.main_hadoop import run_jaccard
from custom_util import (
    env_dict,
    write_data_to_file,
)

HADOOP_PATH = env_dict["hadoop_path"]

input_path = f"{HADOOP_PATH}/input/input_file.txt"

result = []
result_data = run_jaccard(input_path)
result.append(result_data)
result = [line for row in result for line in row]

output_path = f"./hadoop_output/jaccard.txt"
write_data_to_file(output_path, result)
