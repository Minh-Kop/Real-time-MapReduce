import re
import os

from dotenv import load_dotenv

load_dotenv()

env_dict = {"hadoop_path": os.getenv("HADOOP_PATH")}


def write_data_to_file(filename, data, mode="w"):
    output_file = open(filename, mode)
    for el in data:
        output_file.writelines(el)
    output_file.close()


def get_txt_filename(file_path):
    pattern = "[^\/]\/([^\/\.]+)(\..*)?$"
    match = re.search(pattern, file_path)
    if match:
        name = match.group(1)
        return f"{name}.txt"
    return None


def put_files_to_hdfs(local_path, hdfs_path):
    os.system(f"hdfs dfs -rm -r {hdfs_path}")
    os.system(f"hdfs dfs -put {local_path} {hdfs_path}")


def create_directory_in_hdfs(hdfs_path):
    os.system(f"hdfs dfs -mkdir -p {hdfs_path}")


def delete_directory_in_hdfs(hdfs_path):
    os.system(f"hdfs dfs -rm -r {hdfs_path}")


def create_and_delete_intermediate_directories(func, directory):
    # Create HDFS directory for intermediate input
    create_directory_in_hdfs("temp-input")
    create_directory_in_hdfs(directory)

    # Execute function
    returned_value = func()

    # Delete directories
    delete_directory_in_hdfs("temp-input")
    delete_directory_in_hdfs(directory)

    return returned_value


def run_mr_job(mr_job_class, input_args):
    mr_job = mr_job_class(args=input_args)
    with mr_job.make_runner() as runner:
        runner.run()
        data = []
        for key, value in mr_job.parse_output(runner.cat_output()):
            data.append(f"{key}\t{value}")
        return data


def run_mr_job_hadoop(
    mr_job_class, input_args, output_path=None, create_txt_file=False
):
    input_args = ["-r", "hadoop"] + input_args
    if output_path:
        input_args = input_args + ["--output-dir", output_path]
    else:
        create_txt_file = False

    mr_job = mr_job_class(args=input_args)
    with mr_job.make_runner() as runner:
        if output_path:
            runner.fs.rm(output_path)
        runner.run()

        data = []
        for key, value in mr_job.parse_output(runner.cat_output()):
            data.append(f"{key}\t{value}")

        if create_txt_file:
            filename = get_txt_filename(output_path)
            file_path = f"./hadoop_output/{filename}"
            write_data_to_file(file_path, data)

            hdfs_path = f"{env_dict['hadoop_path']}/temp-input/{filename}"
            runner.fs.rm(hdfs_path)
            runner.fs.put(file_path, hdfs_path)

        return data
