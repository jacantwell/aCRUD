import os


def get_meta_data_file_path(file_path: str):
    file_path = file_path.split(".")[:-1]
    file_path = ".".join(file_path)
    meta_data_file_path = file_path + "_meta.json"
    return meta_data_file_path
