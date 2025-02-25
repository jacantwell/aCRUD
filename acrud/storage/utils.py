import os


def get_meta_data_key(key: str):
    key = key.split(".")[:-1]
    key = ".".join(key)
    meta_data_key = key + "_meta.json"
    return meta_data_key
