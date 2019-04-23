import os

def file_size_in_mb(filename):
    return round(os.path.getsize(filename)/(1024*1024.0),2)