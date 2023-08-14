def load_file(filename):
    with open(filename, 'r') as f:
        file_str = f.read()
    return file_str
