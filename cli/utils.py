import re


def load_file(filename):
    f = open(filename, 'r')
    file_str = f.read()
    f.close()
    return file_str


def extract_manifest_filename_from_command(command: str, MANIFEST_FILENAME: str):
    processed_command = command
    m = re.search('--manifest (.+?)( |$)', command)
    if m:
        manifest_filename = m.group(1)
        begin, end = m.span()
        if processed_command[end:] != "":
            processed_command = processed_command[:begin] + processed_command[end:]
        else:
            processed_command = processed_command[:begin-1]
    else:
        manifest_filename = MANIFEST_FILENAME
    return manifest_filename, processed_command
