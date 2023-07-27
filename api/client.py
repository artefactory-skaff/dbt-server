import requests,json,os
from firestore import get_status
from utils import parse_command
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('.env.client')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"
MANIFEST_FILENAME = os.getenv('MANIFEST_FILENAME')
DBT_PROJECT_FILE = os.getenv('DBT_PROJECT_FILE')
PROFILES_FILE = os.getenv('PROFILES_FILE')

def load_file(filename):
    f = open(filename,'r')
    file_str = f.read()
    f.close()
    return file_str

def send_command(command,dbt_project_file,profiles_file):
    url = SERVER_URL + "dbt"
    main_command,args = parse_command(command)
    if "--manifest" in args.keys():
        manifest_filename = args["--manifest"]
        del args["--manifest"]
    else:
        manifest_filename = MANIFEST_FILENAME
    manifest_str = load_file(manifest_filename)
    dbt_project_str = load_file(dbt_project_file)
    profiles_str = load_file(profiles_file)

    if args != {}:
        data = {
            "command":main_command,
            "args": args,
            "manifest":manifest_str,
            "dbt_project":dbt_project_str,
            "profiles":profiles_str
        }
    else:
        data = {
            "command":main_command,
            "manifest":manifest_str,
            "dbt_project":dbt_project_str,
            "profiles":profiles_str
        }

    res = requests.post(url=url,json=data)
    print(res.status_code)
    return res.text



def main():
    dbt_project_file,profiles_file = DBT_PROJECT_FILE,PROFILES_FILE

    command1 = "list"
    command2 = "run --select vbak_dbt"
    command3 = "list --manifest ../manifest.json"

    print(command1)
    uuid1 = json.loads(send_command(command1,dbt_project_file,profiles_file))["uuid"]
    print("uuid: "+uuid1)
    status = get_status(uuid1)
    print(status)

    print(command2)
    uuid2 = json.loads(send_command(command2,dbt_project_file,profiles_file))["uuid"]
    print("uuid: ",uuid2)
    status = get_status(uuid2)
    print(status)

    print(command3)
    uuid3 = json.loads(send_command(command3,dbt_project_file,profiles_file))["uuid"]
    print("uuid: "+uuid3)
    status = get_status(uuid3)
    print(status)


main()
