import csv
import json
import multiprocessing as mp
import os
import re
import warnings
from glob import glob
from itertools import chain

import requests
from requests import ConnectionError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

connection_timeout = 30  # seconds


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def dropbox_upload(path):
    split_path = path.split("/")
    if " " not in split_path[-1] and not bool(re.search("[A-Z]{2,4}?\s?[0-9]+$", path)):
        # print(path)
        final_path = "/".join(split_path[4::][:-1]).replace(" ", "\\ ")
        path = path.replace(" ", "\\ ")
        return "sudo bash /Users/patrick/Downloads/Dropbox-Uploader/dropbox_uploader.sh upload -s " + \
               path + " Bill\ datasets\ fr\ openstates\ 2014_2017/" + final_path + "/"


def make_request(path, state, url=None, file_type=None, id=None, session=None, bill_name=None, house=None ):
    try:
        # We catch the insecure request warning because we have SSL certificate issues for states like MA
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # r = requests.get(url, allow_redirects=True, verify=False)
            r = requests_retry_session().get(url, allow_redirects=True, verify=False)
        if r.status_code == 200:
            filename = "_".join([state, session, house, bill_name])
            if "html" in file_type and ".html" not in filename:
                filename += ".html"
            if "htm" in file_type and ".htm" not in filename:
                filename += ".htm"
            if "pdf" in file_type and ".pdf" not in filename:
                filename += ".pdf"
            if "msword" in file_type and ".doc" not in filename:
                filename += ".doc"
            else:
                open(os.path.dirname(path) + "/" + house + "/" + filename, 'wb').write(r.content)
            return dict(state=state, url=url, path=path, filename=filename, id=id, success=True)
        else:
            print(r.status_code)
            return dict(state=state, url=url, path=path, filename=None, id=id, success=False)

    except (ConnectionError, requests.exceptions.ChunkedEncodingError, requests.exceptions.MissingSchema):
        print("got error")
        return dict(state=state, url=url, path=path, filename=None, id=id, success=False)


def make_ftp_request(path, state, url, file_type, id):
    split_url = url.split("/")
    server = split_url[0]
    directory = "/".join(split_url[1:-1]) + "/"
    file = split_url[-1]

    # # log in
    # ftp = ftplib.FTP(server)
    # ftp.login()
    try:
        # go back to top directory then go to the directory of interest
        ftp.cwd("/")
        ftp.cwd(directory)

        # we should log in to the server, list out the entire subdirectory for a session and walk through it
        # so if we get a file from the 84th session of texas we go to the top of the session directory and walk through

        # Create the file path
        split_path = path.split("/")
        session_name = split_path[-3].replace(" ", "_")
        house = split_path[-2]
        bill_name = split_path[-1].replace(" ", "_")
        filename = "_".join([state, session_name, house, bill_name])
        if "html" in file_type and ".html" not in filename:
            filename += ".html"
        if "htm" in file_type and ".htm" not in filename:
            filename += ".htm"
        if "pdf" in file_type and ".pdf" not in filename:
            filename += ".pdf"
        if "msword" in file_type and ".doc" not in filename:
            filename += ".doc"

        # Do this last once we have the file and path ready
        # We have to put the write statement as a callback for the retrbinary function
        ftp.retrbinary("RETR {}".format(file),
                       open(os.path.dirname(path) + "/" + filename, 'wb').write)
        return dict(state=state, url=url, path=path, filename=filename, id=id, success=True)
    except:
        print("{0} failed".format(url))
        return dict(state=state, url=url, path=path, filename=None, id=id, success=False)


def get_bill_text(args):
    path = args[0]
    state = args[1]
# def get_bill_text(path, state):
#     print(path)
    with open(path) as file:
        try:
            bill_json = json.load(file)
        except (UnicodeDecodeError, json.decoder.JSONDecodeError):
            return dict(state=state, url=None, path=path, filename=None, id=None, success=False)
    version_list = bill_json.get('versions')
    id = bill_json.get('_id')
    session = bill_json.get('legislative_session')
    bill_name = bill_json.get('identifier').replace(" ", "_")
    url = None
    if "lower" in bill_json.get("from_organization"):
        house = "lower"
    else:
        house = "upper"
    # If there is more than one version, get the last one
    if len(version_list) > 0 and version_list[0].get('links') is not None:
        final_version = version_list[len(version_list)-1]
        if final_version.get("links", None) is not None and len(final_version.get("links", None)) > 0:
            links = final_version.get("links")
            url = links[len(links)-1].get("url")
            file_type = links[len(links)-1].get("media_type")
            return make_request(path, state, url, file_type, id, session, bill_name, house)
    else:
        return dict(state=state, url=url, path=path, filename=None, id=id, success=False)


def state_func(args):
    state = args[0]
    completed_paths = args[1]
    single_state = args[2]

    rootdir = '/Users/patrick/Downloads/open_state_data'
    state_path = rootdir + "/" + state + "/bills/" + state + "/"
    bill_paths = glob(state_path + "*/*", recursive=True)
    # bill_paths = [path for path in bill_paths if path not in completed_paths]
    bill_paths = list(set(bill_paths) - set(completed_paths))
    bill_paths = list(filter(
        lambda path: not path.endswith(".pdf") and not path.endswith(
            ".html") and not path.endswith(".htm") and "bill_" in path and path.endswith(".json"), bill_paths))
    # now go through each bill and pull the url to download the file and then we want to download it to the same
    # folder
    state_results = []
    fieldnames = ["state", "url", "path", "filename", "id", "success"]
    # with open("/Users/patrick/Downloads/open_states_data/diagnostic_results.csv", 'wb') as f:


    with open("/Users/patrick/Downloads/open_state_data/diagnostic_results.csv", 'a') as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        # dictwriter.writeheader()
        if len(bill_paths) > 0:
            if single_state:
                pool = mp.Pool(processes=mp.cpu_count())
                inputs = [(path, state) for path in bill_paths]
                state_results = list(pool.map(get_bill_text, inputs))
                for result in state_results:
                    if result is None:
                        result = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
                    # if set(res.keys()) != set(fieldnames):
                    #     res = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
                    dictwriter.writerow(result)
                    with open('/Users/patrick/Downloads/finished_paths.txt', 'a') as fd:
                        writer = csv.writer(fd)
                        writer.writerow([result['path']])
            else:

                for path in bill_paths:
                    if not path.endswith(".pdf") and not path.endswith(".html") and not path.endswith(".htm"):
                        print(path)
                        res = get_bill_text([path, state])
                        # writer.writerows(chain.from_iterable(final_results))
                        if res is None:
                            res = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
                        # if set(res.keys()) != set(fieldnames):
                        #     res = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
                        dictwriter.writerow(res)
                        state_results.append(res)
                        with open('/Users/patrick/Downloads/finished_paths.txt', 'a') as fd:
                            writer = csv.writer(fd)
                            writer.writerow([path])
                        # if res.get('path', None) is not None and res.get('filename', None) is not None and res.get('success'):
                        #     # Try to upload the file to dropbox
                        #     file_path = res['path']
                        #     split_path = file_path.split("/")
                        #     upload_file = "/".join(split_path[-1]) + "/" + res.get('filename')
                        #     command = dropbox_upload(upload_file)
                        #     process = subprocess.Popen(command, shell=True)
    return state_results


if __name__ == '__main__':
    pool = mp.Pool(processes=mp.cpu_count()-1)
    # states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY",
    #           "LA", "ME", "MD", "MA", "MI",
    #           "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    #           "SC", "SD", "TN", "TX", "UT",
    #           "VT", "VA", "WA", "WV", "WI", "WY"]
    # states = ["tx", "ma", "ct"]
    states = ["ga"]
    states = [x.lower() for x in states]
    ignored_states = ["de", "la", "mt", "or", "sd", "ut", "wa", "wv", "oh"]
    states = [state for state in states if state not in ignored_states]
    rootdir = '/Users/patrick/Downloads/open_state_data/'
    # final_results = []
    single_state = False

    # finished_states = ["al", "ak"]
    finished_states = []

    # load list of completed paths
    completed_paths = []
    with open('/Users/patrick/Downloads/finished_paths.txt') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            path = row[0]
            if not path.endswith(".pdf") and not path.endswith(".html") and not path.endswith(".htm"):
                completed_paths.append(path)

    inputs = [(state, completed_paths, single_state) for state in states if state not in finished_states]

    if not single_state:
        final_results = list(pool.map(state_func, inputs))
    else:
        final_results = []
        for input in inputs:
            final_results.append(state_func(input))


    # Write the results to a csv file
    fieldnames = ["state", "url", "path", "filename", "id", "success"]
    # with open("/Users/patrick/Downloads/open_states_data/diagnostic_results.csv", 'wb') as f:
    with open("/Users/patrick/Downloads/open_state_data/final_results.csv", 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chain.from_iterable(final_results))



