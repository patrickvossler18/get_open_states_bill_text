import csv
import ftplib
import json
import os
import re
import subprocess
import warnings
from glob import glob
from io import StringIO
from itertools import chain

import requests
# import multiprocessing as mp
from pathos.multiprocessing import ProcessingPool as Pool
from requests import ConnectionError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import wpf


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


connection_timeout = 30  # seconds


class Bill:

    # DONE: Write up get vote function
    def __init__(self, path=None, state=None, ftp=None):
        self.path = path
        self.state = state
        self.bill_json = None
        self.url = None
        self.id = None
        self.version_list = None
        self.file_type = None
        self.filename = None
        self.document_content = None
        self.write_file_path = None
        self.uuid = None
        self.passage = None
        self.success = None
        self.failure_reason = None
        self.ftp = ftp

    def load_json(self):
        # try to read the json file from the path provided
        with open(self.path) as file:
            try:
                self.bill_json = json.load(file)
            except (UnicodeDecodeError, json.decoder.JSONDecodeError):
                self.bill_json = None
                self.success = False
                self.failure_reason = "JSONDecodeError,UnicodeDecodeError"

    def get_bill_text(self):
        path = self.path
        state = self.state

        self.version_list = self.bill_json.get('versions')
        version_list_length = len(self.version_list)
        self.id = self.bill_json.get('_id')
        # If there is more than one version, get the last one
        if version_list_length > 0 and self.version_list[version_list_length - 1].get('url', None) is not None:
            self.url = self.version_list[version_list_length - 1].get('url')
            self.file_type = self.version_list[version_list_length - 1].get("mimetype")

            if "ftp://" in self.url:
                # remove the ftp:// prefix
                self.url = self.url.replace("ftp://", "")
                # return make_ftp_request(path, state, url, file_type, id)
            else:

                # Alaska's links don't work anymore :(
                # New format http://www.akleg.gov/basis/Bill/Text/27?Hsid=HB0001C
                if self.state == "ak":
                    # get the session and the bill name from the old url
                    bill_name_re = re.compile("hsid=(.*)&")
                    bill_name = bill_name_re.search(self.url).group(1)
                    session_num_re = re.compile("session=(.*)")
                    session_num = session_num_re.search(self.url).group(1)
                    new_base_url = "http://www.akleg.gov/basis/Bill/Text/{0}?Hsid={1}".format(session_num, bill_name)
                    self.url = new_base_url
                    # return make_request(path, state, url, file_type, id)
                # Smaller change in url for Alabama
                # old "http://alisondb.legislature.state.al.us/acas/searchableinstruments/2011rs/PrintFiles/HB1-int.pdf"
                # new "http://alisondb.legislature.state.al.us/ALISON/SearchableInstruments/2019rs/PrintFiles/HJR259-enr.pdf"
                if self.state == "al":
                    # get the session and the bill name from the old url
                    split_url = self.url.split("/")
                    bill_name = split_url[-1]
                    session_num = split_url[-3]
                    new_base_url = "http://alisondb.legislature.state.al.us/ALISON/SearchableInstruments/{0}/PrintFiles/{1}".format(
                        session_num, bill_name)
                    self.url = new_base_url
                    # DONE: Figure out why we would end up with None for url here
                    # We'd have a None for url if there isn't a version list.. duh
                # Delaware has a complete new system but I've scraped it to get the new ids and stuff and I think I can just
                # pull the data directly from their website
                if self.state == "de":
                    pass
                # Moved to new website. Uses aspx :(
                # Using the open states scraper to get the updated websites
                if self.state == "ga":
                    # need to scrape with open states
                    # got the open states links
                    pass
                # id is using wordpress now
                if state == "id":
                    # old url "http://legislature.idaho.gov/legislation/2011/H0001.pdf"
                    # new url "https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2011/legislation/H0001.pdf"
                    # get the session and the bill name from the old url
                    split_url = self.url.split("/")
                    bill_name = split_url[-1]
                    session_num = split_url[-2]
                    new_base_url = "https://legislature.idaho.gov/wp-content/uploads/sessioninfo/{0}/legislation/{1}".format(
                        session_num, bill_name)
                    self.url = new_base_url
                    # return make_request(path, state, url, file_type, id)
                if self.state == "il":
                    self.url += "&print=True"
                    # return make_request(path, state, url, file_type, id)
                if self.state == "in":
                    for version in self.version_list:
                        if "Enrolled Act" in version.get("name"):
                            self.url = version.get('url')
                            break
                    # return make_request(path, state, url, file_type, id)
                if self.state == "ks":
                    for version in self.version_list:
                        if "Enrolled" in version.get("name"):
                            self.url = version.get('url')
                            break
                    # return make_request(path, state, url, file_type, id)
                if self.state == "ky":
                    # old url "http://www.lrc.ky.gov/record/11RS/HB1/bill.doc"
                    # new url "https://apps.legislature.ky.gov/record/12RS/hb1/bill.doc"
                    # get the session and the bill name from the old url
                    split_url = self.url.split("/")
                    bill_name = split_url[-2].lower()
                    session_num = split_url[-3]
                    new_base_url = "https://apps.legislature.ky.gov/record/{0}/{1}/bill.doc".format(
                        session_num, bill_name)
                    self.url = new_base_url
                    # return make_request(path, state, url, file_type, id)
                if self.state == "la":
                    # need to scrape with open states
                    # open states only has 2017-present data
                    pass
                if self.state == "ma":
                    term = self.bill_json.get('_term')
                    bill_id = self.bill_json.get("bill_id").replace(" ", "")
                    new_url = "https://malegislature.gov/Bills/{0}/{1}.pdf".format(term, bill_id)
                    self.url = new_url
                    self.file_type = "pdf"
                    # return make_request(path, state, url, file_type, id)
                if self.state == "mt":
                    # use open states
                    pass
                if self.state == "or":
                    # use open states
                    # open states only has 2017-present
                    pass
                if self.state == "sd":
                    # use open states
                    pass
                if state == "ut":
                    # use open states
                    pass
                # va is ok
                if self.state == "wa":
                    # use open states
                    pass
                # wi is ok
                if self.state == "wv":
                    # use open states
                    # only has data from 2016-present
                    pass
                # wy is ok
                if self.state == "ny":
                    # old url "http://open.nysenate.gov/legislation/bill/J14-2011"
                    # new url "https://legislation.nysenate.gov/pdf/bills/2011/J14"
                    split_url = self.url.split("/")
                    if len(split_url[-1].split("-")) > 1:
                        bill_id, term = split_url[-1].split("-")
                        self.url = "https://legislation.nysenate.gov/pdf/bills/{0}/{1}".format(term, bill_id)
                    else:
                        bill_id = split_url[-1]
                        term = split_url[-2]
                        self.url = "https://legislation.nysenate.gov/pdf/bills/{0}/{1}".format(term, bill_id)
                    # return make_request(path, state, url, file_type, id)
                if self.state == "oh":
                    # use open states
                    pass
                # colorado uses word perfect??
                # Use the version with final in name
                elif self.state == "co" and "octet-stream" in self.file_type:
                    for version in self.version_list:
                        if "Final" in version.get("name"):
                            self.url = version.get('url')
                            break

    def make_request(self):
        if self.url:
            if self.state == "co" and "octet-stream" in self.file_type:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    r = requests_retry_session().get(self.url, allow_redirects=True, verify=False)
                if r.status_code == 200:
                    try:
                        wpf_bytes = wpf.WordPerfect(r.content)
                        for i in wpf_bytes.elements:
                            if i.type == 1:
                                wpf_text = i.data
                                break
                        split_path = self.path.split("/")
                        session_name = split_path[-3].replace(" ", "_")
                        house = split_path[-2]
                        bill_name = split_path[-1].replace(" ", "_")
                        self.filename = "_".join([self.state, session_name, house, bill_name])
                        self.filename += ".txt"
                        self.write_file_path = os.path.dirname(self.path) + "/" + self.filename
                        self.document_content = wpf_text
                        # open(os.path.dirname(self.path) + "/" + filename, 'wb').write(r.content)
                        # return dict(state=state, url=url, path=path, filename=filename, id=id, success=True)
                    except IndexError:
                        # DONE: write function for logging failures
                        print("{0} failed".format(self.url))
                        self.success = False
                        self.failure_reason = "IndexError reading wpf file"
                        # return dict(state=state, url=url, path=path, filename=None, id=id, success=False)
    
            elif self.state in ['tx', 'ct']:
                # DONE: set up initialization of FTP server
                split_url = self.url.split("/")
                server = split_url[0]
                directory = "/".join(split_url[1:-1]) + "/"
                file = split_url[-1]
    
                # # log in
                # ftp = ftplib.FTP(server)
                # ftp.login()
                try:
                    # go back to top directory then go to the directory of interest
                    self.ftp.cwd("/")
                    self.ftp.cwd(directory)
    
                    # we should log in to the server, list out the entire subdirectory for a session and walk through it
                    # so if we get a file from the 84th session of texas we go to the top of the session directory and walk through
    
                    # Create the file path
                    split_path = self.path.split("/")
                    session_name = split_path[-3].replace(" ", "_")
                    house = split_path[-2]
                    bill_name = split_path[-1].replace(" ", "_")
                    filename = "_".join([self.state, session_name, house, bill_name])
                    if "html" in self.file_type and ".html" not in self.filename:
                        self.filename += ".html"
                    if "htm" in self.file_type and ".htm" not in self.filename:
                        self.filename += ".htm"
                    if "pdf" in self.file_type and ".pdf" not in self.filename:
                        self.filename += ".pdf"
                    if "msword" in self.file_type and ".doc" not in self.filename:
                        self.filename += ".doc"
    
                    self.write_file_path = os.path.dirname(self.path) + "/" + filename
    
                    ftp_response = StringIO()
                    self.ftp.retrbinary("RETR {}".format(file), ftp_response.write)
    
                    self.document_content = ftp_response.getvalue()
                    # Do this last once we have the file and path ready
                    # We have to put the write statement as a callback for the retrbinary function
                    # ftp.retrbinary("RETR {}".format(file),
                    #                open(os.path.dirname(self.path) + "/" + filename, 'wb').write)
                    # return dict(state=state, url=url, path=path, filename=filename, id=id, success=True)
                except:
                    # DONE: write function for logging failures
                    self.success = False
                    self.failure_reason = "ftp issue"
                    print("{0} failed".format(self.url))
                    # return dict(state=state, url=url, path=path, filename=None, id=id, success=False)
    
            else:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        r = requests_retry_session().get(self.url, allow_redirects=True, verify=False)
                    if r.status_code == 200:
                        split_path = self.path.split("/")
                        session_name = split_path[-3].replace(" ", "_")
                        house = split_path[-2]
                        bill_name = split_path[-1].replace(" ", "_")
                        self.filename = "_".join([self.state, session_name, house, bill_name])
                        if "html" in self.file_type and ".html" not in self.filename:
                            self.filename += ".html"
                        if "pdf" in self.file_type and ".pdf" not in self.filename:
                            self.filename += ".pdf"
                        if "msword" in self.file_type and ".doc" not in self.filename:
                            self.filename += ".doc"
                        self.write_file_path = os.path.dirname(self.path) + "/" + self.filename
                        self.document_content = r.content
                        self.success = True
                        # else:
                        #     open(os.path.dirname(self.path) + "/" + filename, 'wb').write(r.content)
                        # return dict(state=state, url=url, path=path, filename=filename, id=id, success=True)
                    # else:
                    #     return dict(state=state, url=url, path=path, filename=None, id=id, success=False)
    
                except (
                        ConnectionError, requests.exceptions.ChunkedEncodingError, requests.exceptions.MissingSchema):
                    print("{0} failed".format(self.url))
                    # DONE: write function for logging failures
                    self.success = False
                    self.failure_reason = "connectionError,ChunkedEncodingError,MissingSchema"
                    # Add function to log exceptions
                    # return dict(state=state, url=url, path=path, filename=None, id=id, success=False)
        else:
            self.success = False
            self.failure_reason = 'url does not exist'

    def get_vote_info(self):
        path = self.path
        state = self.state
        # read in the json data
        # print(path)
        bill_json = self.bill_json

        # We want to see if signed is not null.
        action_dates = bill_json.get("action_dates")
        signed = action_dates.get("signed")
        # If it is signed into law we want the bill_id and vote information
        if signed is not None:
            votes = bill_json.get("votes")
            if len(votes) > 0:
                session = votes[0].get("session")
                bill_id = bill_json.get("bill_id")
                # create uuid
                self.uuid = "_".join([state, str(session).replace(" ", ""), bill_id.replace(" ", "")])
                self.passage = True
            else:
                # see if we can get the session from within the bill json
                if bill_json.get("session", None) is not None:
                    session = bill_json.get("session", None)
                    bill_id = bill_json.get("bill_id")
                    self.uuid = "_".join([state, str(session).replace(" ", ""), bill_id.replace(" ", "")])
                    self.passage = True
                else:
                    # situation where we somehow have signed the bill but don't have information about the votes?
                    # and where we can't get the session from the bill_json itself
                    self.uuid = None
                    self.passage = True
        else:
            self.uuid = None
            self.passage = False

    def write_to_file(self):
        open(self.write_file_path, 'wb').write(self.document_content)

    def upload_file(self):

        def dropbox_upload(path):
            split_path = path.split("/")
            if " " not in split_path[-1] and not bool(re.search("[A-Z]{2,4}?\s?[0-9]+$", path)):
                # print(path)
                final_path = "/".join(split_path[4::][:-1]).replace(" ", "\\ ")
                path = path.replace(" ", "\\ ")
                return "sudo bash /home/ubuntu/Dropbox-Uploader/dropbox_uploader.sh upload -s " + \
                       path + " Bill\ datasets\ fr\ openstates\ 2014_2017/" + final_path + "/"

        if self.write_file_path is not None and self.success:
            # Try to upload the file to dropbox
            file_path = self.write_file_path
            split_path = file_path.split("/")
            upload_file = "/".join(split_path[-1]) + "/" + self.filename
            command = dropbox_upload(upload_file)
            process = subprocess.Popen(command, shell=True)

    def combine_info(self):
        self.load_json()
        self.get_bill_text()
        self.make_request()
        self.get_vote_info()
        self.write_to_file()
        self.upload_file()


class State:
    # TODO: Test log, upload, and write functions
    def __init__(self, state=None, rootdir=None, completed_paths=None):
        self.state = state
        self.completed_paths = completed_paths
        self.rootdir = rootdir
        self.state_path = self.rootdir + "/" + self.state + "/bills/" + self.state + "/"
        self.ftp = None
        self.results = []
        bill_paths = glob(self.state_path + "*/*/*", recursive=True)
        bill_paths = list(set(bill_paths) - set(self.completed_paths))
        bill_paths = list(filter(
            lambda path: not path.endswith(".pdf") and not path.endswith(
                ".html") and not path.endswith(".htm"), bill_paths))
        self.bill_paths = bill_paths

    def init_ftp(self):
        if self.state == 'tx' or self.state == 'ct':
            ftp_server_dict = {"tx": "ftp.legis.state.tx.us",
                               "ct": "ftp.cga.ct.gov"}
            # create a single ftp login and we will just jump to the necessary working directory to get our file
            server = ftp_server_dict[self.state]
            # this is a no-no but for these two states I don't think it's a huge deal
            self.ftp = ftplib.FTP(server)
            self.ftp.login()

    def write_to_log(self):
        fieldnames = ["path", "state", "url", "id", "file_type", "filename", "write_file_path", "uuid",
                      "passage", "success", "failure_reason"]

        for obj in self.results:
            if os.path.exists(rootdir + "/diagnostic_results.csv"):
                append_write_diag = "a"
            else:
                append_write_diag = 'w'

            if os.path.exists(rootdir + '/finished_paths.txt'):
                append_write_finished = "a"
            else:
                append_write_finished = 'w'

            with open(rootdir + "/diagnostic_results.csv", append_write_diag) as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerow({k: getattr(obj, k) for k in fieldnames})
            with open(rootdir + '/finished_paths.txt', append_write_finished) as fd:
                writer = csv.writer(fd)
                writer.writerow([obj.path])

    def process_files(self):
        state = self.state

        def process_bill(args):
            state = args[0]
            path = args[1]
            bill_info = Bill(path, state)
            bill_info.combine_info()
            return (bill_info)

        if state == 'tx' or state == 'ct':
            self.init_ftp()
            # But because we are using one session we can't use multiple processes because we need to jump to different directories
            for path in self.bill_paths:
                bill_info = Bill(self.path, state, self.ftp)
                bill_info.combine_info()
                self.results.append(bill_info)
        else:
            pool = Pool(processes=3)
            inputs = [(state, path) for path in self.bill_paths]
            state_results = list(pool.map(process_bill, inputs))
            for result in state_results:
                self.results.append(result)


def state_func(args):

    state = args[0]
    completed_paths = args[1]
    single_state = args[2]

    rootdir = args[3]
    print(state)
    state_class = State(state, rootdir, completed_paths)
    state_class.init_ftp()
    state_class.process_files()
    print("processed files for {0}".format(state))
    state_class.write_to_log()
    print("wrote {0} files to log".format(state))
    # now state_class will have a list results with all of the bill classes for each file
    fieldnames = ["state", "url", "path", "filename", "id", "success"]

    # DONE: Write function to write file contents to file
    # DONE: Write function to upload file to dropbox

    # with open("/home/ubuntu/Downloads/open_state_data/diagnostic_results.csv", 'a') as f:
    #     dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
    #     # dictwriter.writeheader()
    #     if len(bill_paths) > 0:
    #         if single_state:
    #             pool = mp.Pool(processes=mp.cpu_count())
    #             inputs = [(path, state) for path in bill_paths]
    #             state_results = list(pool.map(get_bill_text, inputs))
    #             for result in state_results:
    #                 if result is None:
    #                     result = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
    #                 # if set(res.keys()) != set(fieldnames):
    #                 #     res = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
    #                 dictwriter.writerow(result)
    #                 with open('/home/ubuntu/Downloads/finished_paths.txt', 'a') as fd:
    #                     writer = csv.writer(fd)
    #                     writer.writerow([result['path']])


if __name__ == '__main__':
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY",
              "LA", "ME", "MD", "MA", "MI",
              "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
              "SC", "SD", "TN", "TX", "UT",
              "VT", "VA", "WA", "WV", "WI", "WY"]
    # states = ["tx", "ma", "ct"]
    states = [x.lower() for x in states]
    ignored_states = ["de", "ga", "la", "mt", "or", "sd", "ut", "wa", "wv", "oh"]
    states = [state for state in states if state not in ignored_states]
    # rootdir = '/home/ubuntu/Downloads/open_state_data'
    rootdir = "/Users/patrick/Downloads/open_state_data"
    # final_results = []
    single_state = False

    # finished_states = ["al", "ak"]
    finished_states = []

    # load list of completed paths
    completed_paths = []
    # with open('/home/ubuntu/Downloads/finished_paths.txt') as csv_file:
    #     csv_reader = csv.reader(csv_file, delimiter=',')
    #     for row in csv_reader:
    #         path = row[0]
    #         if not path.endswith(".pdf") and not path.endswith(".html") and not path.endswith(".htm"):
    #             completed_paths.append(path)

    state_inputs = [(state, completed_paths, single_state, rootdir) for state in states if state not in finished_states]

    final_results = []
    # This is really just for state in state
    for state in state_inputs:
        final_results.append(state_func(state))


    # Write the results to a csv file
    fieldnames = ["state", "url", "path", "filename", "id", "success"]
    # with open("/Users/patrick/Downloads/open_states_data/diagnostic_results.csv", 'wb') as f:
    with open("/home/ubuntu/Downloads/open_state_data/final_results.csv", 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chain.from_iterable(final_results))






