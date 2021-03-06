import csv
import json
import multiprocessing as mp
import os
import re
import time
from glob import glob
from itertools import chain

import requests
from requests import ConnectionError

import wpf

connection_timeout = 30  # seconds
# url = "http://www.leg.state.co.us/clics/clics2011a/csl.nsf/billcontainers/D9ABC2E028CA4D3487257808008012EA/$FILE/1032.enr"


def track_job(job, update_interval=3):
    while job._number_left > 0:
        print("Tasks remaining = {0}".format(
            job._number_left * job._chunksize))
        time.sleep(update_interval)


def make_request(path, state, url=None, file_type=None, id=None):
    try:
        r = requests.get(url, allow_redirects=True)
        if r.status_code == 200:
            split_path = path.split("/")
            session_name = split_path[-3].replace(" ", "_")
            house = split_path[-2]
            bill_name = split_path[-1].replace(" ", "_")
            filename = "_".join([state, session_name, house, bill_name])
            if "html" in file_type and ".html" not in filename:
                filename += ".html"
            if "pdf" in file_type and ".pdf" not in filename:
                filename += ".pdf"
            if "msword" in file_type and ".doc" not in filename:
                filename += ".doc"
            else:
                open(os.path.dirname(path) + "/" + filename, 'wb').write(r.content)
            return dict(state=state, url=url, path=path, filename=filename, id=id, success=True)
        else:
            return dict(state=state, url=url, path=path, filename=None, id=id, success=False)

    except (ConnectionError, requests.exceptions.ChunkedEncodingError, requests.exceptions.MissingSchema):
        return dict(state=state, url=url, path=path, filename=None, id=id, success=False)


def get_bill_text(args):
    path = args[0]
    state = args[1]
# def get_bill_text(path, state):
    print(path)
    with open(path) as file:
        try:
            bill_json = json.load(file)
        except (UnicodeDecodeError, json.decoder.JSONDecodeError):
            return dict(state=state, url=None, path=path, filename=None, id=None, success=False)
    version_list = bill_json.get('versions')
    id = bill_json.get('_id')
    # If there is more than one version, get the last one
    if len(version_list) > 0 and version_list[len(version_list)-1].get('url', None) is not None:
        url = version_list[len(version_list)-1].get('url')
        file_type = version_list[len(version_list) - 1].get("mimetype")

        if "ftp://" in url:
            return dict(state=state, url=url, path=path, filename=None, id=id, success=False)
            # with closing(request.urlopen(url)) as r:
            #     split_path = path.split("/")
            #     session_name = split_path[-3].replace(" ", "_")
            #     house = split_path[-2]
            #     bill_name = split_path[-1].replace(" ", "_")
            #     filename = "_".join([state, session_name, house, bill_name])
            #     if "htm" in file_type and ".html" not in filename:
            #         filename += ".htm"
            #     with open(filename, 'wb') as f:
            #         shutil.copyfileobj(r, f)
            #         return dict(state=state, url=None, path=path, filename=filename, id=id, success=True)
        else:

            # Alaska's links don't work anymore :(
            # New format http://www.akleg.gov/basis/Bill/Text/27?Hsid=HB0001C
            if state == "ak":
                # get the session and the bill name from the old url
                bill_name_re = re.compile("hsid=(.*)&")
                bill_name = bill_name_re.search(url).group(1)
                session_num_re = re.compile("session=(.*)")
                session_num = session_num_re.search(url).group(1)
                new_base_url = "http://www.akleg.gov/basis/Bill/Text/{0}?Hsid={1}".format(session_num, bill_name)
                url = new_base_url
                return make_request(path, state, url, file_type, id)
            # Smaller change in url for Alabama
            # old "http://alisondb.legislature.state.al.us/acas/searchableinstruments/2011rs/PrintFiles/HB1-int.pdf"
            # new "http://alisondb.legislature.state.al.us/ALISON/SearchableInstruments/2019rs/PrintFiles/HJR259-enr.pdf"
            if state == "al":
                # get the session and the bill name from the old url
                split_url = url.split("/")
                bill_name = url.split("/")[-1]
                session_num = url.split("/")[-3]
                new_base_url = "http://alisondb.legislature.state.al.us/ALISON/SearchableInstruments/{0}/PrintFiles/{1}".format(session_num, bill_name)
                url = new_base_url
                return make_request(path, state, url, file_type, id)
            # Delaware has a complete new system but I've scraped it to get the new ids and stuff and I think I can just
            # pull the data directly from their website
            if state == "de":
                pass
            # Moved to new website. Uses aspx :(
            # Using the open states scraper to get the updated websites
            if state == "ga":
                # need to scrape with open states
                # got the open states links
                pass
            # fl is ok
            # hi is ok
            # ia is ok
            # id is using wordpress now
            if state == "id":
                # old url "http://legislature.idaho.gov/legislation/2011/H0001.pdf"
                # new url "https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2011/legislation/H0001.pdf"
                # get the session and the bill name from the old url
                split_url = url.split("/")
                bill_name = url.split("/")[-1]
                session_num = url.split("/")[-2]
                new_base_url = "https://legislature.idaho.gov/wp-content/uploads/sessioninfo/{0}/legislation/{1}".format(
                    session_num, bill_name)
                url = new_base_url
                return make_request(path, state, url, file_type, id)
            if state == "il":
                url += "&print=True"
                return make_request(path, state, url, file_type, id)
            if state == "in":
                for version in version_list:
                    if "Enrolled Act" in version.get("name"):
                        url = version.get('url')
                        break
                return make_request(path, state, url, file_type, id)
            if state == "ks":
                for version in version_list:
                    if "Enrolled" in version.get("name"):
                        url = version.get('url')
                        break
                return make_request(path, state, url, file_type, id)
            if state == "ky":
                # old url "http://www.lrc.ky.gov/record/11RS/HB1/bill.doc"
                # new url "https://apps.legislature.ky.gov/record/12RS/hb1/bill.doc"
                # get the session and the bill name from the old url
                split_url = url.split("/")
                bill_name = url.split("/")[-2].lower()
                session_num = url.split("/")[-3]
                new_base_url = "https://apps.legislature.ky.gov/record/{0}/{1}/bill.doc".format(
                    session_num, bill_name)
                url = new_base_url
                return make_request(path, state, url, file_type, id)
            if state == "la":
                # need to scrape with open states
                # open states only has 2017-present data
                pass
            if state == "ma":
                term = bill_json.get('_term')
                bill_id = bill_json.get("bill_id").replace(" ","")
                new_url = "https://malegislature.gov/Bills/{0}/{1}.pdf".format(term, bill_id)
                url = new_url
                file_type = "pdf"
                return make_request(path, state, url, file_type, id)
            # md is ok
            # me is ok
            # mi is ok
            # mn is ok
            # mo is ok
            # ms is ok
            if state == "mt":
                # use open states
                pass
            # nc is ok
            # nd is ok
            # ne is ok
            # nh is ok
            # nj is ok
            # nm is ok
            # nv is ok
            # ok is ok (lol)
            if state == "or":
                # use open states
                # open states only has 2017-present
                pass
            # pa is ok
            # ri is ok
            # sc is ok
            if state == "sd":
                # use open states
                pass
            # tn is ok
            # tx is ok
            if state == "ut":
                # use open states
                pass
            # va is ok
            if state == "wa":
                # use open states
                pass
            # wi is ok
            if state == "wv":
                # use open states
                # only has data from 2016-present
                pass
            # wy is ok
            if state == "ny":
                # old url "http://open.nysenate.gov/legislation/bill/J14-2011"
                # new url "https://legislation.nysenate.gov/pdf/bills/2011/J14"
                split_url = url.split("/")
                if len(split_url[-1].split("-")) > 1:
                    bill_id, term = split_url[-1].split("-")
                    url = "https://legislation.nysenate.gov/pdf/bills/{0}/{1}".format(term, bill_id)
                else:
                    bill_id = split_url[-1]
                    term = split_url[-2]
                    url = "https://legislation.nysenate.gov/pdf/bills/{0}/{1}".format(term, bill_id)
                return make_request(path, state, url, file_type, id)
            if state == "oh":
                # use open states
                pass
            # colorado uses word perfect??
            # Use the version with final in name
            elif state == "co" and "octet-stream" in file_type:
                for version in version_list:
                    if "Final" in version.get("name"):
                        url = version.get('url')
                        break
                r = requests.get(url, allow_redirects=True)
                if r.status_code == 200:
                    try:
                        wpf_bytes = wpf.WordPerfect(r.content)
                        for i in wpf_bytes.elements:
                            if i.type == 1:
                                wpf_text = i.data
                                break
                        split_path = path.split("/")
                        session_name = split_path[-3].replace(" ", "_")
                        house = split_path[-2]
                        bill_name = split_path[-1].replace(" ", "_")
                        filename = "_".join([state, session_name, house, bill_name])
                        filename += ".txt"
                        open(os.path.dirname(path) + "/" + filename, 'wb').write(r.content)
                        return dict(state=state, url=url, path=path, filename=filename, id=id, success=True)
                    except IndexError:
                        return dict(state=state, url=url, path=path, filename=None, id=id, success=False)
            else:

                try:
                    r = requests.get(url, allow_redirects=True)
                    if r.status_code == 200:
                        split_path = path.split("/")
                        session_name = split_path[-3].replace(" ", "_")
                        house = split_path[-2]
                        bill_name = split_path[-1].replace(" ", "_")
                        filename = "_".join([state, session_name, house, bill_name])
                        if "html" in file_type and ".html" not in filename:
                            filename += ".html"
                        if "pdf" in file_type and ".pdf" not in filename:
                            filename += ".pdf"
                        if "msword" in file_type and ".doc" not in filename:
                            filename += ".doc"
                        else:
                            open(os.path.dirname(path) + "/" + filename, 'wb').write(r.content)
                        return dict(state=state, url=url, path=path, filename=filename, id=id, success=True)
                    else:
                        return dict(state=state, url=url, path=path, filename=None, id=id, success=False)

                except (ConnectionError, requests.exceptions.ChunkedEncodingError, requests.exceptions.MissingSchema):
                    return dict(state=state, url=url, path=path, filename=None, id=id, success=False)

    else:
        return dict(state=state, url=None, path=path, filename=None, id=id, success=False)


def state_func(args):
    state = args[0]
    completed_paths = args[1]
    single_state = args[2]

    rootdir = '/home/ubuntu/Downloads/open_state_data'
    state_path = rootdir + "/" + state + "/bills/" + state + "/"
    bill_paths = glob(state_path + "*/*/*", recursive=True)
    # bill_paths = [path for path in bill_paths if path not in completed_paths]
    bill_paths = list(set(bill_paths) - set(completed_paths))
    bill_paths = list(filter(
        lambda path: not path.endswith(".pdf") and not path.endswith(
            ".html") and not path.endswith(".htm"), bill_paths))
    # now go through each bill and pull the url to download the file and then we want to download it to the same
    # folder
    state_results = []
    fieldnames = ["state", "url", "path", "filename", "id", "success"]
    # with open("/Users/patrick/Downloads/open_states_data/diagnostic_results.csv", 'wb') as f:

    with open("/home/ubuntu/Downloads/open_state_data/diagnostic_results.csv", 'a') as f:
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
                    with open('/home/ubuntu/Downloads/finished_paths.txt', 'a') as fd:
                        writer = csv.writer(fd)
                        writer.writerow([result['path']])
            else:

                for path in bill_paths:
                    if not path.endswith(".pdf") and not path.endswith(".html") and not path.endswith(".htm"):
                        print(path)
                        res = get_bill_text(path, state)
                        # writer.writerows(chain.from_iterable(final_results))
                        if res is None:
                            res = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
                        # if set(res.keys()) != set(fieldnames):
                        #     res = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
                        dictwriter.writerow(res)
                        state_results.append(res)
                        with open('/home/ubuntu/Downloads/finished_paths.txt', 'a') as fd:
                            writer = csv.writer(fd)
                            writer.writerow([path])
    return state_results


# Go through the file paths and remove unnecessary session

if __name__ == '__main__':
    pool = mp.Pool(processes=mp.cpu_count())
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY",
              "LA", "ME", "MD", "MA", "MI",
              "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
              "SC", "SD", "TN", "TX", "UT",
              "VT", "VA", "WA", "WV", "WI", "WY"]
    states = [x.lower() for x in states]
    ignored_states = ["de", "ga", "la", "mt", "or", "sd", "ut", "wa", "wv", "oh"]
    states = [state for state in states if state not in ignored_states]
    states = ["ga", "oh"]
    rootdir = '/home/ubuntu/'
    # final_results = []
    single_state = True

    # finished_states = ["al", "ak"]
    finished_states = []

    # load list of completed paths
    completed_paths = []
    with open('/home/ubuntu/Downloads/finished_paths.txt') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            path = row[0]
            if not path.endswith(".pdf") and not path.endswith(".html") and not path.endswith(".htm"):
                completed_paths.append(path)

    # for state in states:
    #     if state not in finished_states:
    #         print(state)
    #         # get all of the files in the bill folder
    #         state_path = rootdir + "/" + state + "/bills/" + state + "/"
    #         bill_paths = glob(state_path + "*/*/*", recursive=True)
    #         # now go through each bill and pull the url to download the file and then we want to download it to the same
    #         # folder
    #         inputs = [(path, state) for path in bill_paths]
    #         print(inputs[0])
    #         state_results = list(pool.map(get_bill_text, inputs))
    #         # track_job(state_results)
    #         final_results.append(state_results)
    #         finished_states.append(state)

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
    with open("/home/ubuntu/Downloads/open_state_data/final_results.csv", 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chain.from_iterable(final_results))

