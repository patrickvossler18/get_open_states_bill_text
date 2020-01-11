import json
import json
import multiprocessing as mp
from glob import glob

connection_timeout = 30  # seconds


def get_vote_info(args):
    path = args[0]
    state = args[1]
    # read in the json data
    print(path)
    with open(path) as file:
        try:
            bill_json = json.load(file)
        except (UnicodeDecodeError, json.decoder.JSONDecodeError):
            print("oh no")
            return dict(state=state, path=path, id=None, uuid=None, passage=None, success=False)

    # We want to see if signed is not null.
    action_dates = bill_json.get("action_dates")
    signed = action_dates.get("signed")
    id = bill_json.get('_id')
    # If it is signed into law we want the bill_id and vote information
    if signed is not None:

        votes = bill_json.get("votes")
        if len(votes) > 0:
            session = votes[0].get("session")
            bill_id = bill_json.get("bill_id")
            # create uuid
            uuid = "_".join([state, str(session).replace(" ", ""), bill_id.replace(" ", "")])
            return dict(state=state, path=path, id=id, uuid=uuid, passage=True, success=True)
        else:
            # see if we can get the session from within the bill json
            if bill_json.get("session", None) is not None:
                session = bill_json.get("session", None)
                bill_id = bill_json.get("bill_id")
                uuid = "_".join([state, str(session).replace(" ", ""), bill_id.replace(" ", "")])
                return dict(state=state, path=path, id=id, uuid=uuid, passage=True, success=True)
            else:
                # situation where we somehow have signed the bill but don't have information about the votes?
                # and where we can't get the session from the bill_json itself
                return dict(state=state, path=path, id=id, uuid=None, passage=True, success=False)
    else:
        return dict(state=state, path=path, id=id, uuid=None, passage=False, success=False)


def state_func(args):
    state = args[0]
    completed_paths = args[1]
    single_state = args[2]

    rootdir = '/Users/patrick/Downloads/open_state_data'
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
    fieldnames = ["state", "path", "id", "uuid", "passage", "success"]
    with open("/Users/patrick/Downloads/open_state_data/get_vote_data_results.csv", 'a') as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        # dictwriter.writeheader()
        if len(bill_paths) > 0:
            if single_state:
                pool = mp.Pool(processes=mp.cpu_count())
                inputs = [(path, state) for path in bill_paths]
                state_results = list(pool.map(get_vote_info, inputs))
                for result in state_results:
                    if result is None:
                        print("{0} has file that is returning None??".format(state))
                        break
                    # if set(res.keys()) != set(fieldnames):
                    #     res = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
                    dictwriter.writerow(result)
                    with open('/Users/patrick/Downloads/finished_vote_paths.txt', 'a') as fd:
                        writer = csv.writer(fd)
                        writer.writerow([result['path']])
            else:

                for path in bill_paths:
                    if not path.endswith(".pdf") and not path.endswith(".html") and not path.endswith(".htm"):
                        print(path)
                        res = get_vote_info([path, state])
                        # writer.writerows(chain.from_iterable(final_results))
                        if res is None:
                            res = dict(state=state, path=path, id=None, uuid=None, passage=False, success=False)
                        # if set(res.keys()) != set(fieldnames):
                        #     res = dict(state=state, url=None, path=path, filename=None, id=None, success=False)
                        dictwriter.writerow(res)
                        state_results.append(res)
                        with open('/Users/patrick/Downloads/finished_vote_paths.txt', 'a') as fd:
                            writer = csv.writer(fd)
                            writer.writerow([path])
    return state_results


if __name__ == '__main__':
    pool = mp.Pool(processes=mp.cpu_count())
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY",
              "LA", "ME", "MD", "MA", "MI",
              "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
              "SC", "SD", "TN", "TX", "UT",
              "VT", "VA", "WA", "WV", "WI", "WY"]
    states = [x.lower() for x in states]
    ignored_states = ["de", "la", "mt", "or", "sd", "ut", "wa", "wv", "oh"]
    states = [state for state in states if state not in ignored_states]
    rootdir = '/Users/patrick/Downloads/open_state_data'
    # final_results = []
    single_state = False

    # finished_states = ["al", "ak"]
    finished_states = []

    # load list of completed paths
    completed_paths = []
    with open('/Users/patrick/Downloads/finished_vote_paths.txt') as csv_file:
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
    # fieldnames = ["state", "path", "uuid", "passage", "success"]
    # # with open("/Users/patrick/Downloads/open_states_data/diagnostic_results.csv", 'wb') as f:
    # with open("/home/ubuntu/Downloads/open_state_data/final_results.csv", 'w') as f:
    #     writer = csv.DictWriter(f, fieldnames=fieldnames)
    #     writer.writeheader()
    #     writer.writerows(chain.from_iterable(final_results))






