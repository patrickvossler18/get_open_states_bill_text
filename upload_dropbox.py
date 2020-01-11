import csv
import re
import subprocess
from functools import partial
from glob import glob
from multiprocessing.dummy import Pool
from subprocess import PIPE, STDOUT
from subprocess import call


def dropbox_upload(path):
    split_path = path.split("/")
    if not " " in split_path[-1] and not bool(re.search("[A-Z]{2,4}?\s?[0-9]+$", path)):
        # print(path)
        final_path = "/".join(split_path[4::][:-1]).replace(" ", "\\ ")
        path = path.replace(" ", "\\ ")
        return "sudo bash /home/ubuntu/Dropbox-Uploader/dropbox_uploader.sh upload -s " + \
               path + " Bill\ datasets\ fr\ openstates\ 2014_2017/" + final_path + "/"


def check_for_missing_files(state_path, bill_paths):
    uploaded_files = []
    sub_dir = glob(state_path + "*/")
    sub_dir_names = [dir.split("/")[-2].replace(" ", "\\ ") for dir in sub_dir]
    for name in sub_dir_names:
        print("getting uploaded files for {0}".format(name))
        for house in ['lower', 'upper']:
            process = subprocess.Popen(
                "sudo bash /home/ubuntu/Dropbox-Uploader/dropbox_uploader.sh list  Bill\ datasets\ fr\ openstates\ 2014_2017/" +
                "open_state_data/" + state + "/bills/" + state + "/" + name + "/" + house + "/",
                shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
            stdout, stderr = process.communicate()
            data = stdout.decode('ascii').splitlines()[1::]
            reader = csv.DictReader(data,
                                    delimiter=' ', skipinitialspace=True,
                                    fieldnames=['file_type', 'size', 'path'])
            for row in reader:
                uploaded_files.append(state_path + name + "/" + house + "/" + row.get('path'))
        uploaded_files = list(filter(
            lambda path: (path.endswith(".pdf") or path.endswith(
                ".html") or path.endswith(".htm") or path.endswith(".doc") or path.endswith(".txt")) and
                         (not bool(re.search('[A-Z]+ [0-9]$', path.strip()))) and path not in finished_path_upload,
            uploaded_files))
    space = False
    for dir in sub_dir:
        if " " in dir:
          space = True
          break
    if space:
        uploaded_files = [upload.replace("\\ ", " ") for upload in uploaded_files]
    remaining_files = set(bill_paths) - set(uploaded_files)
    return remaining_files


finished_states = ['ak', 'ar', "ca"]
# with open("finished_state_upload.txt", "r") as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=',')
#     for row in csv_reader:
#         finished_states.append(row[0])

finished_path_upload = []
with open("finished_path_upload.txt", "r") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        finished_states.append(row[0])

# pool = mp.Pool(processes=mp.cpu_count())
rootdir = '/home/ubuntu/Downloads/open_state_data'
# states = ['ak', 'az', 'ar', 'ca', 'co', 'ct', 'fl', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'me', 'md', 'ma', 'mi',
#           'mn', 'ms', 'mo', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'ok', 'pa', 'ri', 'sc', 'tn', 'tx', 'vt',
#           'va', 'wi', 'wy']

states = ['tx','ct']

states = [state for state in states if state not in finished_states]

for state in states:
    print(state)
    state_path = rootdir + "/" + state + "/bills/" + state + "/"
    bill_paths = glob(state_path + "*/*/*", recursive=True)
    bill_paths = list(filter(
        lambda path: (path.endswith(".pdf") or path.endswith(
            ".html") or path.endswith(".htm") or path.endswith(".doc") or path.endswith(".txt")) and
                     (not bool(re.search('[A-Z]+ [0-9]$', path.strip()))) and path not in finished_path_upload,
        bill_paths))

    remaining_files = check_for_missing_files(state_path, bill_paths)

    print("{0} remaining files for {1}".format(len(remaining_files), state))

    if len(remaining_files) > 0:
        commands = [dropbox_upload(path) for path in remaining_files]

        pool = Pool(4)  # go fast the first time
        for i, returncode in enumerate(pool.imap(partial(call, shell=True, stdin=PIPE), commands)):
            if returncode != 0:
                print("%d command failed: %d" % (i, returncode))

        print("starting second check for missing files")
        still_remaining_files = check_for_missing_files(state_path, bill_paths)

        print("{0} remaining files for {1}".format(len(still_remaining_files), state))
        # pool.map(dropbox_upload, bill_paths)

        if len(still_remaining_files) > 0:
            commands = [dropbox_upload(path) for path in still_remaining_files]

            pool = Pool(4)  # go slower to be safe
            for i, returncode in enumerate(pool.imap(partial(call, shell=True), commands)):
                if returncode != 0:
                    print("%d command failed: %d" % (i, returncode))

    # with open("finished_state_upload.txt", "a") as fd:
    #     writer = csv.writer(fd)
    #     writer.writerow([state])



