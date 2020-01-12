import csv
import os
import re
import subprocess

import lxml
import lxml.html.clean
# from pathos.multiprocessing import ProcessingPool as Pool
from p_tqdm import p_map

# from sklearn.feature_extraction.text import CountVectorizer
# from tika import parser


# The game plan is to go through all of the files in the downloaded open states data set and find ANY bill text files
# that contain immigra*.

# We're going to need to read .txt, .html, .pdf, .doc

# 1/2/2020: The plan for this second go-around is to look at Latinx legislators and use a larger dictionary. Instead
# of just looking for 'immigra' we're going to have more search terms Depending on the structure of dictionary we can
# do something like: https://stackoverflow.com/a/34797488 But my guess is that this will be a simple list of stems in
# which case I can slightly modify the code below by just concatenating the search terms into one long regular
# expression and just compile it once. Ideally this will be faster than looping through all of the search terms.


tokens = {
    'criminal justice' : ['criminal justice reform','juvenile justice reform', 'criminal justice'],

    'healthcare' : ['healthcare', 'mental health', 'public health'],

    'education': ['early childhood education', 'higher education', 'education'],

    'immigration' : ['immigration', 'immigr'],

    "worker's rights" : ["worker's rights"],

    "minimum wage" : ["minimum wage"],

    'economic issues related to employment' : ['economic security', 'jobs', 'economic development'],

    'housing' : ['housing', 'homeless'],

    'transportation' : ['transportation'],

    'human trafficking' : ['human trafficking'],

    'reproductive health' : ['reproductive health'],

    'poverty' : ['poverty'],

    'childcare' : ['childcare'],

    'marijuana/cannabis' : ['marijuana', 'cannabis'],

    'environment' : ['environment'],

    'safety' : ['safety'],

    'early childhood' : ['early childhood']
}


def create_macro_re(tokens, flags=0):
    """
    Given a dict in which keys are token names and values are lists
    of strings that signify the token, return a macro re that encodes
    the entire set of tokens.
    """
    d = {}
    for token, vals in tokens.items():
        token = token.replace(" ", "").replace("'", "").replace("/", "")
        d[token] = '(?P<{}>{})'.format(token, '|'.join(vals))
    combined = '|'.join(d.values())
    return re.compile(combined, flags)


def find_tokens(macro_re, s):
    """
    Given a macro re constructed by `create_macro_re()` and a string,
    return a list of tuples giving the token name and actual string matched
    against the token.
    """
    found = []
    for match in re.finditer(macro_re, s):
        for key, value in match.groupdict().items():
            if value is not None:
                # if we have a match let's get the context in the string
                match_location = match.span()
                buffer_area = 100 + (match_location[1] - match_location[0])
                lower_bound = match_location[0] - buffer_area
                if (match_location[0] - buffer_area) < 0:
                    lower_bound = 0
                if match_location[1] + buffer_area > len(s):
                    upper_bound = len(s)
                else:
                    upper_bound = match_location[1] + buffer_area
                context = s[lower_bound:upper_bound]
                found.append((key, value, match_location, context))
        # found.append([(t, v) for t, v in match.groupdict().items() if v is not None][0])
    return found


compiled_regex = create_macro_re(tokens)
RESULTS_FILE_NAME = "/home/ubuntu/Downloads/search_dictionary_results_1_12_20.csv"


def pdfparser(file):
    if "/de/" not in file:
        if os.path.exists(file):
            uuid = os.path.splitext(file)[0]
        # this redirects pdftotext to stdout instead of to a file so we don't have to read and delete files each time
        proc = subprocess.Popen(["pdftotext", "-q", "{0}".format(file), "-"], stdout=subprocess.PIPE)
        text = proc.stdout.read().decode('utf-8')
        return text
    else:
        return None


def cleanme(content):
    cleaner = lxml.html.clean.Cleaner(
        allow_tags=[''],
        remove_unknown_tags=False,
        style=True,
    )
    html = lxml.html.document_fromstring(content)
    html_clean = cleaner.clean_html(html)
    return html_clean.text_content().strip()


def htmlparser(file):
    if os.path.exists(file):
        # just read the string, less overhead hopefully
        html = open(file, encoding="utf8", errors='ignore').read()
        # soup = BeautifulSoup(html,features="lxml")
        # text = cleanme(html)
        text = html
        return text
    else:
        return None


def txtparser(file):
    if os.path.exists(file):
        text = open(file, encoding="utf8", errors='ignore').read()
        return text
    else:
        return None


def docparser(file):
    if os.path.exists(file):
        text = subprocess.run(["antiword", "-t", "{0}".format(file)], stdout=subprocess.PIPE).stdout.decode('utf-8')
        return text
    else:
        return None


def find_word(text):
    if compiled_regex.search(text) is not None:
        return True
    else:
        return False


def check_text(file):
    text = None
    if file.endswith(".pdf"):
        text = pdfparser(file)

    if file.endswith(".doc"):
        text = docparser(file)

    if file.endswith(".html"):
        text = htmlparser(file)

    if file.endswith(".txt"):
        text = txtparser(file)
    if text is not None:
        text = pre_process(text)
        results = find_tokens(compiled_regex,text)
        if len(results) > 0:
            # print(file)
            # get the uuid
            uuid = os.path.splitext(os.path.basename(file))[0]
            if 'missing_legislators' in file:
                folder_name = os.path.basename(os.path.dirname(file))
                uuid = folder_name+ "_" + uuid.strip(" ")

            # result = os.path.splitext(os.path.basename(file))[0]
            # with open(RESULTS_FILE_NAME, 'a+') as csv_file:
            #     writer = csv.writer(csv_file, delimiter=',')
            #     writer.writerow([result])
            return [uuid, results]
        else:
            return None
    else:
        return None

# We now want to go through each state and look at files from 2014-2017. But we can't easily get years because of the
# because of the term versus year variation. Instead we'll just go through all of the files and if we get a match
# pull the uuid from the file name and then we can filter through them afterwards.


def pre_process(text):
    # lowercase
    text = text.lower()
    #remove tags
    text = re.sub("</?.*?>","",text)
    # remove special characters and digits
    # text = re.sub("(\\d|\\W)+"," ",text)
    return text


if __name__ == '__main__':
    # read the finished files and ignore the id if it has been processed already
    finished_files = []
    if os.path.exists(RESULTS_FILE_NAME):
        with open(RESULTS_FILE_NAME, "r") as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                finished_files.append(row)

    root_path = "/home/ubuntu/Downloads/open_state_data/"
    all_files = []
    immigrant_states = ["ca", "il", "tx", "mi", "ma", "or", "nm", "ny", "ks", "wi", "md", "wy", "fl", "wa", "pa", "az",
                        "nj", "ut", "ct", "nv", "co", "ri", "in", "ga", "nh", "oh", "mo", "va","hi", "me", "vt", "de",
                        'nc', "ak", "sd", "ms", "tn", "ok", "wv", "mn", "ar", "ky", "ne", "la"]
    print('getting file paths')
    for dirpath, dirnames, filenames in os.walk(root_path):
        for filename in [f for f in filenames if f.endswith(".pdf") or f.endswith(".txt") or f.endswith(".doc") or f.endswith(".html")]:
            # we don't want pdfs from Delaware AND we don't want it if we've already processed it
            # if (not bool("/de/" in filename)) and (os.path.splitext(os.path.basename(filename))[0] not in finished_files):
            if (not bool("/de/" in filename)) and (any(substring in filename for substring in immigrant_states)):
                all_files.append(os.path.join(dirpath, filename))

    # remove the first N files that have already been processed
    # N = 373431
    # all_files = all_files[N+1:]
    # pool = Pool(processes=3)
    # match_results = list(pool.map(check_text, all_files))
    match_results = list(p_map(check_text, all_files))

    # the missing data files are a bit more complicated

    with open(RESULTS_FILE_NAME, 'a+') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for lines in match_results:
            if lines is not None:
                uuid = lines[0]
                for line in lines[1]:
                    writer.writerow([uuid] + list(line))

