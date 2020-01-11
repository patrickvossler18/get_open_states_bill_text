import json

import requests

page = 1
total_data = []
initial_url = "https://legis.delaware.gov/json/AllLegislation/GetAllLegislation?sort=&page=1&pageSize=100&group=&filter=&selectedGA%5B0%5D=149&selectedGA%5B1%5D=148&selectedGA%5B2%5D=147&selectedGA%5B3%5D=146&sponsorName=&fromIntroDate=&toIntroDate=&coSponsorCheck=false"

initial_data = requests.post(initial_url)

response = json.loads(initial_data.text)
total = response.get("Total")
first_dat = response.get("Data")
total_data.append(first_dat)
amt_left = int((total-100)/100)

for page in range(2,amt_left):
    print(page)
    url = "https://legis.delaware.gov/json/AllLegislation/GetAllLegislation?sort=&page={0}&pageSize=100&group=&filter=&selectedGA%5B0%5D=149&selectedGA%5B1%5D=148&selectedGA%5B2%5D=147&selectedGA%5B3%5D=146&sponsorName=&fromIntroDate=&toIntroDate=&coSponsorCheck=false".format(
        page)
    post = requests.post(url)
    response = json.loads(post.text)
    data = response.get("Data")
    total_data.append(data)



fieldnames = list(first_dat[0].keys())

with open("/Users/patrick/Downloads/open_state_data/de_leg_id_info.csv", 'w') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for tot in total_data:
        for dat in tot:
            writer.writerow(dat)
    # writer.writerows(chain.from_iterable(total_data))


completed_list = []


for data in total_data:
    for dat in data:
        leg_id = str(dat.get("LegislationId"))
        if leg_id in completed_list:
            pass
        else:
            leg_type_id = str(dat.get("LegislationId"))
            leg_type_id = str(dat.get("LegislationTypeId"))
            leg_name = dat.get("LegislationNumber").replace(" ", "")
            assem_num = str(dat.get("AssemblyNumber"))
            gen_html_url = "https://legis.delaware.gov/json/BillDetail/GetHtmlDocument?fileAttachmentId={0}".format(leg_id)
            # gen_html_url = "https://legis.delaware.gov/json/BillDetail/GenerateHtmlDocument?legislationId={0}&legislationTypeId={1}&docTypeId=2&legislationName={2}".format(leg_id,leg_type_id,leg_name)
            post = requests.post(gen_html_url)
            if post.status_code == 200:
                filename = "_".join([assem_num, leg_id,leg_name])
                filename += ".html"
                open("/Users/patrick/Downloads/open_state_data/de_data" + "/" + filename, 'wb').write(post.content)
                print(filename)
                completed_list.append(leg_id)
            else:
                print("error for {0}".format("_".join([assem_num, leg_id,leg_name])))
