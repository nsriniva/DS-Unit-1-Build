import requests
from bs4 import BeautifulSoup
from csv import reader, DictReader, DictWriter
from collections import OrderedDict


dcm_keys = ['misc', 'lifestyle', 'entertainment','bus', 'socmed', 'tech',\
    'world', 'culture', 'u.s.', 'social-good']

dcm_vals_next = len(dcm_keys)

dcm = OrderedDict()

for idx,key in enumerate(dcm_keys):
    dcm[key] = idx
    
def get_data_channel(url):
    global dcm_vals_next
    hgroup = BeautifulSoup(requests.get(url).content,'html.parser').find('hgroup')
    ret = 0
    if hgroup is not None:
        dc = hgroup['data-channel']
        if dc not in dcm_keys:
            dcm[dc] = dcm_vals_next
            dcm_keys.append(dc)
            dcm_vals_next += 1
        ret = dcm[dc]
    return ret

start = 7984
with open('data_channel_df.csv','r') as ifile:
    df = DictReader(ifile)
    with open(f'data_channel_cleaned_{start}_df.csv','w') as ofile:
        out_df = DictWriter(ofile,fieldnames=df.fieldnames)
        out_df.writeheader()
        for row in df:
            if int(row['']) < start:
                print(f"Ignoring row<{row['']}>")
                continue
            if row['data_channel'] == '0':
                dc = get_data_channel(row['url'])
                row['data_channel'] = str(dc)
                row['data_channel_name'] = dcm_keys[dc]
                print(f"row<{row['']}>")
                out_df.writerow(row)

