import requests

import itertools
import os
from glob import glob
from collections import namedtuple
import csv

from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen

import multiprocessing
from joblib import Parallel, delayed

import pandas as pd
#os.system("taskset -p 0xfff %d" % os.getpid())
# https://stackoverflow.com/questions/15639779/why-does-multiprocessing-use-only-a-single-core-after-i-import-numpy

# or: requests.get(url).content
    #res = requests.get(logFile)
logFileList = "./EDGAR_LogFileListShort.txt"

SecGovUrl = 'https://www.sec.gov/Archives'
FormIndexUrl = os.path.join(SecGovUrl, 'edgar', 'full-index', '{}', 'QTR{}', 'form.idx')
IndexRecord = namedtuple("IndexRecord", ["AccessionNumber","form_type"])

def DownloadFormIdx():

    # Download form.idx file from server
    # Uncomment only when not downloaded yet.
    """
    for year, qtr in itertools.product(range(1993, 2018), range(1, 5)):

        indexUrl = FormIndexUrl.format(year, qtr)
        try:
            print("request index - {}".format(indexUrl))
            res = requests.get(indexUrl)
            
            formIdxPath = "./Index/year{}_qtr{}.index".format(year, qtr)
            #form_idx_path = os.path.join(index_dir, form_idx)

            print("writing index to {}".format(formIdxPath))
            with open(formIdxPath, 'w') as fout:
                fout.write(res.text)
        except:
            print("download index failed - {}".format(indexUrl))
    #"""
    
    # Helper function
    def parse_row_to_record(row, fields_begin):
        record = []

        # for begin, end in zip(fields_begin[:], fields_begin[1:] + [len(row)]):
        
        field = row[fields_begin[2]:].rstrip()
        field = field.strip('\"')
        # get accession number part
        field = field.split("/")[3].rstrip(".txt")
        record.append(field)
        
        field = row[fields_begin[0]:fields_begin[1]].rstrip()
        field = field.strip('\"')
        record.append(field)

        return record

    # Extracting records from form.idx files

    records = []
    for idxFile in sorted(glob("./Index/*.index")):
        print("Extracting 10k records from index {}".format(idxFile))
    
        with open(idxFile, 'r') as fin:

            for row in fin.readlines():
                if row.startswith("Form Type"):
                    fields_begin = [row.find("Form Type"),
                                    row.find("Company Name"),
                                    #row.find('CIK'),
                                    #row.find('Date Filed'),
                                    row.find("File Name")]

                elif row.startswith("10-K ") or row.startswith("10-Q ") \
                or row.startswith("8-K ") or row.startswith("DEF 14A ") or row.startswith("S-1 "):
                    rec = parse_row_to_record(row, fields_begin)
                    records.append(IndexRecord(*rec))
    
    records.sort(key=lambda x: x[0])

    # For reference
    """
    with open('./Index/index.csv', 'w') as fout:
        writer = csv.writer(fout, delimiter=',', quotechar='\"', quoting=csv.QUOTE_ALL)
        for rec in records:
            writer.writerow(tuple(rec))
    """

    return [x[0] for x in records]

# global parameter
accessionNumbers = []
sp = pd.read_csv("./sp500_constituents.csv", delimiter=',', header=0, usecols=[4,5,6,7,12,13,15], dtype={'co_cik':'Int64'}, parse_dates = ['from', 'thru'], date_parser = pd.to_datetime)

def DownloadLogFile(logFile):
    print(logFile)
    #fileName = logFile[-17:-6]
    fileName = logFile.split("/")[-1].split(".")[-2]

    resp = urlopen("http://" + logFile)
    
    with ZipFile(BytesIO(resp.read())) as zipFile:
        
        # Read csv file into dataframe
        df = pd.read_csv(zipFile.open(fileName + ".csv"), delimiter=',', header=0, usecols=[0,4,5,13]).astype({'cik':'int64'})

    df.drop(df[df['crawler']==1].index, inplace=True)
    df.drop(columns='crawler', inplace=True)
    df.sort_values(by='ip', kind='mergesort', inplace=True)
    #df.to_csv(path_or_buf='./df7.csv')

    # step 1&2: #CIK > 1 && #CIK <= 50
    print("Step 1&2: Filtering #CIK > 1 && #CIK <=50")

    #print(df.loc[(df.groupby('ip', 'cik').count()>1).sum(axis=1),:])
    #print(df.set_index(['ip', 'cik'])) # .groupby(level=1).count())

    distinctCount = df.drop_duplicates(subset=['ip', 'cik'])['ip'].value_counts()
    #distinctCount.to_csv(path_or_buf='./df2.csv')
    
    filtered = df[df['ip'].isin(distinctCount[distinctCount <= 50].index)]
    filtered = filtered[filtered['ip'].isin(distinctCount[distinctCount > 1].index)]
    #filtered.to_csv(path_or_buf='./df3.csv')
    #filtered.drop_duplicates(subset=['ip', 'cik'])['ip'].value_counts().to_csv(path_or_buf='./df4.csv')

    # step 3: only 10-k, 10-Q, 8-K, DEF 14A, S-1 (Use accessionNumbers derived from form.idx)
    print("step 3: Filter out only 10-k, 10-Q, 8-K, DEF 14A, S-1 records.")
    
    global accessionNumbers
    filtered = filtered[filtered['accession'].isin(accessionNumbers)].drop(columns='accession')
    #filtered.to_csv(path_or_buf='./df5.csv')

    # step 4A: distinct A->A, ---> A
    
    prevCik = 0
    prevIp = ""
    indexToRemove = []
    for row in filtered.itertuples():
        if getattr(row, "cik") == prevCik and getattr(row, "ip") == prevIp:
            indexToRemove.append(getattr(row, "Index"))
                        
        prevCik = getattr(row, "cik")
        prevIp = getattr(row, "ip")
    
    filtered.drop(index = indexToRemove, inplace = True)
    #filtered.to_csv(path_or_buf='./df8.csv')
    
    """
    # step 4B: distinct A->B, A->B ---> A->B
    
    prevCik = 0
    prevIp = ""
    cikChain = {}
    indexToRemove = []
    for row in filtered.itertuples():
        cik = getattr(row, "cik")
        ip = getattr(row, "ip")
        
        if prevIp != ip:
            # Initialize set
            cikChain = {}

        else:
            if (prevCik, cik) in cikChain:
                indexToRemove.append(getattr(row, "Index"))
            else:
                cikChain.add((prevCik, cik))                
        
        prevCik = cik
        prevIp = ip
    #filtered.drop(index = indexToRemove, inplace = True)
    #filtered.to_csv(path_or_buf='./df9.csv')
    """

    # Add S&P500 S&P1500 flag
    print("Setting S&P flags")
    spDate = sp.drop(sp[(sp['from'] > pd.Timestamp(fileName[3:11])) | (sp['thru'] < pd.Timestamp(fileName[3:11]))].index)
    #spDate.to_csv(path_or_buf = './sp2.csv')
    
    SP1500CIKs = set()
    SP500CIKs = set()
    
    for row in spDate.itertuples():
        co_cik = getattr(row, 'co_cik') 
        if co_cik == '':
            continue
        conm = getattr(row, 'conm')
        
        if conm == 'S&P 1500 Super Composite':
            SP1500CIKs.add(co_cik)

        elif conm == 'S&P 500 Comp-Ltd':
            SP500CIKs.add(co_cik)
    
    SP500 = []
    SP1500 = []
    
    for row in filtered.itertuples():
        cik = getattr(row, 'cik') 
        if cik in SP1500CIKs:
            SP1500.append(1)
        else:
            SP1500.append(0)
        
        if cik in SP500CIKs:
            SP500.append(1)
        else:
            SP500.append(0)

    filtered.insert(2, "S&P500", SP500, True)
    filtered.insert(3, "S&P1500", SP1500, True)

    # Save daily file
    
    if not os.path.exists("./MyFilteredData/" + fileName[3:7]):
       os.makedirs("./MyFilteredData/" + fileName[3:7])
    filtered.to_csv(path_or_buf = "./MyFilteredData/" + fileName[3:7] + "/" + fileName + ".dat")


if __name__ == "__main__":

    logFiles = []
    with open(logFileList, 'r') as fin:
        for line in fin:
            logFiles.append(line)

    # Load S&P 500 list
    #sp = pd.read_csv("./sp500_constituents.csv", delimiter=',', header=0, usecols=[4,5,6,7,12,13,15], dtype={'co_cik':'Int64'})
    sp.drop(sp[(sp['conm']!='S&P 1500 Super Composite') & (sp['conm']!='S&P 500 Comp-Ltd')].index, inplace=True)
    #sp.to_csv(path_or_buf = './sp1.csv')
    #sp.drop_duplicates(subset=['conm'])['conm'].to_csv(path_or_buf = './dfsp.csv')


    # Load form.idx
    accessionNumbers = DownloadFormIdx()

    #os.system('taskset -cp 0-%d %s' % (num_cores, os.getpid()))
    Parallel(n_jobs=-4)(delayed(DownloadLogFile)(logFile) for logFile in logFiles)
