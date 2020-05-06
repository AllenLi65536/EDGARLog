"""
Usage: 
    ProcessingFilteredData.py <year> (--SP500 | --SP1500 | --all)

"""

import pandas as pd
from glob import glob
from docopt import docopt
import json

if __name__ == "__main__":
    
    opt = docopt(__doc__)
    print(opt)
    
    year = opt["<year>"]

    # Dictionary of dictionary to save occurrance of chain
    cikAccumsDict = {}
        
    for dailyFile in sorted(glob("./MyFilteredData/" + year + "/*.dat")):
        #dailyFile = "./MyFilteredData/2003/log20031111.dat"
        
        print("Extracting filtered data {}".format(dailyFile))
        
        # Read in file and filter by S&P500/S&P1500
        df = pd.read_csv(dailyFile, delimiter=',', header=0, usecols=[1, 2, 3, 4]).astype({'cik':'int64'})
           
        if opt["--SP500"]:
            df.drop(df[df['S&P500']==0].index, inplace=True)
        
        if opt["--SP1500"]:
            df.drop(df[df['S&P1500']==0].index, inplace=True)
        
        df.drop(columns='S&P500', inplace=True)
        df.drop(columns='S&P1500', inplace=True)
        
        #df.to_csv(path_or_buf='./Processed1.csv')
        
        # Filter step 4A: distinct A->A, ---> A

        prevCik = 0
        prevIp = ""
        indexToRemove = []
        for row in df.itertuples():
            if getattr(row, "cik") == prevCik and getattr(row, "ip") == prevIp:
                indexToRemove.append(getattr(row, "Index"))
                            
            prevCik = getattr(row, "cik")
            prevIp = getattr(row, "ip")
        
        df.drop(index = indexToRemove, inplace = True)

        # Filter step 4B: distinct A->B, A->B ---> A->B

        prevCik = 0
        prevIp = ""
        cikChains = set()
        indexToRemove = []
        for row in df.itertuples():
            cik = getattr(row, "cik")
            ip = getattr(row, "ip")
            
            if prevIp != ip:
                # Initialize set
                cikChains = set()

            else:
                if (prevCik, cik) in cikChains:
                    indexToRemove.append(getattr(row, "Index"))
                else:
                    cikChains.add((prevCik, cik))                
            
            prevCik = cik
            prevIp = ip
        df.drop(index = indexToRemove, inplace = True)
        #df.to_csv(path_or_buf='./Processed2.csv')

        # Calculate accumulate 

        prevCik = 0
        prevIp = ""
        
        for row in df.itertuples():
            cik = getattr(row, "cik")
            ip = getattr(row, "ip")

            if prevIp != ip:
                prevCik = cik
                prevIp = ip
                continue
            
            if prevCik in cikAccumsDict:
                if cik in cikAccumsDict[prevCik]:
                    cikAccumsDict[prevCik][cik] += 1
                else:
                    cikAccumsDict[prevCik][cik] = 1
            else:
                cikAccumsDict[prevCik] = {}
                cikAccumsDict[prevCik][cik] = 1
            
            prevCik = cik
            prevIp = ip

    # Convert dictionary of dictionary to dictionary of sorted list
    cikAccumList = {}
 
    for cikDict in cikAccumsDict:
        cikAccumList[cikDict] = sorted(cikAccumsDict[cikDict].items(), key=lambda kv: kv[1], reverse=True)
        sumOver = sum(p[1] for p in cikAccumList[cikDict])
        cikAccumList[cikDict] = [(x[0], x[1]/sumOver) for x in cikAccumList[cikDict]]


    # write result to a file
    json = json.dumps(cikAccumList)
    
    if opt["--SP500"]:
        f = open("./Result/dict"+year+"SP500.json","w")
    if opt["--SP1500"]:
        f = open("./Result/dict"+year+"SP1500.json","w")
    if opt["--all"]:
        f = open("./Result/dict"+year+"all.json","w")
    
    f.write(json)
    f.close()

