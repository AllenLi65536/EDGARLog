#!/bin/bash
# bash run.sh 2007
# $1 = 2007

# Download lod file and preprocess it (step 2 - 4)
# python3 DownloadEDGARLog.py

# Aggregate yearly result (step 5)
#for i in 2008 2009 2010 2011 2012 2013
for i in 2015 2016
do
    python3 ProcessFilteredData.py $i --SP500
    python3 ProcessFilteredData.py $i --SP1500
    python3 ProcessFilteredData.py $i --all
done
