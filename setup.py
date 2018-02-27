"""
Setup scripts for downloading AS data
and pre-processing for analysis
"""

import data_utils as dutil


if __name__ == '__main__':
    # print("Downloading data from AWS")
    # download_data()

    print('Processing data')
    dutil.process_data()
