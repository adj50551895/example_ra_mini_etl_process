import requests
import json
import time
import glob
import os
import pandas as pd
import numpy as np
import shutil
import unicodedata


def export_fullstory_segment_date(token, segmentId, timeRange_start, timeRange_end, segmentTimeRange_start, segmentTimeRange_end, fullstory_file_name, fullstory_file_path):
    fullstory_token = token

    output_path = fullstory_file_path + fullstory_file_name
    
    timeRange_start = timeRange_start+"T00:00:00Z"
    timeRange_end = timeRange_end+"T23:59:59Z"

    segmentTimeRange_start = segmentTimeRange_start+"T00:00:00Z"
    segmentTimeRange_end = segmentTimeRange_end+"T23:59:59Z"

    print("segmentTimeRange_start: ", segmentTimeRange_start)
    print("segmentTimeRange_end: ", segmentTimeRange_end)
    print("timeRange_start: ", timeRange_start)
    print("timeRange_end: ", timeRange_end)

    # ## Part 1, get operationId
    url = "https://api.fullstory.com/segments/v1/exports"

    payload = json.dumps({
        "segmentId": segmentId,
        "type": "TYPE_INDIVIDUAL",
        "format": "FORMAT_CSV",
        "timeRange": {
            "start": timeRange_start,
            "end": timeRange_end
         },
         "segmentTimeRange": {
             "start": segmentTimeRange_start,
             "end":segmentTimeRange_end
         }
    })
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": "Basic "+fullstory_token
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    data_1 = response.json()


    # ## Part 2, get searchExportId
    while True:
        response = requests.get('https://api.fullstory.com/operations/v1/'+data_1["operationId"], headers=headers)
        data_2 = response.json()
        if data_2["state"] == "COMPLETED":
            break
        time.sleep(1)  # Wait for 1 second before checking again


    # ## Part 3, get URL - file location path
    response = requests.get('https://api.fullstory.com/search/v1/exports/'+data_2["results"]["searchExportId"]+'/results', headers=headers)
    data_3 = response.json()


    # ## Part 4, download csv.gz file from URL
    response = requests.get(data_3["location"])
    url = data_3["location"]

    # Send a GET request to download the file
    response = requests.get(url)
    if response.status_code == 200:
        # Save the file content to disk
        with open(output_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully at {output_path}")
    else:
        print(f"Download failed with status code: {response.status_code}")


def import_fullstory_data_from_file(path_):
    gz_files = glob.glob(os.path.join(path_, "*.gz"))
    gz_files_incr = []
    for fl in gz_files:
        gz_files_incr.append(fl)

    df_ = pd.DataFrame()
    if len(gz_files_incr) != 0:
        for f in gz_files_incr:
            print(f)
            df_tmp = pd.read_csv(f, compression='gzip', header=0, sep=',', quotechar='"', low_memory=False, encoding='utf8') #files are UTF-8 encodings
            df_ = pd.concat([df_, df_tmp])
            df_ = df_.fillna(np.nan).replace([np.nan], [None])
            #df_ = df_.replace('{}', None)
            del df_tmp
    return df_


def moveFullstoryFilesToimported(filePath_):
    gz_files = glob.glob(os.path.join(filePath_, "*.gz"))
    destination_folder = os.path.join(filePath_, "imported")
    
    for fl in gz_files:

        destination_file = os.path.join(destination_folder, os.path.basename(fl))
        if os.path.exists(destination_file):
            os.remove(destination_file)

        shutil.move(fl, destination_folder)
    print("File(s) moved to imported folder")



def convert_math_bold_to_utf8(text):
   return unicodedata.normalize('NFKC', text)
