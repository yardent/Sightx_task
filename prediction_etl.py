import json
import os
from google_drive_client import Google_Drive_Client
from bq_client import BQ_Client
import argparse
import pandas as pd
from termcolor import cprint



parser = argparse.ArgumentParser(description='Read Predictions')
parser.add_argument('--file_id', required=True)
args = parser.parse_args()


#TODO - move to a new file
def read_json(file):
    try:
        with open(file, "rb") as file:
            return json.load(file)
    except IOError as err:
        if err.errno == 2:
            return {}
        else:
            raise

def get_file_content(prediction_file):
    data = pd.read_csv(prediction_file[0], sep="\n", header=None)

    return data

def organize_predictions(prediction):
    predictions = pd.DataFrame()
    predictions["file"] = pd.Series([], dtype=str)
    predictions["frame"] = pd.Series([], dtype=str)
    predictions["class"] = pd.Series([], dtype=int)
    predictions["score"] = pd.Series([], dtype=float)
    predictions["centerX"] = pd.Series([], dtype=float)
    predictions["centerY"] = pd.Series([], dtype=float)
    predictions["width"] = pd.Series([], dtype=float)
    predictions["height"] = pd.Series([], dtype=float)

    cprint('Load Predictions', 'blue', attrs=['bold', 'underline'], end='\n')
    for row in prediction.iterrows():
        predict_list = row[1][0].split()
        file_frame = predict_list[0].replace('/home/user/data/SOI/preprocessed_data/frames/', "")
        prediction_dict = {}
        prediction_dict["file"] = file_frame.split("_framenum_", 1)[0]
        prediction_dict["frame"] = file_frame.split("_framenum_", 1)[1].split('.')[0]

        #remove header
        predict_list.pop(0)
        for bb in predict_list:
            bb_list = bb.split(',')
            prediction_dict["class"] = bb_list[0]
            prediction_dict["score"] = bb_list[1]
            prediction_dict["centerX"] = bb_list[2]
            prediction_dict["centerY"] = bb_list[3]
            prediction_dict["width"] = bb_list[4]
            prediction_dict["height"] = bb_list[5]
            predictions = predictions.append(prediction_dict, ignore_index=True)

    return predictions

def main(argv=None):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    file_type = "text"
    config = read_json(root_dir + "/configurations.json")
    service_account_location = config['service_account']
    cprint('Run Prediction ', 'green', attrs=['bold', 'underline'], end='\n')
    print('Folder ID - ', args.file_id)
    drive_client = Google_Drive_Client(service_account_location)
    prediction_file = drive_client.get_all_files(args.file_id, file_type)
    prediction = get_file_content(prediction_file)
    prediction_df = organize_predictions(prediction)
    bq_client= BQ_Client(service_account_location,'sightx-project')
    cprint('Load To Prediction to BQ', 'yellow', attrs=['bold', 'underline'], end='\n')
    bq_client.load_df(prediction_df, 'Raw_Data.Prediction')

if __name__ == "__main__":
    main()
