import json
import os
from google_drive_client import Google_Drive_Client
from bq_client import BQ_Client
import pandas as pd



def read_json(file):
    try:
        with open(file, "rb") as file:
            return json.load(file)
    except IOError as err:
        if err.errno == 2:
            return {}
        else:
            raise



def get_all_file_content(all_file):
    file_content = {}
    for file in all_file:
        data = read_json(file)
        file_content[file] = data

    return file_content

#TODO - dataframe type to configuration
def create_df():
    movies = pd.DataFrame()
    movies["version"] = pd.Series([], dtype=str)
    movies["file"] = pd.Series([], dtype=str)
    movies["frames"] = pd.Series([], dtype=int)
    movies["frame_skip"] = pd.Series([], dtype=int)
    movies["original_frame"] = pd.Series([], dtype=int)
    movies["part"] = pd.Series([], dtype=int)
    movies["keyframes"] = pd.Series([], dtype=int)
    movies["key_bb"] = pd.Series([], dtype=int)
    movies["attributes"] = pd.Series([], dtype=object)
    movies["objects"] = pd.Series([], dtype=object)
    movies_objects = pd.DataFrame()
    movies_objects["version"] = pd.Series([], dtype=str)
    movies_objects["file"] = pd.Series([], dtype=str)
    movies_objects["nm"] = pd.Series([], dtype=str)
    movies_objects["type"] = pd.Series([], dtype=str)
    movies_objects["shape"] = pd.Series([], dtype=str)
    movies_objects["parent"] = pd.Series([], dtype=str)
    movies_objects["attributes"] = pd.Series([], dtype=object)
    movies_objects["attribute_type"] = pd.Series([], dtype=object)
    frame = pd.DataFrame()
    frame["version"] = pd.Series([], dtype=str)
    frame["file"] = pd.Series([], dtype=str)
    frame["frame_key"] = pd.Series([], dtype=str)
    frame["key"] = pd.Series([], dtype=bool)
    frame["attr_key_frame"] = pd.Series([], dtype=bool)
    frame["part"] = pd.Series([], dtype=int)
    frame["attributes"] = pd.Series([], dtype=object)
    frame["objects"] = pd.Series([], dtype=object)
    frame["ignore_frame"] = pd.Series([], dtype=str)

    frame_objects = pd.DataFrame()
    frame_objects["version"] = pd.Series([], dtype=str)
    frame_objects["file"] = pd.Series([], dtype=str)
    frame_objects["frame_key"] = pd.Series([], dtype=str)
    frame_objects["nm"] = pd.Series([], dtype=str)
    frame_objects["key"] = pd.Series([], dtype=bool)
    frame_objects["occluded"] = pd.Series([], dtype=int)
    frame_objects["x1"] = pd.Series([], dtype=float)
    frame_objects["y1"] = pd.Series([], dtype=float)
    frame_objects["x2"] = pd.Series([], dtype=float)
    frame_objects["y2"] = pd.Series([], dtype=float)
    frame_objects["attr_key_frame"] = pd.Series([], dtype=bool)
    frame_objects["path"] = pd.Series([], dtype=object)
    frame_objects["skeleton"] = pd.Series([], dtype=object)
    frame_objects["attributes"] = pd.Series([], dtype=object)
    frame_objects["ignore_frame"] = pd.Series([], dtype=str)
    return movies, movies_objects, frame, frame_objects



def get_movies_info(file_content):

    movies, movies_objects, frame, frame_objects = create_df()

    counter = 0
    for file in file_content:
        print("load the movie's meta data")
        movie_meta = file_content[file][0]

        movies = movies.append(movie_meta, ignore_index=True)
        print("load the movie's objects")
        for obj in movie_meta['objects']:
            obj_extend = {}
            obj_extend['version'] = movie_meta['version']
            obj_extend['file'] = movie_meta['file']
            obj_extend.update(obj)
            if 'type' in obj['attributes']:
                obj_extend['attribute_type'] = obj['attributes']['type']
            movies_objects = movies_objects.append(obj_extend, ignore_index=True)

            counter = counter+1
        #remove meta data
        file_content[file].pop(0)

        print("load the frame's meta data")
        fram_num = 0
        for frame_content in file_content[file]:
            frame_extend = {}
            frame_extend['version'] = movie_meta['version']
            frame_extend['file'] = movie_meta['file']
            frame_extend['frame_key'] = fram_num
            fram_num = fram_num + 1
            frame_extend.update(frame_content)
            if 'ignore_frame' in frame_content['attributes']:
                frame_extend['ignore_frame'] = frame_content['attributes']['ignore_frame']
            frame = frame.append(frame_extend, ignore_index=True)
            for frame_obj_dict in frame_content['objects']:
                frame_object_extend = {}
                frame_object_extend['version'] = movie_meta['version']
                frame_object_extend['file'] = movie_meta['file']
                frame_object_extend['frame_key'] = frame_extend['frame_key']
                frame_object_extend.update(frame_obj_dict)
                if 'ignore_frame' in frame_content['attributes']:
                    frame_object_extend['ignore_frame'] = frame_content['attributes']['ignore_frame']
                frame_objects = frame_objects.append(frame_object_extend, ignore_index=True)

    print(counter)
    return movies, movies_objects, frame, frame_objects


def main(argv=None):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    file_type = "application/json"
    config = read_json(root_dir + "/configurations.json")
    service_account_location = config['service_account']
    drive_client = Google_Drive_Client(service_account_location)
    all_folders = drive_client.get_all_folders({}, '1alWTLdYGr32GrwFCCN7NXn1z7MDT6M_6')
    bq_client= BQ_Client(service_account_location,'sightx-project')
    for key in all_folders:
        print('Load folder - {}'.format(key))
        all_file = drive_client.get_all_files(all_folders[key], file_type)
        file_content = get_all_file_content(all_file)
        movies, movies_objects, frame, frame_objects = get_movies_info(file_content)
        bq_client.load_df(movies, 'Raw_Data.Movie')
        bq_client.load_df(movies_objects, 'Raw_Data.Movie_Objects')
        bq_client.load_df(frame, 'Raw_Data.Frame')
        bq_client.load_df(frame_objects,  'Raw_Data.Frame_Objects')
    # email = Email_Service()
    # email.send_mail()

if __name__ == "__main__":
    main()


#TODO -
# data validation,
# file validation,
# location validation,
# dupliation - handle with query in th code