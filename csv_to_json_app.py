import sys
import os
import json
import glob
import re
import pandas as pd

def get_column_names(schemas, ds_name, sorting_key = "column_position"):
    '''
    schemas = loaded json 
    ds_name = folder name at -2
    '''
    # Getting spcific object from JSON dict
    column_details = schemas[ds_name] 
    # Sorting is based on sorting_key = "column_position" which is part of  object from JSON dict
    columns = sorted(column_details, key=lambda col: col[sorting_key]) 
    # 'column_name' is part of  object from JSON dict 
    return [col['column_name'] for col in columns] 


def read_csv(file, schemas):
    #Removes slashes form path and provides list of every folder name seprated
    file_path_list = re.split('[/\\\]', file) 
    #Gettings folder at -2 Which is also one of the key in json
    ds_name = file_path_list[-2] 
     # Sending loaded json and folder name at -2
    columns = get_column_names(schemas, ds_name)
    #names arg is used to specify a list of column names for the DataFrame
    df = pd.read_csv(file, names=columns)
    return df

def to_json(df, tgt_base_dir, ds_name, file_name):
    '''
    df = csv file converted to df
    ds_name = Key from schema json
    '''
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)
    df.to_json(json_file_path, orient='records', lines=True)

def file_converter(src_base_dir, tgt_base_dir, ds_name):
    '''
    ds_name = Key from schema json
    '''
    schemas = json.load(open(f"{src_base_dir}/schemas.json"))
    #Getting file names
    files = glob.glob(f"{src_base_dir}/{ds_name}/part-*") 
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')
    for file in files:
        df = read_csv(file, schemas)
        #Removes slashes form path and provides list of every folder name seprated and get at -1 which are part-00000
        file_name = re.split('[/\\\]', file)[-1] 
        to_json(df, tgt_base_dir, ds_name, file_name)
        
# Only reading JSON and sending keys 
def process_files(ds_names = None): 
    src_base_dir = os.environ.get("SRC_BASE_DIR")
    tgt_base_dir = os.environ.get("TGT_BASE_DIR")
    if not src_base_dir or not tgt_base_dir:
        print("Souce and Target directories not set")
        return
    schemas = json.load(open(f"{src_base_dir}/schemas.json"))
    
    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f'Processing {ds_name}')
            #Sending keys in Json one by one
            file_converter(src_base_dir, tgt_base_dir, ds_name) 
        except NameError as ne:
            print(ne)
            print(f'Error Processing {ds_name}')
            pass 


if __name__ == '__main__':
    # sys.argv[0] is path of this python file
    if len(sys.argv) <= 1:
        process_files()
    else:
        process_files(json.loads(sys.argv[1]))

    print("Program Exit")