import requests
import pickle
import datetime
import pandas as pd
import xml.etree.ElementTree as ET

## Getting Datetime
currentDT = datetime.datetime.now().strftime("%Y-%m-%d %H:%M").replace(':','_').replace('-','_').replace(' ',' - ')
currentDT_ts = datetime.datetime.now()

## Getting Wind Farm Data
df_wind_farm_info = pd.read_csv('wind_farm_info.csv')

with open('dict_wind_farm_historic_dfs.pickle', 'rb') as handle:
    dict_wind_farm_historic_dfs = pickle.load(handle)

header = { 'Accept': 'application/xml' }
wf_data = requests.get('https://www.thecrownestate.co.uk/api/windfeed/get', headers=header)
open('wf_data.xml', 'wb').write(wf_data.content)

wind_data_req = ET.parse('wf_data.xml') 
wind_data_root = wind_data_req.getroot()

## Cleaning Wind Farm Data
def get_content(wind_farm):
    df_temp = pd.DataFrame([])
    for i in range(len(wind_farm)):
        tag = wind_farm[i].tag[81:]
        text = wind_farm[i].text
        df_temp.set_value(i, 'Tag', tag)
        df_temp.set_value(i, 'Text', text)
    return df_temp

def format_content():
    dict_wind_farm_dfs = {}
    for wind_farm in wind_data_root[0]:
        ID = wind_farm[1].text.replace('-','_')
        dict_wind_farm_dfs['df_{}'.format(ID)] = get_content(wind_farm)
    return dict_wind_farm_dfs

dict_wind_farm_dfs = format_content()

## Pickling Output Dictionary of Dataframes    
def pickle_dict():
    with open('dict_wf_dfs - {}.pickle'.format(currentDT), 'wb') as handle:
        pickle.dump(dict_wind_farm_dfs, handle, protocol=pickle.HIGHEST_PROTOCOL)

#pickle_dict()

##
def output_content(dict_wind_farm_historic_dfs):        
    for i in range(len(dict_wind_farm_dfs)):
        ID = df_wind_farm_info["ID"][i].replace('-','_')
        
        length = dict_wind_farm_historic_dfs['df_{}'.format(ID)].shape[0]
        
        df_temp1 = dict_wind_farm_dfs['df_{}'.format(ID)]
        
        wf_output = df_temp1["Text"][11]
        wf_capacity = df_temp1["Text"][14]
        
        df_temp2 = pd.DataFrame([])
        df_temp2.set_value(length, "Datetime", currentDT_ts)
        df_temp2.set_value(length, "Output", wf_output)
        df_temp2.set_value(length, "Capacity", wf_capacity)
        
        dict_wind_farm_historic_dfs['df_{}'.format(ID)] =  dict_wind_farm_historic_dfs['df_{}'.format(ID)].append(df_temp2)
    
    with open('dict_wind_farm_historic_dfs.pickle', 'wb') as handle:
        pickle.dump(dict_wind_farm_historic_dfs, handle, protocol=pickle.HIGHEST_PROTOCOL)
                           
    return dict_wind_farm_historic_dfs

dict_wind_farm_historic_dfs = output_content(dict_wind_farm_historic_dfs)
#df_test = dict_wind_farm_historic_dfs['df_{}'.format('LARYO')]