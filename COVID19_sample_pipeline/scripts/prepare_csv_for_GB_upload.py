from zipfile import ZipFile
import json
import pandas as pd
import os


def split_geos(covid_main_table, title_dict):
    save_files_path = f'D:\Coronavirus data\Data for upload\\'

    sl040_data = covid_main_table[covid_main_table['SUMLEV'] == 'SL040']
    sl050_data = covid_main_table[covid_main_table['SUMLEV'] == 'SL050']

    file_name = covid_main_table.columns[1][:11]

    for col in sl040_data.columns:
        for k, v in title_dict.items():
            if k[12:] != 'Date' and k[12:] == col:
                sl040_data.rename(columns={col: v}, inplace=True)

    for col in sl050_data.columns:
        for k, v in title_dict.items():
            if k[12:] != 'Date' and k[12:] == col:
                sl050_data.rename(columns={col: v}, inplace=True)

    sl040_data.rename(columns={'GeoID': 'Geo_FIPS'}, inplace=True)
    sl050_data.rename(columns={'GeoID': 'Geo_FIPS'}, inplace=True)

    sl040_data.drop(['SUMLEV'], inplace=True, axis=1)
    sl050_data.drop(['SUMLEV'], inplace=True, axis=1)

    sl040_data.to_csv("" + save_files_path + file_name + '_SL040.csv', index=False)
    sl050_data.to_csv("" + save_files_path + file_name + '_SL050.csv', index=False)

    for file in os.listdir(save_files_path):
        if file.endswith('.csv'):
            zipObj = ZipFile(os.path.join(save_files_path, file)[:-4] + '.zip', mode='w')
            zipObj.write(os.path.join(save_files_path, file), os.path.basename(os.path.join(save_files_path, file)))
            os.remove(os.path.join(save_files_path, file))


if __name__ == '__main__':
    Path = f'D:\Coronavirus data\Data for upload\Prepare GUIDs for upload\\'
    files_for_update = os.listdir(Path)

    for file in files_for_update:
        if file.endswith('.csv'):
            covid19_data = pd.read_csv(os.path.join(Path, file), dtype={'GeoID': 'str'})
        elif file.endswith('.json'):
            with open(os.path.join(Path, file), 'r') as f:
                data = f.read()
            title_dictionary = json.loads(data)

    split_geos(covid19_data, title_dictionary)
