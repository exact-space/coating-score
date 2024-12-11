import os
import requests, json
import datetime,time
from datetime import datetime,date,timedelta
import pandas as pd
import traceback
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
import warnings
import seaborn as sns
import pytz
warnings.filterwarnings("ignore")


import platform
import timeseries as ts

qr = ts.timeseriesquery()
# import schedule
# import apscheduler
# from apscheduler.schedulers.background import BackgroundScheduler


global payload_task
payload_task = []

version = platform.python_version().split(".")[0]
if version == "3":
    import app_config.app_config as cfg
elif version == "2":
    import app_config as cfg


runtime_id = {"id": ""}
runtime_id["id"]= "1"

config = cfg.getconfig()
cfg = config["api"]["meta"]
broker = config["BROKER_ADDRESS"]
getKairosUrl =config["api"]["query"]
global url1
url1 = getKairosUrl
print(url1)

BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS")
if not BROKER_ADDRESS:
    BROKER_ADDRESS = config["BROKER_ADDRESS"]

unitId = '65ae141fd0928341551f8695'
flow_tag = 'SDCW2_KN_Kiln Feed Act'
FEED_THRESHOLD = 300
coating_score_tag, shell_temp_score_tag, process_score_tag, quality_score_tag = 'SDCW2_Kiln_Coating_Score', 'SDCW2_Kiln_Coating_Score_Shell_Temp', 'SDCW2_Kiln_Coating_Score_Process', 'SDCW2_Kiln_Coating_Score_Quality'
excess_temp_score_tag, less_temp_score_tag = 'SDCW2_Kiln_Excess_Coating_Score', 'SDCW2_Kiln_Less_Coating_Score'
temp_tags = ['SDCW2_KN_KILNTEMP00M_05M','SDCW2_KN_KILNTEMP06M_07M','SDCW2_KN_KILNTEMP09-5M_11M','SDCW2_KN_KILNTEMP12M_14M','SDCW2_KN_KILNTEMP18M_19-5M','SDCW2_KN_KILNTEMP21-5M_23M','SDCW2_KN_KILNTEMP23M_24-5M','SDCW2_KN_KILNTEMP28M_30M','SDCW2_KN_KILNTEMP30M_32M','SDCW2_KN_KILNTEMP36M_38M','SDCW2_KN_KILNTEMP38M_40M']
process_tags = ['SDCW2_KN_462KL01_PT01', 'SDCW2_CL_472FNB_ST01', 'SDCW2_KN_462GA1_AT01', 'SDCW2_KN_Kiln Feed Act', 'SDCW2_Kiln_Coal_Ratio', 'SDCW2_KN_L2_TOTAL_COAL_TPH','SDCW2_KN_442FN1_VFD_ST01']   
#process_columns = ['Kiln Inlet Pressure', 'Cooler id rpm','Kiln Inlet CO','Kiln Total Feed','Kiln Coal Ratio','Total coal consumption']
quality_tags = ['SDCW2_QCX_KN_LSF','SDCW2_QCX_KN_R_90micron','SDCW2_QCX_CLK_Liquid','SDCW2_QCX_CLK_CaO','SDCW2_QCX_CLK_Al2O3','SDCW2_QCX_HM_Na2O','SDCW2_QCX_HM_LOI-KF','SDCW2_QCX_Coatability_Index_Calc','SDCW2_QCX_Hydraulic_Ratio_Calc','SDCW2_QCX_KN_AM','SDCW2_QCX_KN_SM','SDCW2_QCX_CLK_C3S','SDCW2_QCX_CLK_C2S','SDCW2_QCX_KM_ASH']


def get_data(tag, num_of_weeks):
    qr = ts.timeseriesquery()
    qr.addMetrics(tag)
    qr.chooseTimeType("relative",{"start_value":num_of_weeks, "start_unit":"weeks"}) #  30 mins,15 datapoints
    qr.submitQuery()
    qr.formatResultAsDF()
    try:
        if type(qr.resultset["results"][0])==dict and (len(qr.resultset["results"]) > 0):
            df = qr.resultset["results"][0]["data"]
            return df
        else:
            print("no datapoints found in kairos")
            df = pd.DataFrame()
            return df
    except Exception as e:
        if 'SDCW2_QCX_KN_LSF' in tag:
            name = 'Quality'
        elif 'SDCW2_KN_KILNTEMP00M_05M' in tag:
            name = 'Shell Temp'
        else:
            name = 'Process'
        print(e,'in get_data',name)
        df = pd.DataFrame()
        return df

def postData(tag,time,val):
    rounded_hour = time.replace(minute=0, second=0, microsecond=0)
    epoch_time = int(rounded_hour.timestamp() * 1000)
    # epoch_time = 1731907910000
    body=[{"name":tag,"datapoints":[[epoch_time,val]], "tags" : {"type":"simulation"}}]
    #print(body)
    #postBody = [{'t':float(epoch_time),'v':float(val)}]
    #print(postBody)
    #print(topic_line+tag+"/r",postBody)
    #client.publish(topic_line+tag+"/r",json.dumps(postBody))
    
    qr.postDataPacket(body)


def calculate_combined_score(row):
    temp_score = row['final_coating_score_temp_smoothed']
    process_score = row['final_coating_score_process_smoothed']
    if pd.isna(temp_score):
        return np.NaN
    if temp_score < 0:
        process_score = -abs(process_score)
    if pd.isna(process_score):
        combined_score = temp_score * 1  
    elif abs(temp_score) < 1:  
        combined_score = temp_score * 1 
    else:
        combined_score = temp_score * 0.6 + process_score * 0.4

    return combined_score


def smooth_scores(df, diff_col, reset_flag_col, coating_score_col, smoothed_col, spike_threshold, alpha):
    previous_score = 0
    smoothed_scores = []

    for _, row in df.iterrows():
        previous_score = calculate_score(row, previous_score, diff_col, reset_flag_col, coating_score_col, spike_threshold, alpha)
        smoothed_scores.append(previous_score)

    df[smoothed_col] = smoothed_scores
    return df


def calculate_score(row, prev_score, diff_col, reset_flag_col, initial_score_col, spike_threshold, alpha):
    if row[reset_flag_col] == 1:
        return row[initial_score_col]
    
    current_score = row[initial_score_col]
    
    if abs(current_score - prev_score) > spike_threshold:
        adjustment = alpha * (current_score - prev_score) / 2
    else:
        adjustment = alpha * (current_score - prev_score)
    
    new_score = prev_score + adjustment
    return new_score


def save_abnormalities_to_csv(df, csv_path, time_column, score_column, abnormalities_column):
    data_to_save = df[[time_column, score_column, abnormalities_column]]

    # If the file exists, load it, append new data, and remove duplicates
    if os.path.exists(csv_path):
        existing_data = pd.read_csv(csv_path)
        combined_data = pd.concat([existing_data, data_to_save], ignore_index=True)
        combined_data.drop_duplicates(subset=[time_column], inplace=True)  # Avoid duplicate entries
    else:
        combined_data = data_to_save

    combined_data[time_column] = pd.to_datetime(combined_data[time_column])
    combined_data.sort_values(by=time_column, inplace=True)

    combined_data.to_csv(csv_path, index=False)



def calculate_temp_score(temp_tags, flow_tag):
    
    def validate_temp(row, last_valid_values, out_of_range_columns):
        valid_row = row.copy()
        columns_with_variation = []
        for col in last_valid_values.keys():
            current_value = row[col]
            last_valid_value = last_valid_values[col]

            if last_valid_value is None:
                last_valid_values[col] = current_value
            else:
                lower_bound = last_valid_value - TEMP_THRESHOLD
                upper_bound = last_valid_value + TEMP_THRESHOLD
                #     327 <= 305 no  327 >= 345 yes. if 327 .. 
                # abs(327 - 305)
                #if abs(current_value - last_valid_value) <= TEMP_THRESHOLD:
                if current_value <= lower_bound or current_value >= upper_bound:
                    columns_with_variation.append(col)
                    valid_row[col] = last_valid_value
                else:
                    last_valid_values[col] = current_value
                    out_of_range_columns.discard(col)

        if len(columns_with_variation) >= 6:
            out_of_range_columns.update(columns_with_variation)
            return valid_row
        else:
            out_of_range_columns.clear()
            return row

    def correct_temperature_data(df, temp_tags, temp_threshold):
        global TEMP_THRESHOLD
        TEMP_THRESHOLD = temp_threshold

        last_valid_values = {col: None for col in temp_tags}
        out_of_range_columns = set()
        df['six_hour_interval'] = (df['time'] - df['time'].min()) // pd.Timedelta(hours=6)

        validated_data = []
        current_interval = -1
        for _, row in df.iterrows():
            if row['six_hour_interval'] != current_interval:
                last_valid_values = {col: None for col in temp_tags}
                current_interval = row['six_hour_interval']

            validated_row = validate_temp(row, last_valid_values, out_of_range_columns)
            validated_data.append(validated_row)

        return pd.DataFrame(validated_data)

    def calculate_coating_score(row, temp_col, ideal_temp_min, ideal_temp_med, ideal_temp_max):
        rolling_mean = row[temp_col]
        
        if rolling_mean < ideal_temp_min: #less shell temp, coating formed
            value = abs((rolling_mean - ideal_temp_min) / ideal_temp_min * 100)
            
        elif rolling_mean > ideal_temp_max:
            value = -abs((rolling_mean - ideal_temp_max) / ideal_temp_max * 100)
            
        else:
            return 0
        # if abs(value) > 10:
        #     value = value * 0.1

        #return pd.Series({'value': value, 'abnormalities_shell_temp': abnormalities})
        return value

    def calculate_overall_coating_score(row, temp_tags):
        negative_scores = []
        positive_scores = []
        positive_abnormalities = {}
        negative_abnormalities = {}

        for temp_col in temp_tags:
            score = row[f'{temp_col} Coating Score']
            if score < 0:
                negative_scores.append(score)
                negative_abnormalities[temp_col] = row[temp_col]
            elif score > 0:
                positive_scores.append(score)
                positive_abnormalities[temp_col] = row[temp_col]

        # if len(negative_scores) > 6: # Check if more than 6 coating scores are negative
        #     return pd.Series({'value': sum(negative_scores) / len(ideal_temp_min), 'abnormalities_shell_temp': negative_abnormalities})
        #     #return sum(negative_scores) / len(ideal_temp_min)
        # elif len(positive_scores) >= 6: #if more than 6 coating scores are positive
        #     return pd.Series({'value': sum(positive_scores) / len(ideal_temp_min), 'abnormalities_shell_temp': positive_abnormalities})
        #     #return sum(positive_scores) / len(ideal_temp_min)
        # else: # If neither condition is met, return 0
        #     return 0
        #logic 1 is below
        #return pd.Series({'value': (sum(negative_scores) / len(ideal_temp_min)) + (sum(positive_scores) / len(ideal_temp_min)), 'abnormalities_shell_temp': negative_abnormalities})
        
        #logic 2 is below
        if len(negative_scores) > len(positive_scores): # Check if more than 6 coating scores are negative
            return pd.Series({'value': sum(negative_scores) / len(ideal_temp_min), 'abnormalities_shell_temp': negative_abnormalities, 'excess': sum(positive_scores) / len(ideal_temp_min), 'less': abs(sum(negative_scores) / len(ideal_temp_min))})
        else: #if more than 6 coating scores are positive
            return pd.Series({'value': sum(positive_scores) / len(ideal_temp_min), 'abnormalities_shell_temp': positive_abnormalities, 'excess':sum(positive_scores) / len(ideal_temp_min), 'less': abs(sum(negative_scores) / len(ideal_temp_min))})
        #logic 3 is below
        # if len(negative_abnormalities) == 0 and len(positive_abnormalities) == 0:
        #     return pd.Series({'value': 0, 'abnormalities_shell_temp': {}})
        # elif len(negative_abnormalities) != 0 and len(positive_abnormalities) == 0:
        #     return pd.Series({'value': sum(negative_scores) / len(ideal_temp_min), 'abnormalities_shell_temp': negative_abnormalities})
        # elif len(negative_abnormalities) == 0 and len(positive_abnormalities) != 0:
        #     return pd.Series({'value': sum(positive_scores) / len(ideal_temp_min), 'abnormalities_shell_temp': positive_abnormalities})
        # else:
        #     #print(row['time'], abs(sum(negative_scores)/len(negative_abnormalities)),'and',abs(sum(positive_scores)/len(positive_abnormalities)))
        #     if abs(sum(negative_scores)/len(negative_abnormalities)) > abs(sum(positive_scores)/len(positive_abnormalities)):
        #         return pd.Series({'value': sum(negative_scores) / len(ideal_temp_min), 'abnormalities_shell_temp': negative_abnormalities})
        #         #return sum(negative_scores) / len(ideal_temp_min)
        #     else: #if more than 6 coating scores are positive
        #         return pd.Series({'value': sum(positive_scores) / len(ideal_temp_min), 'abnormalities_shell_temp': positive_abnormalities})
        #         #return sum(positive_scores) / len(ideal_temp_min)
       
    df_temp = get_data([flow_tag]+temp_tags, num_of_weeks=3)
    #print(df_temp.tail(1))
    if df_temp is not None and not df_temp.empty:
        df_temp['time'] = pd.to_datetime(df_temp['time']/1000, unit="s")
        # Filter data based on 'Kiln Total Feed'
        df_temp = df_temp[(df_temp[flow_tag] >= FEED_THRESHOLD)]

        
        if df_temp is None or df_temp.empty:
            return pd.DataFrame(columns=['time','coating_score_temp'])

        try:
            df_temp = df_temp[['time']+temp_tags]
            for temp in temp_tags:
                df_temp = df_temp[(df_temp[temp] < 430) & (df_temp[temp] > 100)]

            if df_temp is None or df_temp.empty:
                return pd.DataFrame(columns=['time','coating_score_temp'])
        except:
            print('no data/ invalid Shell temperature')
            return pd.DataFrame(columns=['time','coating_score_temp'])
        #df_temp1 = df_temp.copy()
        df_temp = correct_temperature_data(df_temp, temp_tags,temp_threshold=19)
        #print(df_temp.tail(1)) 1830
        df_temp.set_index('time', inplace=True)
        df_temp[temp_tags] = df_temp[temp_tags].rolling(window='1H', min_periods=1).mean()
        
        df_temp = df_temp.resample('60T').mean().dropna()
        df_temp.reset_index(inplace=True)
        #print(df_temp.tail(1)) 1800
        for temp_col in temp_tags:
            df_temp[f'{temp_col} Coating Score'] = df_temp.apply(calculate_coating_score, axis=1, 
                temp_col=temp_col, 
                ideal_temp_min=ideal_temp_min[temp_col], 
                ideal_temp_med=ideal_temp_med[temp_col], 
                ideal_temp_max=ideal_temp_max[temp_col])

        results = df_temp.apply(calculate_overall_coating_score, axis=1, temp_tags=temp_tags)

        df_temp['coating_score_temp'] = results['value']
        df_temp['coating_score_temp'] = df_temp['coating_score_temp'].where(df_temp['coating_score_temp'] < 9.74, df_temp['coating_score_temp'] * 0.1)
        df_temp['abnormalities_shell_temp'] = results['abnormalities_shell_temp']

        df_temp['coating_score_temp_excess'] = results['excess'] #
        df_temp['coating_score_temp_less'] = results['less']
        
        return df_temp.sort_values(by='time', ascending=True)

    else:
        return pd.DataFrame(columns=['time','coating_score_temp'])

    
def calculate_process_score(process_tags):
    def calculate_score_process(row):
        score = 0
        abnormalities = {}
        if (row['Kiln Inlet Pressure'] <= -55.21) and (row['PH RPM to Kiln Inlet Pressure'] < -0.646): 
            if (row['Cooler id rpm'] >= 689.900148):
                score += abs(row['Cooler id rpm'] - 689.900148) * 0.01
                abnormalities['Cooler id rpm'] = row['Cooler id rpm']

            score += abs(row['Kiln Inlet Pressure'] + 55.21) * 0.1
            score += abs(row['PH RPM to Kiln Inlet Pressure'] + 0.646) * 0.8
            abnormalities['Kiln Inlet Pressure'] = row['Kiln Inlet Pressure']
            abnormalities['PH RPM to Kiln Inlet Pressure'] = row['PH RPM to Kiln Inlet Pressure']

            if row['Kiln Inlet CO'] >= 31.442821:
                if (row['Kiln Total Feed'] > 319.7):
                    if (row['Kiln Coal Ratio'] > 0.323) and (row['Total coal consumption'] > 22.5):
                        score += abs(row['Kiln Coal Ratio'] - 0.323) * 6 
                        abnormalities['Kiln Coal Ratio'] = row['Kiln Coal Ratio']
                        abnormalities['Total coal consumption'] = row['Total coal consumption']
            else:
                if (row['Kiln Total Feed'] <= 319.7):
                    if (row['Kiln Coal Ratio'] <= 0.323) and (row['Total coal consumption'] <= 22.5):
                        score += abs(row['Kiln Coal Ratio'] - 0.323) * 6
                        abnormalities['Kiln Coal Ratio'] = row['Kiln Coal Ratio']
                        abnormalities['Total coal consumption'] = row['Total coal consumption']
        return pd.Series({'score': score, 'abnormalities_process': abnormalities})
        #return score

    df_process = get_data(process_tags, num_of_weeks=3)

    if df_process is not None and not df_process.empty:
        df_process = df_process.rename(columns=tagMapper_rolling)
        #df_process['time'] = df_process['time'].str[:19]
        df_process['time'] = pd.to_datetime(df_process['time']/1000, unit="s")

        # Filter data based on 'Kiln Total Feed'
        df_process = df_process[(df_process['Kiln Total Feed'] >= FEED_THRESHOLD)]

        if df_process is None or df_process.empty:
            return pd.DataFrame(columns=['time','coating_score_process'])
        else:
            df_process['PH RPM to Kiln Inlet Pressure'] = np.where(df_process['Kiln Inlet Pressure'] != 0, df_process['Kiln Inlet Pressure'] / df_process['PH RPM'], np.nan) * 10
            df_process.drop(columns=['PH RPM'],axis=1,inplace=True)
            process_columns = ['Kiln Inlet Pressure','PH RPM to Kiln Inlet Pressure','Cooler id rpm','Kiln Inlet CO','Kiln Total Feed','Kiln Coal Ratio','Total coal consumption']

            if df_process.shape[-1] == len(process_columns)+1:
                
                df_process = df_process[['time']+process_columns]

                df_process.set_index('time', inplace=True)
                df_process.loc[:, df_process.columns != 'time'] = df_process.loc[:, df_process.columns != 'time'].rolling(window='1H').mean()
                #df_process = df_process.dropna(how='all', subset=df_process.columns.difference(['time']))
                #df_process = df_process.dropna(how='all')
                
                df_process = df_process.resample('60T').mean().dropna()
                df_process.reset_index(inplace=True)

                if df_process is not None and not df_process.empty:
                    #df_process['coating_score_process'], abnormalities = df_process.apply(calculate_score_process, axis=1)
                    results = df_process.apply(calculate_score_process, axis=1)

                    # Extract columns from results
                    df_process['coating_score_process'] = results['score']
                    df_process['abnormalities_process'] = results['abnormalities_process']
                    return df_process.sort_values(by='time', ascending=True)
                else:
                    print('no data in process tag-')
                    return pd.DataFrame(columns=['time','coating_score_process'])
            else:
                print('no data in process tag')
                return pd.DataFrame(columns=['time','coating_score_process'])
    else:
        return pd.DataFrame(columns=['time','coating_score_process'])

    
def calculate_coating_risk_quality(quality_tags):

    def calculate_rolling_std(df, time_column='time', window_size='7D'):
        df[time_column] = pd.to_datetime(df[time_column])

        df = df.sort_values(by=time_column)

        df.set_index(time_column, inplace=True)

        rolling_std = df.rolling(window=window_size).std()

        rolling_std.reset_index(inplace=True)

        return rolling_std

#rolling_std_df = calculate_rolling_std(df, time_column='time', window_size='30D')

    def calculate_std_score(row, rolling_std_df):
        total_std_score = 0
        std_dict = {}

        for column in rolling_std_df.columns:
            if column != 'time':  
                if column in row:
                    std_value = rolling_std_df[rolling_std_df['time'] == row['time']][column].values

                    if std_value.size > 0:
                        std_value = std_value[0] 
                        if np.isnan(std_value):
                            std_value = 0

                        total_std_score += std_value
                        std_dict[column] = std_value

        return pd.Series({'score': total_std_score, 'std_quality': std_dict})

    df_quality = get_data(quality_tags, num_of_weeks=75)

    if df_quality is not None and not df_quality.empty:
        df_quality['time'] = pd.to_datetime(df_quality['time']/1000, unit="s")
        for column in df_quality.columns:
            df_quality = df_quality[df_quality[column] != 0]

        df_quality = df_quality.reset_index(drop=True)
        df_quality = df_quality.sort_values(by='time', ascending=True)
        df_quality = df_quality.set_index('time')
        df_quality = df_quality[~df_quality.index.duplicated(keep='first')]

        full_time_index = pd.date_range(start=df_quality.index.min(), end=df_quality.index.max(), freq='H')
        df_quality = df_quality.reindex(full_time_index)
        
        for col in df_quality.columns: #last known for next two days 
            mask = df_quality[col].notna() 
            df_quality[col] = df_quality[col].where(mask).fillna(method='ffill', limit=48)

        df_quality = df_quality.reset_index().rename(columns={'index': 'time'})

        df_quality = df_quality.dropna(how='all', subset=df_quality.columns.difference(['time']))
        std_df = calculate_rolling_std(df_quality, time_column='time', window_size='30D')
       
        df_quality[['coating_score_quality', 'std_quality']] = df_quality.apply(calculate_std_score, axis=1, rolling_std_df=std_df)

        return df_quality.sort_values(by='time', ascending=True)
    
    else:
        pd.DataFrame(columns=['time','coating_score_quality'])


tagMapper_rolling = {"SDCW2_KN_Kiln Feed Act" : 'Kiln Total Feed',
"SDCW2_TRQ_CALC_TAG" : 'Kiln Torque',
"SDCW2_KN_KILNTEMP00M_05M":'SDCW2_KN_KILNTEMP00M_05M',
"SDCW2_KN_KILNTEMP06M_07M":'SDCW2_KN_KILNTEMP06M_07M',
"SDCW2_KN_KILNTEMP09-5M_11M":'SDCW2_KN_KILNTEMP09-5M_11M',
"SDCW2_KN_KILNTEMP12M_14M":'SDCW2_KN_KILNTEMP12M_14M',
"SDCW2_KN_KILNTEMP18M_19-5M":'SDCW2_KN_KILNTEMP18M_19-5M',
"SDCW2_KN_KILNTEMP21-5M_23M":'SDCW2_KN_KILNTEMP21-5M_23M',
"SDCW2_KN_KILNTEMP23M_24-5M":'SDCW2_KN_KILNTEMP23M_24-5M',
"SDCW2_KN_KILNTEMP28M_30M":'SDCW2_KN_KILNTEMP28M_30M',
"SDCW2_KN_KILNTEMP30M_32M":'SDCW2_KN_KILNTEMP30M_32M',
"SDCW2_KN_KILNTEMP36M_38M":'SDCW2_KN_KILNTEMP36M_38M',
"SDCW2_KN_KILNTEMP38M_40M":'SDCW2_KN_KILNTEMP38M_40M',
"SDCW2_KN_462MD1U01_IT01":'Kiln Main Drive Current',
"SDCW2_CL_472FNB_ST01":'Cooler id rpm',
"SDCW2_KN_462KL01_PT01":'Kiln Inlet Pressure',
"SDCW2_Kiln_Coal_Ratio": "Kiln Coal Ratio",
"SDCW2_KN_462GA1_AT01": 'Kiln Inlet CO',
'SDCW2_KN_L2_TOTAL_COAL_TPH':'Total coal consumption',
'SDCW2_KN_442FN1_VFD_ST01': 'PH RPM'}

# ideal temperature values
ideal_temp_min = {
    'SDCW2_KN_KILNTEMP38M_40M': 220.73684210526315, 'SDCW2_KN_KILNTEMP36M_38M': 312.1363636363636,
    'SDCW2_KN_KILNTEMP30M_32M': 329.3333333333333, 'SDCW2_KN_KILNTEMP28M_30M': 331.03333333333336,
    'SDCW2_KN_KILNTEMP23M_24-5M': 300.1, 'SDCW2_KN_KILNTEMP21-5M_23M': 302.12833333333333,
    'SDCW2_KN_KILNTEMP18M_19-5M': 282.0435294117647, 'SDCW2_KN_KILNTEMP12M_14M': 260.21032258064514,
    'SDCW2_KN_KILNTEMP09-5M_11M': 286.0, 'SDCW2_KN_KILNTEMP06M_07M': 263.2711111111111,
    'SDCW2_KN_KILNTEMP00M_05M': 284.3333333333333
} #05 quantile

ideal_temp_med = {
    'SDCW2_KN_KILNTEMP38M_40M': 227.88888888888889, 'SDCW2_KN_KILNTEMP36M_38M': 318.1578947368421,
    'SDCW2_KN_KILNTEMP30M_32M': 336.0, 'SDCW2_KN_KILNTEMP28M_30M': 339.73333333333335,
    'SDCW2_KN_KILNTEMP23M_24-5M': 316.83870967741933, 'SDCW2_KN_KILNTEMP21-5M_23M': 315.0,
    'SDCW2_KN_KILNTEMP18M_19-5M': 311.7894736842105, 'SDCW2_KN_KILNTEMP12M_14M': 320.22727272727275,
    'SDCW2_KN_KILNTEMP09-5M_11M': 314.3, 'SDCW2_KN_KILNTEMP06M_07M': 286.76666666666665,
    'SDCW2_KN_KILNTEMP00M_05M': 311.4
} #50 quantile

# ideal_temp_max = {'SDCW2_KN_KILNTEMP38M_40M': 229.22253129346313,
#  'SDCW2_KN_KILNTEMP36M_38M': 319.4736111111111,
#  'SDCW2_KN_KILNTEMP30M_32M': 338.1430555555556,
#  'SDCW2_KN_KILNTEMP28M_30M': 343.11944444444447,
#  'SDCW2_KN_KILNTEMP23M_24-5M': 323.41590214067276,
#  'SDCW2_KN_KILNTEMP21-5M_23M': 319.37555555555554,
#  'SDCW2_KN_KILNTEMP18M_19-5M': 322.3601694915254,
#  'SDCW2_KN_KILNTEMP12M_14M': 331.7199074074074,
#  'SDCW2_KN_KILNTEMP09-5M_11M': 318.8113730929265,
#  'SDCW2_KN_KILNTEMP06M_07M': 302.17385257301805,
#  'SDCW2_KN_KILNTEMP00M_05M': 316.075} #75 quantile

ideal_temp_max = {
    'SDCW2_KN_KILNTEMP38M_40M': 273.5343517912027, 
    'SDCW2_KN_KILNTEMP36M_38M': 346.4309701492537,
    'SDCW2_KN_KILNTEMP30M_32M': 340.8412213740458, 
    'SDCW2_KN_KILNTEMP28M_30M': 347.4320237466538,
    'SDCW2_KN_KILNTEMP23M_24-5M': 331.3298465412765, 
    'SDCW2_KN_KILNTEMP21-5M_23M': 325.25291608715804,
    'SDCW2_KN_KILNTEMP18M_19-5M': 329.0513888888889, 
    'SDCW2_KN_KILNTEMP12M_14M': 353.6830555555555,
    'SDCW2_KN_KILNTEMP09-5M_11M': 342.58338014981274, 
    'SDCW2_KN_KILNTEMP06M_07M': 316.979440316395,
    'SDCW2_KN_KILNTEMP00M_05M': 323.5035034506262
} #95 quantile
#weights for each temperature column
qcx_05 = {'SDCW2_QCX_KN_LSF': 98.16,
 'SDCW2_QCX_KN_R_90micron': 18.78,
 'SDCW2_QCX_CLK_Liquid': 26.36,
 'SDCW2_QCX_CLK_CaO': 61.02,
 'SDCW2_QCX_CLK_Al2O3': 5.04,
 'SDCW2_QCX_HM_Na2O': 0.16,
 'SDCW2_QCX_HM_LOI-KF': 35.97,
 'SDCW2_QCX_Coatability_Index_Calc': 29.93,
 'SDCW2_QCX_Hydraulic_Ratio_Calc': 11.326629860777723,
 'SDCW2_QCX_KN_AM':1.17,
 'SDCW2_QCX_KN_SM': 2.12,
 'SDCW2_QCX_CLK_C3S': 46.82,
 'SDCW2_QCX_CLK_C2S': 22.65,
 'SDCW2_QCX_KM_ASH': 17.71}

qcx_50 = {'SDCW2_QCX_KN_LSF': 99.51,
 'SDCW2_QCX_KN_R_90micron': 20.17,
 'SDCW2_QCX_CLK_Liquid': 26.54,
 'SDCW2_QCX_CLK_CaO': 61.25,
 'SDCW2_QCX_CLK_Al2O3': 5.08,
 'SDCW2_QCX_HM_Na2O': 0.29,
 'SDCW2_QCX_HM_LOI-KF': 36.47,
 'SDCW2_QCX_Coatability_Index_Calc': 30.18,
 'SDCW2_QCX_Hydraulic_Ratio_Calc': 11.586880466472302,
 'SDCW2_QCX_KN_AM':1.26,
 'SDCW2_QCX_KN_SM': 2.22,
 'SDCW2_QCX_CLK_C3S': 47.30,
 'SDCW2_QCX_CLK_C2S': 23.29,
 'SDCW2_QCX_KM_ASH': 19.91}

qcx_95 = {'SDCW2_QCX_KN_LSF': 100.68,
 'SDCW2_QCX_KN_R_90micron': 21.73,
 'SDCW2_QCX_CLK_Liquid': 26.79,
 'SDCW2_QCX_CLK_CaO': 61.46,
 'SDCW2_QCX_CLK_Al2O3': 5.18,
 'SDCW2_QCX_HM_Na2O': 0.42,
 'SDCW2_QCX_HM_LOI-KF': 36.54,
 'SDCW2_QCX_Coatability_Index_Calc': 30.48,
 'SDCW2_QCX_Hydraulic_Ratio_Calc': 11.735849649350058,
 'SDCW2_QCX_KN_AM':1.34,
 'SDCW2_QCX_KN_SM': 2.30,
 'SDCW2_QCX_CLK_C3S': 47.87,
 'SDCW2_QCX_CLK_C2S': 23.90,
 'SDCW2_QCX_KM_ASH': 22.02}
weights = {
    'SDCW2_KN_KILNTEMP38M_40M': 1,
    'SDCW2_KN_KILNTEMP36M_38M': 1,
    'SDCW2_KN_KILNTEMP30M_32M': 1,
    'SDCW2_KN_KILNTEMP28M_30M': 1,
    'SDCW2_KN_KILNTEMP23M_24-5M': 1,
    'SDCW2_KN_KILNTEMP21-5M_23M': 1,
    'SDCW2_KN_KILNTEMP18M_19-5M': 1,
    'SDCW2_KN_KILNTEMP12M_14M': 1,
    'SDCW2_KN_KILNTEMP09-5M_11M': 1,
    'SDCW2_KN_KILNTEMP06M_07M': 1,
    'SDCW2_KN_KILNTEMP00M_05M': 1
}
ideal_process_max = {'Kiln Main Drive Current': 418.22, 'Kiln Inlet Pressure': -55.21,'Kiln Coal Ratio': 0.372,'Cooler id rpm': 709.60}


import pytz

def generate_and_post_task_body(last_row, case_num):
    def serialize_task_template(obj):
        if isinstance(obj, dict):
            return {key: serialize_task_template(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [serialize_task_template(item) for item in obj]
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()  
        else:
            return obj  

    excess_task_template = {
        "type": "task",
        "voteAcceptCount": 0,
        "voteRejectCount": 0,
        "acceptedUserList": [],
        "rejectedUserList": [],
        "assignee": "Shift-A",
        "source": "Kiln Coating Module",
        "team": "Operations",
        "createdBy": "60599de3a382f577ef0e38c1",
        "createdOn": "2024-12-07T20:46:29.000Z",
        "siteId": "65ae141fd0928341551f8695",
        "dataTagId" : "SDCW2_Kiln_Excess_Coating_Score",
        "subTaskOf": None,
        "subTasks": [],
        "chats": [],
        "chatOf": None,
        "taskPriority": "medium",
        "updateHistory": [
            {
                "action": "Pulse created this task",
                "by": "60599de3a382f577ef0e38c1",
                "on": "2024-12-07T20:46:29.000Z"
            }
        ],
        "unitsId": "65ae141fd0928341551f8695",
        "collaborators": ["60599de3a382f577ef0e38c1"],
        "equipmentIds": [],
        "status": "inprogress",
        "content": [{

          "type": "title",

          "value": "Excess Coating in Kiln"

        },

        {

          "type": "boldtext",

          "value": "KPI"

    }],
        "taskGeneratedBy": "Pulse 2",
        "incidentId": "",
        "category": "",
        "sourceURL": "",
        "notifyEmailIds": ["somasundaram.s@exactspace.co"],
        "lastUpdatedOn": "2024-12-07T20:49:47.531Z",
        "chat": [],
        "taskDescription": "&lt;p&gt;Kiln got coating issues&lt;/p&gt;",
        "viewedUsers": [],
        "mentions": [],
        "systems": [],
        "completedBy": "Somasundaram S"
    }
    less_task_template = {
        "type": "task",
        "voteAcceptCount": 0,
        "voteRejectCount": 0,
        "acceptedUserList": [],
        "rejectedUserList": [],
        "assignee": "Shift-A",
        "source": "Kiln Coating Module",
        "team": "Operations",
        "createdBy": "60599de3a382f577ef0e38c1",
        "createdOn": "2024-12-07T20:46:29.000Z",
        "siteId": "65ae141fd0928341551f8695",
        "dataTagId" : "SDCW2_Kiln_Less_Coating_Score",
        "subTaskOf": None,
        "subTasks": [],
        "chats": [],
        "chatOf": None,
        "taskPriority": "medium",
        "updateHistory": [
            {
                "action": "Pulse created this task",
                "by": "60599de3a382f577ef0e38c1",
                "on": "2024-12-07T20:46:29.000Z"
            }
        ],
        "unitsId": "65ae141fd0928341551f8695",
        "collaborators": ["60599de3a382f577ef0e38c1"],
        "equipmentIds": [],
        "status": "inprogress",
        "content": [{

          "type": "title",

          "value": "Less Refractory Coating in Kiln"

        },

        {

          "type": "boldtext",

          "value": "KPI"

    }],
        "taskGeneratedBy": "Pulse 2",
        "incidentId": "",
        "category": "",
        "sourceURL": "",
        "notifyEmailIds": ["somasundaram.s@exactspace.co"],
        "lastUpdatedOn": "2024-12-07T20:49:47.531Z",
        "chat": [],
        "taskDescription": "&lt;p&gt;Kiln got coating issues&lt;/p&gt;",
        "viewedUsers": [],
        "mentions": [],
        "systems": [],
        "completedBy": "Somasundaram S"
    }

    excess_coating = []
    less_refractory = []
    activity_url = "http://10.13.1.75/exactapi/activities"
    localized_time = last_row['time'].replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    image_path = "kiln_shell_temperature_pattern_"+str(localized_time)+".png"
    size = os.path.getsize(image_path)

    for col in temp_tags:
        column_name = col + ' Coating Score'
        percentage = last_row[column_name]
        area = col  

        if percentage < -2:
            less_refractory.append({
                "Time": last_row['time'],
                "Area": area[17:],
                "Excess Percentage": f"{round(abs(percentage),1)}%"  # Convert to positive for display
            })
        elif percentage > 2:
            excess_coating.append({
                "Time": last_row['time'],
                "Area": area[17:],
                "Excess Percentage": f"{round(percentage,1)}%"  # Positive percentages
            })
    
    
    if excess_coating:
        excess_task_template["createdOn"] = localized_time
        excess_body = excess_task_template["content"]
        excess_task_template["lastUpdatedOn"] = localized_time
        excess_task_template["updateHistory"][0]["on"] = localized_time
        
        excess_body = {
            "type": "table",
            "value": [
                ["Time", "Area", "Excess Percentage"]
            ] + [[localized_time[:16].replace('T',' '), row["Area"], row["Excess Percentage"]] for row in excess_coating]  
        }
        excess_task_template["attachments"] = [
      {
        "file_name": image_path,
        "size": size,
        "type": "image/png"
      }
    ]

        excess_task_template["content"].append(excess_body)
        excess_task_template = serialize_task_template(excess_task_template)

        excess_task_template = json.dumps(excess_task_template, indent=4)
        # print(excess_task_template)
        

    # If less refractory coating is found
    if less_refractory:
        less_body = less_task_template["content"]
        less_task_template["createdOn"] = localized_time
        less_task_template["lastUpdatedOn"] = localized_time
        less_task_template["updateHistory"][0]["on"] = localized_time
        less_body={
            "type": "table",
            "value": [
                ["Time", "Area", "Less Refractory Percentage"]
            ] + [[localized_time[:16].replace('T',' '), row["Area"], row["Excess Percentage"]] for row in less_refractory]  # Adding dynamic rows here
        }
        less_task_template["attachments"] = [
      {
        "file_name": image_path,
        "size": size,
        "type": "image/png"
      }
    ]
        less_task_template["content"].append(less_body)
        less_task_template = serialize_task_template(less_task_template)

        less_task_template = json.dumps(less_task_template, indent=4)
       
    
    def publish_task(template):
        print(activity_url)
        activity_response = requests.post(activity_url, json=json.loads(template))
        print(f"Body Posted: {activity_response.status_code}")
        #print(template)

    if case_num == 4:
        publish_task(excess_task_template)
        publish_task(less_task_template)
    elif case_num == 3:
        publish_task(less_task_template)
    elif case_num == 2:
        publish_task(excess_task_template)
    else:
        print('yoyo no case')
        


def generate_temp_pattern(last_row):

    x = [tag[17:] for tag in temp_tags]
    y = last_row[temp_tags].values
    localized_time = last_row['time'].replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    plt.figure(figsize=(12, 5))
    sns.set_theme(style="whitegrid")

    sns.lineplot( x=x, y=y, marker="o", markersize=10, linewidth=2, color="#1f77b4", label="Temperature",)

    for i, value in enumerate(y):
        plt.text( x[i], value + 2, f"{value:.1f}", ha="center", fontsize=10, color="black",)

    plt.title( f"Kiln Shell Temperature Pattern at {localized_time[:16].replace('T',' ')}", fontsize=16, weight="bold", pad=15,)
    plt.xlabel("Kiln Shell Sections", fontsize=14)
    plt.ylabel("Temperature (°C)", fontsize=14)
    plt.xticks(rotation=20, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12)
    plt.tight_layout()

    output_path = "kiln_shell_temperature_pattern_"+str(localized_time)+".png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")

    # plt.show()
    print(f"Plot saved as {output_path}")

    files = {'upload_file': open(str(output_path),'rb')}
    url="http://10.13.1.75/exactapi/attachments/tasks/upload/"
    response = requests.post(url, files=files)
    print(response)

def Kiln_Coating_Score():
    try:
        df_temp = calculate_temp_score(temp_tags, flow_tag)
        #return df_temp, df_temp1
        df_process = calculate_process_score(process_tags)
        #df_quality = calculate_quality_score(quality_tags, start, end)
        
        if df_temp is not None and not df_temp.empty:
            df_temp['score_diff_temp'] = df_temp['coating_score_temp'].diff().fillna(0)
            df_temp['time_diff_hours_temp'] = df_temp['time'].diff().dt.total_seconds() / 3600
            df_temp['time_diff_hours_temp'] = df_temp['time_diff_hours_temp'].fillna(0)
            df_temp['time_diff_hours_temp'].iloc[0] = 9
            df_temp['reset_flag_temp'] = (df_temp['time_diff_hours_temp'] > 3).astype(int)
            spike_threshold, alpha = 1.5, 0.3
            df_temp = smooth_scores(df_temp, 'score_diff_temp', 'reset_flag_temp', 'coating_score_temp', 'final_coating_score_temp_smoothed',spike_threshold, alpha)
            df_temp = smooth_scores(df_temp, 'score_diff_temp', 'reset_flag_temp', 'coating_score_temp_excess', 'final_excess_coating_score_temp_smoothed',spike_threshold, alpha)
            df_temp = smooth_scores(df_temp, 'score_diff_temp', 'reset_flag_temp', 'coating_score_temp_less', 'final_less_coating_score_temp_smoothed',spike_threshold, alpha)


            # df_temp['final_coating_score_temp_smoothed'] = df_temp.apply(
            #     lambda row: calculate_score(row, 'score_diff_temp', 'reset_flag_temp', 'coating_score_temp'), axis=1)# Smooth temperature score
            # df_temp['final_excess_coating_score_temp_smoothed'] = df_temp.apply(
            #     lambda row: calculate_score(row, 'score_diff_temp', 'reset_flag_temp', 'coating_score_temp_excess'), axis=1)# Smooth temperature score
            # df_temp['final_less_coating_score_temp_smoothed'] = df_temp.apply(
            #     lambda row: calculate_score(row, 'score_diff_temp', 'reset_flag_temp', 'coating_score_temp_less'), axis=1)# Smooth temperature score
            for tag in temp_tags:
                coating_score_col = f"{tag} Coating Score"
                score_diff_col = f"{tag}_score_diff"
                smoothed_score_col = f"{tag} Coating Score_smoothed"
                
                df_temp[score_diff_col] = df_temp[coating_score_col].diff().fillna(0) #1
                df_temp = smooth_scores(df_temp, score_diff_col, 'reset_flag_temp', coating_score_col, smoothed_score_col,spike_threshold, alpha)

                # df_temp[smoothed_score_col] = df_temp.apply(
                #     lambda row: calculate_score(row, score_diff_col, 'reset_flag_temp', coating_score_col), axis=1
                # )
            #df_temp['final_coating_score_temp_smoothed'] = df_temp['coating_score_temp']
            # df_temp['final_excess_coating_score_temp_smoothed'] = df_temp['coating_score_temp_excess']
            # df_temp['final_less_coating_score_temp_smoothed'] = df_temp['coating_score_temp_less']
            df_temp.set_index('time', inplace=True)
            df_temp['final_coating_score_temp_smoothed'] = df_temp['final_coating_score_temp_smoothed'].rolling(window='6H', min_periods=0).mean().ffill()
            df_temp['final_excess_coating_score_temp_smoothed'] = df_temp['final_excess_coating_score_temp_smoothed'].rolling(window='6H', min_periods=0).mean().ffill()
            df_temp['final_less_coating_score_temp_smoothed'] = df_temp['final_less_coating_score_temp_smoothed'].rolling(window='6H', min_periods=0).mean().ffill()

            df_temp.reset_index(inplace=True)
            
        else:
            df_temp = pd.DataFrame(columns=['time','final_coating_score_temp_smoothed'])
         
        
        if df_process is not None and not df_process.empty:
            # For process score
            df_process['score_diff_process'] = df_process['coating_score_process'].diff().fillna(0)
            df_process['time_diff_hours_process'] = df_process['time'].diff().dt.total_seconds() / 3600
            df_process['time_diff_hours_process'] = df_process['time_diff_hours_process'].fillna(0)
            df_process['time_diff_hours_process'].iloc[0] = 9
            df_process['reset_flag_process'] = (df_process['time_diff_hours_process'] > 3).astype(int)
            spike_threshold, alpha = 1.5, 0.1
            # df_process['final_coating_score_process_smoothed'] = df_process.apply(
            #     lambda row: calculate_score(row, 'score_diff_process', 'reset_flag_process', 'coating_score_process'), axis=1) # Smooth process score
            df_process = smooth_scores(df_process, 'score_diff_process', 'reset_flag_process', 'coating_score_process', 'final_coating_score_process_smoothed', spike_threshold, alpha)
            #df_process['final_coating_score_process_smoothed'] = df_process['coating_score_process']
            df_process.set_index('time', inplace=True)

            df_process['final_coating_score_process_smoothed'] = df_process['final_coating_score_process_smoothed'].rolling(window='1H', min_periods=0).mean().ffill()
            df_process.reset_index(inplace=True)

        else:
            df_process = pd.DataFrame(columns=['time','final_coating_score_process_smoothed'])

        # if df_quality is not None and not df_quality.empty:
        #     # For quality score
        #     df_quality['score_diff_quality'] = df_quality['coating_score_quality'].diff().fillna(0)
        #     df_quality['time_diff_hours_quality'] = df_quality['time'].diff().dt.total_seconds() / 3600
        #     df_quality['time_diff_hours_quality'] = df_quality['time_diff_hours_quality'].fillna(0)
        #     df_quality['time_diff_hours_quality'].iloc[0] = 9
        #     df_quality['reset_flag_quality'] = (df_quality['time_diff_hours_quality'] > 3).astype(int)
        #     spike_threshold, alpha = 1.5, 0.3
        #     # df_quality['final_coating_score_quality_smoothed'] = df_quality.apply(
        #     #     lambda row: calculate_score(row, 'score_diff_quality', 'reset_flag_quality', 'coating_score_quality'), axis=1) # Smooth quality score
        #     df_quality = smooth_scores(df_quality, 'score_diff_quality', 'reset_flag_quality', 'coating_score_quality', 'final_coating_score_quality_smoothed',spike_threshold, alpha)

        #     df_quality.set_index('time', inplace=True)
        #     df_quality['final_coating_score_quality_smoothed'] = df_quality['final_coating_score_quality_smoothed'].rolling(window='1D', min_periods=0).mean().ffill()
        #     df_quality.reset_index(inplace=True)
        # else:
        #     df_quality = pd.DataFrame(columns=['time','final_coating_score_quality_smoothed'])

                # Merge df_temp and df_process using an outer join
        merged_temp_process = df_temp[['time', 'final_coating_score_temp_smoothed','final_excess_coating_score_temp_smoothed','final_less_coating_score_temp_smoothed']].merge(
            df_process[['time', 'final_coating_score_process_smoothed']],
            on='time',
            how='outer'
        )
        merged_temp_process = merged_temp_process.sort_values(by='time', ascending=True)
        # Drop rows where both columns are NaN
        merged_temp_process = merged_temp_process.dropna(
            subset=['final_coating_score_temp_smoothed', 'final_coating_score_process_smoothed'],
            how='all'
        )

        # Merge the resulting DataFrame with df_quality
        # df = merged_temp_process.merge(
        #     df_quality[['time', 'final_coating_score_quality_smoothed']],
        #     on='time',
        #     how='outer'  
        # )
        #return df_temp
        df = merged_temp_process.sort_values(by='time', ascending=True)
        # print(df_temp['time'].tail(1))
        # print(df_process['time'].tail(1))
        # print(df_quality['time'].tail(1))
        # print(df.tail(1)) 
        if df is not None and not df.empty:
            df['combined_score'] = df.apply(calculate_combined_score, axis=1)
            last_row = df.iloc[-1]
            time = last_row['time']
            now_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            df_last_datetime = datetime.strptime(time.strftime('%Y-%m-%dT%H:%M:%S.000Z'), '%Y-%m-%dT%H:%M:%S.000Z')
            now_datetime = datetime.strptime(now_time, '%Y-%m-%dT%H:%M:%S.000Z')
            
            combined_score = last_row['combined_score']
            shell_temp_score = last_row['final_coating_score_temp_smoothed']
            process_score = last_row['final_coating_score_process_smoothed']
            excess_temp_score = last_row['final_excess_coating_score_temp_smoothed']
            less_temp_score = last_row['final_less_coating_score_temp_smoothed']
            #quality_score = last_row['final_coating_score_quality_smoothed']
            
            # csv_path = 'abnormalities_process_logic2.csv'
            # save_abnormalities_to_csv(df_process, csv_path, 'time', 'final_coating_score_process_smoothed', 'abnormalities_process')

            # csv_path = 'abnormalities_quality_logic2.csv'
            # save_abnormalities_to_csv(df_quality, csv_path, 'time', 'final_coating_score_quality_smoothed', 'abnormalities_quality')

            # csv_path = 'abnormalities_shell_temp_logic2.csv'
            # save_abnormalities_to_csv(df_temp, csv_path, 'time', 'final_coating_score_temp_smoothed', 'abnormalities_shell_temp')
            # topic_line = "u/"+unitId+"/"
            if excess_temp_score <= 0.5 and less_temp_score <= 0.5 : #both are fine
                case_num = 1
            elif excess_temp_score > 0.5 and less_temp_score <= 0.5: #excess coating
                case_num = 2
            elif excess_temp_score <= 0.5 and less_temp_score > 0.5 : #less coating
                case_num = 3
            else: # both are abnormal
                case_num = 4

            #print(df_last_datetime.date(),now_datetime.date(),df_last_datetime.hour,now_datetime.hour)
            if df_last_datetime.date() == now_datetime.date() and df_last_datetime.hour == now_datetime.hour:
                
                if case_num != 1:
                    last_row_temp = df_temp.iloc[-1]
                    generate_temp_pattern(last_row_temp)
                    generate_and_post_task_body(last_row_temp, case_num) 
                #return df, df_temp, df_process

                if not np.isnan(combined_score) :
                    postData(coating_score_tag,time,round(combined_score,2))

                else:
                    print('NaN',time)

                scores = {
                    shell_temp_score_tag: last_row['final_coating_score_temp_smoothed'],
                    process_score_tag: last_row['final_coating_score_process_smoothed'],
                    # quality_score_tag: quality_score,  # Uncomment if needed
                    excess_temp_score_tag: last_row['final_excess_coating_score_temp_smoothed'],
                    less_temp_score_tag: last_row['final_less_coating_score_temp_smoothed']
                }

                last_row_temp = df_temp.iloc[-1]
                for tag in temp_tags:
                    scores[tag+'_Coating_Score'] = last_row_temp[tag+' Coating Score_smoothed']

                for tag, score in scores.items():
                    if not np.isnan(score):
                        postData(tag, time, round(score, 2))
                             
        else:
            print('df is empty')    
        
        #return df, df_temp, df_process
    except Exception as e:
        print(e)
        traceback.print_exc()


def Kiln_Coating_Risk():
    try:
        df_quality = calculate_coating_risk_quality(quality_tags)
        #return df_quality
        if df_quality is not None and not df_quality.empty:
            # For quality score
            df_quality['score_diff_quality'] = df_quality['coating_score_quality'].diff().fillna(0)
            df_quality['time_diff_hours_quality'] = df_quality['time'].diff().dt.total_seconds() / 3600
            df_quality['time_diff_hours_quality'] = df_quality['time_diff_hours_quality'].fillna(0)
            df_quality['time_diff_hours_quality'].iloc[0] = 9
            df_quality['reset_flag_quality'] = (df_quality['time_diff_hours_quality'] > 3).astype(int)
            spike_threshold, alpha = 1.5, 0.3
            # df_quality['final_coating_score_quality_smoothed'] = df_quality.apply(
            #     lambda row: calculate_score(row, 'score_diff_quality', 'reset_flag_quality', 'coating_score_quality'), axis=1) # Smooth quality score
            df_quality = smooth_scores(df_quality, 'score_diff_quality', 'reset_flag_quality', 'coating_score_quality', 'final_coating_score_quality_smoothed',spike_threshold, alpha)

            df_quality.set_index('time', inplace=True)
            df_quality['final_coating_score_quality_smoothed'] = df_quality['final_coating_score_quality_smoothed'].rolling(window='1D', min_periods=0).mean().ffill()
            df_quality.reset_index(inplace=True)
        else:
            df_quality = pd.DataFrame(columns=['time','final_coating_score_quality_smoothed'])
        #return df_quality
                      
    except Exception as e:
        print(e)
        traceback.print_exc()


# start_date = datetime.strptime("17-11-2024 00:00", "%d-%m-%Y %H:%M")
# end_date = datetime.strptime("10-12-2024 20:00", "%d-%m-%Y %H:%M")
Kiln_Coating_Score()
# start_date = datetime.strptime("30-06-2023 00:00", "%d-%m-%Y %H:%M")
# end_date = datetime.strptime("04-12-2024 20:00", "%d-%m-%Y %H:%M")
# Kiln_Coating_Risk()
