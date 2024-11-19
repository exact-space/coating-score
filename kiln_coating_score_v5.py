import paho.mqtt.client as mqtt
import requests
import json
import pandas as pd
import numpy as np
import time
import platform
import datetime
from datetime import datetime, timedelta
from collections import Counter
import os
import timeseries as ts
import math
qr = ts.timeseriesquery()
import schedule
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

unitId='65ae141fd0928341551f8695'

client = mqtt.Client()
port = 1883

print("Running port", port)

config = cfg.getconfig()
cfg = config["api"]["meta"]
broker = config["BROKER_ADDRESS"]
getKairosUrl =config["api"]["query"]
global url1
url1 = getKairosUrl
print(url1)
topic_line = "u/"+unitId+"/"

BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS")
if not BROKER_ADDRESS:
    BROKER_ADDRESS = config["BROKER_ADDRESS"]

def get_data(tag):
    qr = ts.timeseriesquery()
    qr.addMetrics(tag)
    qr.chooseTimeType("relative",{"start_value":4, "start_unit":"weeks"}) #  30 mins,15 datapoints
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
        print(e)
        print(time.time())
        df = pd.DataFrame()
        return df

def postData(tag,time,val,topic_line):
    rounded_hour = time.replace(minute=0, second=0, microsecond=0)
    epoch_time = int(rounded_hour.timestamp() * 1000)
    # epoch_time = 1731907910000
    body=[{"name":tag,"datapoints":[[epoch_time,val]], "tags" : {"type":"simulation"}}]
    
    postBody = [{'t':float(epoch_time),'v':float(val)}]
    print(postBody)
    print(topic_line+tag+"/r",postBody)
    client.publish(topic_line+tag+"/r",json.dumps(postBody))
    
    qr.postDataPacket(body)

def calculate_combined_score(row):
    temp_score = row['final_coating_score_temp_smoothed']
    process_score = row['final_coating_score_process_smoothed']
    quality_score = row['final_coating_score_quality_smoothed']
    
    if pd.isna(quality_score):  # No quality data
        if pd.isna(process_score) or temp_score < 1:  # No process data or temp < 1
            combined_score = temp_score * 1  # Temp 100%
        else:  # Use temp and process only
            combined_score = temp_score * 0.6 + process_score * 0.4
    elif pd.isna(process_score) or temp_score < 1:  # No process data or temp < 1
        combined_score = temp_score * 0.6 + quality_score * 0.4
    else:  # All data present and temp > 1
        combined_score = temp_score * 0.4 + process_score * 0.3 + quality_score * 0.3 # 5 2 3 
    
    return combined_score
    
def calculate_score(row, diff_col, reset_flag_col, initial_score_col):
    previous_score = float(row[initial_score_col])
    
    if row[reset_flag_col] == 1:
        previous_score = float(row[initial_score_col])  # Reset score after time gap
        return previous_score
    else:
        if row[diff_col] >= 1.5:
            diff_value = row[diff_col] * 0.1
        else:
            diff_value = row[diff_col] * 0.3
        
        previous_score += diff_value
        return previous_score

def calculate_temp_score(temp_tags, flow_tag):
    TEMP_THRESHOLD = 19
    def validate_temp(row, last_valid_values, out_of_range_columns):
        valid_row = row.copy()
        columns_with_variation = []

        for col in last_valid_values.keys():
            current_value = row[col]
            last_valid_value = last_valid_values[col]

            # If no last valid value is available, initialize it with the current value
            if last_valid_value is None:
                last_valid_values[col] = current_value
            else:
                # Define the valid range around the last valid value
                lower_bound = last_valid_value - TEMP_THRESHOLD
                upper_bound = last_valid_value + TEMP_THRESHOLD
                #     327 <= 305 no  327 >= 345 yes. if 327 .. 
                # abs(327 - 305)
                # Check if the current value is outside the valid range
                if current_value <= lower_bound or current_value >= upper_bound:
                #if abs(current_value - last_valid_value) <= TEMP_THRESHOLD:
                    # Mark this column as out of range
                    columns_with_variation.append(col)
                    # Hold the last known valid value if out of range
                    valid_row[col] = last_valid_value
                else:
                    # If the current value is back within range, update the last valid value
                    last_valid_values[col] = current_value
                    # Remove the column from out_of_range_columns if it was previously out of range
                    out_of_range_columns.discard(col)

        # Apply correction only if six or more columns exceed the threshold
        if len(columns_with_variation) >= 6:
            out_of_range_columns.update(columns_with_variation)
            return valid_row
        else:
            # Reset out_of_range_columns if fewer than 6 columns exceed the threshold
            out_of_range_columns.clear()
            return row  # Return unmodified row if conditions are not met
    
    def correct_temperature_data(df_temp, temp_tags):
        df_temp = df_temp.sort_values(by='time').reset_index(drop=True)

        last_valid_values = {col: None for col in temp_tags}
        out_of_range_columns = set()

        valid_rows = df_temp.apply(lambda row: validate_temp(row, last_valid_values, out_of_range_columns), axis=1)
        valid_rows = valid_rows.dropna()  

        return valid_rows

    def calculate_coating_score(row, temp_col, ideal_temp_min, ideal_temp_med, ideal_temp_max):
        rolling_mean = row[temp_col]
        if rolling_mean < ideal_temp_min:
            value = abs((rolling_mean - ideal_temp_min) / ideal_temp_min * 100)
        elif rolling_mean > ideal_temp_max:
            value = -abs((rolling_mean - ideal_temp_max) / ideal_temp_max * 100)
        else:
            return 0
        if abs(value) > 10:
            value = value * 0.1

        return value

    def calculate_overall_coating_score(row, temp_tags):
        negative_scores = []
        positive_scores = []

        for temp_col in temp_tags:
            score = row[f'{temp_col} Coating Score']
            if score < 0:
                negative_scores.append(score)
            elif score >= 0:
                positive_scores.append(score)

        if len(negative_scores) > 6: # Check if more than 6 coating scores are negative
            return sum(negative_scores) / len(ideal_temp_min)
        elif len(positive_scores) >= 6: #if more than 6 coating scores are positive
            return sum(positive_scores) / len(ideal_temp_min)
        else: # If neither condition is met, return 0
            return 0
    
    df_temp = get_data([flow_tag]+temp_tags)
    #print(df_temp.tail(1))
    if df_temp is not None and not df_temp.empty:
        df_temp['time'] = pd.to_datetime(df_temp['time']/1000, unit="s")
        # Filter data based on 'Kiln Total Feed'
        df_temp = df_temp[(df_temp[flow_tag] > 300)]

        df_temp = df_temp[['time']+temp_tags]

        for temp in temp_tags:
            df_temp = df_temp[(df_temp[temp] < 430) & (df_temp[temp] > 100)]
        
        if df_temp is None or df_temp.empty:
            return pd.DataFrame(columns=['time','coating_score_temp'])
        else:
            df_temp = correct_temperature_data(df_temp, temp_tags)
            #print(df_temp.tail(1)) 1830
            df_temp[temp_tags] = df_temp[temp_tags].rolling(window=60, min_periods=1).mean()
            df_temp.set_index('time', inplace=True)
            df_temp = df_temp.resample('60T').mean().dropna()
            df_temp.reset_index(inplace=True)
            #print(df_temp.tail(1)) 1800
            for temp_col in temp_tags:
                df_temp[f'{temp_col} Coating Score'] = df_temp.apply(calculate_coating_score, axis=1, 
                    temp_col=temp_col, 
                    ideal_temp_min=ideal_temp_min[temp_col], 
                    ideal_temp_med=ideal_temp_med[temp_col], 
                    ideal_temp_max=ideal_temp_max[temp_col])
            
            # Overall coating score with weights
            weighted_scores = []
            for temp_col in temp_tags:
                weighted_scores.append(df_temp[f'{temp_col} Coating Score'] * weights[temp_col])
            
            df_temp['coating_score_temp'] = df_temp.apply(calculate_overall_coating_score, axis=1, temp_tags=temp_tags)
            return df_temp

    else:
        return pd.DataFrame(columns=['time','coating_score_temp'])

def calculate_process_score(process_tags):
    def calculate_score_process(row):
        score = 0
        if (row['Kiln Inlet Pressure'] <= -55.21) and (row['PH RPM to Kiln Inlet Pressure'] < -0.646):
            if (row['Cooler id rpm'] >= 689.900148):
                score += abs(row['Cooler id rpm'] - 689.900148) * 0.01
    
            score += abs(row['Kiln Inlet Pressure'] + 55.21) * 0.1
            score += abs(row['PH RPM to Kiln Inlet Pressure'] + 0.646) * 0.8
    
            if row['Kiln Inlet CO'] >= 31.442821:
                if (row['Kiln Total Feed'] > 319.7):
                    if (row['Kiln Coal Ratio'] > 0.323) and (row['Total coal consumption'] > 22.5):
                        score += abs(row['Kiln Coal Ratio'] - 0.323) * 6 # check the score and logic again. why calculating score for coal when it is normal/high . #only kiln co acting with inlet press
            else:
                if (row['Kiln Total Feed'] <= 319.7):
                    if (row['Kiln Coal Ratio'] <= 0.323) and (row['Total coal consumption'] <= 22.5):
                        score += abs(row['Kiln Coal Ratio'] - 0.323) * 6
        
        return score

    df_process = get_data(process_tags)

    if df_process is not None and not df_process.empty:
        df_process = df_process.rename(columns=tagMapper_rolling)
        #df_process['time'] = df_process['time'].str[:19]
        df_process['time'] = pd.to_datetime(df_process['time']/1000, unit="s")

        # Filter data based on 'Kiln Total Feed'
        df_process = df_process[(df_process['Kiln Total Feed'] > 300)]

        if df_process is None or df_process.empty:
            return pd.DataFrame(columns=['time','coating_score_process'])
        else:
            df_process['PH RPM to Kiln Inlet Pressure'] = np.where(df_process['Kiln Inlet Pressure'] != 0, df_process['Kiln Inlet Pressure'] / df_process['PH RPM'], np.nan) * 10

            process_columns = ['Kiln Inlet Pressure','PH RPM to Kiln Inlet Pressure','Cooler id rpm','Kiln Inlet CO','Kiln Total Feed','Kiln Coal Ratio','Total coal consumption']

            df_process = df_process[['time']+process_columns]

            df_process.loc[:, df_process.columns != 'time'] = df_process.loc[:, df_process.columns != 'time'].rolling(window=60).mean()
            df_process = df_process.dropna(how='all', subset=df_process.columns.difference(['time']))
            df_process.set_index('time', inplace=True)
            df_process = df_process.resample('60T').mean().dropna()
            df_process.reset_index(inplace=True)

            df_process['coating_score_process'] = df_process.apply(calculate_score_process, axis=1)
            return df_process

    else:
        return pd.DataFrame(columns=['time','coating_score_process'])

def calculate_quality_score(quality_tags):

    def calculate_difference(row):
        total_difference = 0
        for column in qcx_05.keys():
            if column in row:
                if not (qcx_05[column] <= row[column] <= qcx_95[column]):
                    difference = abs(row[column] - qcx_50[column])
                    if np.isnan(difference):
                        difference = 0
                    total_difference += difference
        return total_difference

    df_quality = get_data(quality_tags)

    if df_quality is not None and not df_quality.empty:
        df_quality['time'] = pd.to_datetime(df_quality['time']/1000, unit="s")
        for column in df_quality.columns:
            df_quality = df_quality[df_quality[column] != 0]

        df_quality = df_quality.reset_index(drop=True)

        df_quality = df_quality.set_index('time')
        df_quality = df_quality[~df_quality.index.duplicated(keep='first')]

        full_time_index = pd.date_range(start=df_quality.index.min(), end=df_quality.index.max(), freq='H')
        df_quality = df_quality.reindex(full_time_index)

        for col in df_quality.columns: #last known for next two days 
            mask = df_quality[col].notna() 
            df_quality[col] = df_quality[col].where(mask).fillna(method='ffill', limit=48)

        df_quality = df_quality.reset_index().rename(columns={'index': 'time'})

        df_quality = df_quality.dropna(how='all', subset=df_quality.columns.difference(['time']))
        df_quality['coating_score_quality'] = df_quality.apply(calculate_difference, axis=1)

        return df_quality
    
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

ideal_temp_max = {'SDCW2_KN_KILNTEMP38M_40M': 229.22253129346313,
 'SDCW2_KN_KILNTEMP36M_38M': 319.4736111111111,
 'SDCW2_KN_KILNTEMP30M_32M': 338.1430555555556,
 'SDCW2_KN_KILNTEMP28M_30M': 343.11944444444447,
 'SDCW2_KN_KILNTEMP23M_24-5M': 323.41590214067276,
 'SDCW2_KN_KILNTEMP21-5M_23M': 319.37555555555554,
 'SDCW2_KN_KILNTEMP18M_19-5M': 322.3601694915254,
 'SDCW2_KN_KILNTEMP12M_14M': 331.7199074074074,
 'SDCW2_KN_KILNTEMP09-5M_11M': 318.8113730929265,
 'SDCW2_KN_KILNTEMP06M_07M': 302.17385257301805,
 'SDCW2_KN_KILNTEMP00M_05M': 316.075} #75 quantile

# ideal_temp_max = {
#     'SDCW2_KN_KILNTEMP38M_40M_30_rollingMean': 273.5343517912027, 
#     'SDCW2_KN_KILNTEMP36M_38M_30_rollingMean': 346.4309701492537,
#     'SDCW2_KN_KILNTEMP30M_32M_30_rollingMean': 340.8412213740458, 
#     'SDCW2_KN_KILNTEMP28M_30M_30_rollingMean': 347.4320237466538,
#     'SDCW2_KN_KILNTEMP23M_24-5M_30_rollingMean': 331.3298465412765, 
#     'SDCW2_KN_KILNTEMP21-5M_23M_30_rollingMean': 325.25291608715804,
#     'SDCW2_KN_KILNTEMP18M_19-5M_30_rollingMean': 329.0513888888889, 
#     'SDCW2_KN_KILNTEMP12M_14M_30_rollingMean': 353.6830555555555,
#     'SDCW2_KN_KILNTEMP09-5M_11M_30_rollingMean': 342.58338014981274, 
#     'SDCW2_KN_KILNTEMP06M_07M_30_rollingMean': 316.979440316395,
#     'SDCW2_KN_KILNTEMP00M_05M_30_rollingMean': 323.5035034506262
# } #95 quantile
#weights for each temperature column
qcx_05 = {'SDCW2_QCX_KN_LSF': 98.16,
 'SDCW2_QCX_KN_R_90micron': 18.78,
 'SDCW2_QCX_CLK_Liquid': 26.36,
 'SDCW2_QCX_CLK_CaO': 61.02,
 'SDCW2_QCX_CLK_Al2O3': 5.04,
 'SDCW2_QCX_HM_Na2O': 0.16,
 'SDCW2_QCX_HM_LOI-KF': 35.97,
 'SDCW2_QCX_Coatability_Index_Calc': 29.93,
 'SDCW2_QCX_Hydraulic_Ratio_Calc': 11.326629860777723}

qcx_50 = {'SDCW2_QCX_KN_LSF': 99.51,
 'SDCW2_QCX_KN_R_90micron': 20.17,
 'SDCW2_QCX_CLK_Liquid': 26.54,
 'SDCW2_QCX_CLK_CaO': 61.25,
 'SDCW2_QCX_CLK_Al2O3': 5.08,
 'SDCW2_QCX_HM_Na2O': 0.29,
 'SDCW2_QCX_HM_LOI-KF': 36.47,
 'SDCW2_QCX_Coatability_Index_Calc': 30.18,
 'SDCW2_QCX_Hydraulic_Ratio_Calc': 11.586880466472302}

qcx_95 = {'SDCW2_QCX_KN_LSF': 100.68,
 'SDCW2_QCX_KN_R_90micron': 21.73,
 'SDCW2_QCX_CLK_Liquid': 26.79,
 'SDCW2_QCX_CLK_CaO': 61.46,
 'SDCW2_QCX_CLK_Al2O3': 5.18,
 'SDCW2_QCX_HM_Na2O': 0.42,
 'SDCW2_QCX_HM_LOI-KF': 36.54,
 'SDCW2_QCX_Coatability_Index_Calc': 30.48,
 'SDCW2_QCX_Hydraulic_Ratio_Calc': 11.735849649350058}
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

def Kiln_Coating_Score():
    try:
        unitId = '65ae141fd0928341551f8695'
        flow_tag = 'SDCW2_KN_Kiln Feed Act'
        coating_score_tag = 'SDCW2_Kiln_Coating_Score'
        temp_tags = ['SDCW2_KN_KILNTEMP00M_05M','SDCW2_KN_KILNTEMP06M_07M','SDCW2_KN_KILNTEMP09-5M_11M','SDCW2_KN_KILNTEMP12M_14M','SDCW2_KN_KILNTEMP18M_19-5M','SDCW2_KN_KILNTEMP21-5M_23M','SDCW2_KN_KILNTEMP23M_24-5M','SDCW2_KN_KILNTEMP28M_30M','SDCW2_KN_KILNTEMP30M_32M','SDCW2_KN_KILNTEMP36M_38M','SDCW2_KN_KILNTEMP38M_40M']
        process_tags = ['SDCW2_KN_462KL01_PT01', 'SDCW2_KN_Press_Cl_ID_RPM_Calc_30_rollingMean', 'SDCW2_CL_472FNB_ST01', 'SDCW2_KN_462GA1_AT01', 'SDCW2_KN_Kiln Feed Act', 'SDCW2_Kiln_Coal_Ratio', 'SDCW2_KN_L2_TOTAL_COAL_TPH','SDCW2_KN_442FN1_VFD_ST01']
        #process_columns = ['Kiln Inlet Pressure', 'Cooler id rpm','Kiln Inlet CO','Kiln Total Feed','Kiln Coal Ratio','Total coal consumption']
        quality_tags = ['SDCW2_QCX_KN_LSF','SDCW2_QCX_KN_R_90micron','SDCW2_QCX_CLK_Liquid','SDCW2_QCX_CLK_CaO','SDCW2_QCX_CLK_Al2O3','SDCW2_QCX_HM_Na2O','SDCW2_QCX_HM_LOI-KF','SDCW2_QCX_Coatability_Index_Calc','SDCW2_QCX_Hydraulic_Ratio_Calc']

        df_temp = calculate_temp_score(temp_tags, flow_tag)
        df_process = calculate_process_score(process_tags)
        df_quality = calculate_quality_score(quality_tags)
        
        if df_temp is not None and not df_temp.empty:
            df_temp['score_diff_temp'] = df_temp['coating_score_temp'].diff().fillna(0)
            df_temp['time_diff_hours_temp'] = df_temp['time'].diff().dt.total_seconds() / 3600
            df_temp['time_diff_hours_temp'] = df_temp['time_diff_hours_temp'].fillna(0)
            df_temp['reset_flag_temp'] = (df_temp['time_diff_hours_temp'] > 3).astype(int)
            
            df_temp['final_coating_score_temp_smoothed'] = df_temp.apply(
                lambda row: calculate_score(row, 'score_diff_temp', 'reset_flag_temp', 'coating_score_temp'), axis=1)# Smooth temperature score
            df_temp.set_index('time', inplace=True)
            df_temp['final_coating_score_temp_smoothed'] = df_temp['final_coating_score_temp_smoothed'].rolling(window='6H', min_periods=0).mean().ffill()
            df_temp.reset_index(inplace=True)
            
        else:
            df_temp = pd.DataFrame(columns=['time','final_coating_score_temp_smoothed'])

        if df_process is not None and not df_process.empty:
            # For process score
            df_process['score_diff_process'] = df_process['coating_score_process'].diff().fillna(0)
            df_process['time_diff_hours_process'] = df_process['time'].diff().dt.total_seconds() / 3600
            df_process['time_diff_hours_process'] = df_process['time_diff_hours_process'].fillna(0)
            df_process['reset_flag_process'] = (df_process['time_diff_hours_process'] > 3).astype(int)

            df_process['final_coating_score_process_smoothed'] = df_process.apply(
                lambda row: calculate_score(row, 'score_diff_process', 'reset_flag_process', 'coating_score_process'), axis=1) # Smooth process score

            df_process.set_index('time', inplace=True)
            df_process['final_coating_score_process_smoothed'] = df_process['final_coating_score_process_smoothed'].rolling(window='2H', min_periods=0).mean().ffill()
            df_process.reset_index(inplace=True)

        else:
            df_process = pd.DataFrame(columns=['time','final_coating_score_process_smoothed'])

        if df_quality is not None and not df_quality.empty:
            # For quality score
            df_quality['score_diff_quality'] = df_quality['coating_score_quality'].diff().fillna(0)
            df_quality['time_diff_hours_quality'] = df_quality['time'].diff().dt.total_seconds() / 3600
            df_quality['time_diff_hours_quality'] = df_quality['time_diff_hours_quality'].fillna(0)
            df_quality['reset_flag_quality'] = (df_quality['time_diff_hours_quality'] > 3).astype(int)

            df_quality['final_coating_score_quality_smoothed'] = df_quality.apply(
                lambda row: calculate_score(row, 'score_diff_quality', 'reset_flag_quality', 'coating_score_quality'), axis=1) # Smooth quality score

            df_quality.set_index('time', inplace=True)
            df_quality['final_coating_score_quality_smoothed'] = df_quality['final_coating_score_quality_smoothed'].rolling(window='1D', min_periods=0).mean().ffill()
            df_quality.reset_index(inplace=True)
        else:
            df_quality = pd.DataFrame(columns=['time','final_coating_score_quality_smoothed'])

                # Merge df_temp and df_process using an outer join
        merged_temp_process = df_temp[['time', 'final_coating_score_temp_smoothed']].merge(
            df_process[['time', 'final_coating_score_process_smoothed']],
            on='time',
            how='outer'
        )

        # Drop rows where both columns are NaN
        merged_temp_process = merged_temp_process.dropna(
            subset=['final_coating_score_temp_smoothed', 'final_coating_score_process_smoothed'],
            how='all'
        )

        # Merge the resulting DataFrame with df_quality
        df = merged_temp_process.merge(
            df_quality[['time', 'final_coating_score_quality_smoothed']],
            on='time',
            how='left'  
        )

        # print(df_temp['time'].tail(1))
        # print(df_process['time'].tail(1))
        # print(df_quality['time'].tail(1))
        # print(df.tail(1)) 
        if df is not None and not df.empty:
            df['combined_score'] = df.apply(calculate_combined_score, axis=1)
            combined_score = df.iloc[-1]['combined_score']
            time = df.iloc[-1]['time']

            if not np.isnan(combined_score) :
                topic_line = "u/"+unitId+"/"
                postData(coating_score_tag,time,round(combined_score,2),topic_line)
                
                #publish_mqtt()
            else:
                print('NaN',time)
        else:
            print('df is empty')    
        
        #return df_temp, df_process, df_quality, df
    except Exception as e:
        print(e)
        #traceback.print_exc()
#plot_line_tag_plotly(df, 'combined_score')   

'''def job():
    print("Running coating score calculation...")
    Kiln_Coating_Score()

# Schedule the job to run every minute
schedule.every(1).minutes.do(job)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)'''

def job():
    print(f"Running coating score calculation at {datetime.now()}...")
    Kiln_Coating_Score()

while True:
    now = datetime.now()
    next_run = (now + timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
    if now.minute < 5: 
        next_run = now.replace(minute=5, second=0, microsecond=0)
    
    wait_time = (next_run - now).total_seconds()
    
    time.sleep(wait_time)
    
    job()