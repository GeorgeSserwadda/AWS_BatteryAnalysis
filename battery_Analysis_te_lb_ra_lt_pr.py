#George Sserwadda
#18-11-2024

import TAHMO
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize the API client
api = TAHMO.apiWrapper()
api.setCredentials('API-UserName', 'API-Password')

stations = [
    'TA00001', 'TA00002', 'TA00003', 'TA00004', 'TA00005', 'TA00006', 'TA00007'
]

variables = ['lb', 'te', 'lt', 'ra', 'pr', 'ld']
startDate = '2024-10-17'
endDate = '2024-11-17'

results = []
no_data_stations = []

# Define function to process each station
def process_station(station, start_date, end_date):
    try:
        df = api.getMeasurements(station, startDate=start_date, endDate=end_date, variables=variables)
        
        # Drop rows with NaN in 'lb' or 'te' columns
        df = df.dropna(subset=['lb', 'te'])
        
        if df.empty:
            return station, None  # Skip station if no valid data
        
        lb_stats = {}
        lb_stats['mean'] = round(df['lb'].mean(), 2)
        lb_stats['mode'] = round(df['lb'].mode()[0], 2) if not df['lb'].mode().empty else None
        lb_stats['std'] = round(df['lb'].std(), 2)
        lb_stats['mad'] = round(np.mean(np.abs(df['lb'] - lb_stats['mean'])), 2)
        lb_stats['cv'] = round(lb_stats['std'] / lb_stats['mean'], 2) if lb_stats['mean'] != 0 else None
        lb_stats['kurtosis'] = round(df['lb'].kurtosis(), 2)
        lb_stats['range'] = round(df['lb'].max() - df['lb'].min(), 2)
        lb_stats['iqr'] = round(df['lb'].quantile(0.75) - df['lb'].quantile(0.25), 2)
        lb_stats['skewness'] = round(df['lb'].skew(), 2)
        lb_stats['mmd'] = round(abs(lb_stats['mode'] - lb_stats['mean']), 2) if lb_stats['mode'] is not None else None
        
        # Calculate Battery Performance Index
        if all(v is not None for v in [lb_stats['mmd'], lb_stats['std'], lb_stats['mad'], lb_stats['cv'], lb_stats['iqr']]):
            lb_stats['battery_performance_index'] = round(np.mean([lb_stats['mmd'], lb_stats['std'], lb_stats['mad'], lb_stats['cv'], lb_stats['iqr']]), 2)
        else:
            lb_stats['battery_performance_index'] = None
        
        # Calculate correlations with 'lb'
        if df['lb'].nunique() > 1:
            lb_stats['correlation_with_te'] = round(df[['lb', 'te']].corr().loc['lb', 'te'], 2) if df['te'].nunique() > 1 else None
            lb_stats['correlation_with_ra'] = round(df[['lb', 'ra']].corr().loc['lb', 'ra'], 2) if df['ra'].nunique() > 1 else None
            lb_stats['correlation_with_lt'] = round(df[['lb', 'lt']].corr().loc['lb', 'lt'], 2) if df['lt'].nunique() > 1 else None
            lb_stats['correlation_with_pr'] = round(df[['lb', 'pr']].corr().loc['lb', 'pr'], 2) if df['pr'].nunique() > 1 else None
            lb_stats['correlation_with_ld'] = round(df[['lb', 'ld']].corr().loc['lb', 'ld'], 2) if df['ld'].nunique() > 1 else None
        else:
            lb_stats['correlation_with_te'] = lb_stats['correlation_with_ra'] = lb_stats['correlation_with_lt'] = None
            lb_stats['correlation_with_pr'] = lb_stats['correlation_with_ld'] = None

        # Calculate Environment Effect on Battery as a percentage using absolute values of correlations
        correlations = [lb_stats['correlation_with_te'], lb_stats['correlation_with_ra'], lb_stats['correlation_with_lt'], lb_stats['correlation_with_pr'], lb_stats['correlation_with_ld']]
        correlations = [abs(corr) for corr in correlations if corr is not None]  # Take absolute values
        lb_stats['environment_effect_on_battery'] = round(np.mean(correlations) * 100, 2) if correlations else None

        # Initialize detected_issue to None
        lb_stats['detected_issue'] = None

        # Define the Detected Issue based on specified conditions
        if lb_stats['battery_performance_index'] is not None and lb_stats['battery_performance_index'] > 10:
            if lb_stats['environment_effect_on_battery'] is not None and ((lb_stats['battery_performance_index']-lb_stats['environment_effect_on_battery'])*100/(lb_stats['battery_performance_index'])) >= 70:
                lb_stats['detected_issue'] = "Power Issue (Logger or Sensors - Site visit needed)"
            elif lb_stats['environment_effect_on_battery'] is not None and lb_stats['environment_effect_on_battery'] >= 10:
                lb_stats['detected_issue'] = "Old Batteries (all need Replacing Now)"
        elif lb_stats['battery_performance_index'] is None and lb_stats['environment_effect_on_battery'] is None:
            if lb_stats['mean'] is not None and lb_stats['mean'] > 50:
                lb_stats['detected_issue'] = "No Issue"
            elif lb_stats['mean'] is not None and lb_stats['mean'] <= 50:
                lb_stats['detected_issue'] = "Hardware Issue (faulty or needs cleaning)"
        elif lb_stats['battery_performance_index'] is not None and 0 < lb_stats['battery_performance_index'] < 10:
            if lb_stats['environment_effect_on_battery'] is not None and lb_stats['environment_effect_on_battery'] >= 10:
                lb_stats['detected_issue'] = "Unstable old Batteries (may work but all need replacing)"
        else:
            lb_stats['detected_issue'] = None

        # Save timeseries data to CSV
        filename = f'{station}_timeseries_{start_date}_to_{end_date}.csv'
        df.index.name = 'Timestamp'
        df.to_csv(filename, na_rep='', date_format='%Y-%m-%d %H:%M')
        print(f'Timeseries saved to file "{filename}"')

        return station, lb_stats

    except Exception as e:
        print(f"Error processing station {station}: {e}")
        return station, None

# Run API requests in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(process_station, station, startDate, endDate): station for station in stations}
    for future in as_completed(futures):
        station, lb_stats = future.result()
        if lb_stats is None:
            no_data_stations.append(station)
        else:
            results.append({
                'Station': station,
                'Logger Battery Mean': lb_stats['mean'],
                'Logger Battery Mode': lb_stats['mode'],
                'Logger Battery Mode-Mean Difference (MMD)': lb_stats['mmd'],
                'Logger Battery Standard Deviation (SD)': lb_stats['std'],
                'Logger Battery Mean Absolute Deviation (MAD)': lb_stats['mad'],
                'Logger Battery Coefficient of Variation (CV)': lb_stats['cv'],
                'Logger Battery Kurtosis': lb_stats['kurtosis'],
                'Logger Battery Voltage Range (VR)': lb_stats['range'],
                'Logger Battery Interquartile Range (IQR)': lb_stats['iqr'],
                'Logger Battery Skewness': lb_stats['skewness'],
                'Battery Performance Index (%)': lb_stats['battery_performance_index'],
                'LB-Temperature Correlation': lb_stats['correlation_with_te'],
                'LB-Solar Radiation Correlation': lb_stats['correlation_with_ra'],
                'LB-Logger Temperature Correlation': lb_stats['correlation_with_lt'],
                'LB-Precipitation Correlation': lb_stats['correlation_with_pr'],
                'LB-Lightning Distance Correlation': lb_stats['correlation_with_ld'],
                'Environment Effect on Battery (%)': lb_stats['environment_effect_on_battery'],
                'Detected Issue': lb_stats['detected_issue']
            })

# Save summary of results
results_df = pd.DataFrame(results).sort_values(by='Battery Performance Index (%)', ascending=False)
summary_filename = f'station_battery_summary_{startDate}_to_{endDate}.csv'
results_df.to_csv(summary_filename, index=False)
print(f'Summary saved to file "{summary_filename}"')

# Save stations with no data
if no_data_stations:
    no_data_df = pd.DataFrame(no_data_stations, columns=['Stations'])
    no_data_filename = f'no_data_stations_{startDate}_to_{endDate}.csv'
    no_data_df.to_csv(no_data_filename, index=False)
    print(f'List of stations with no data saved to file "{no_data_filename}"')

# Additional section to process no_data_df for a 12-month period
if not no_data_df.empty:
    start_date_12_months_ago = (pd.to_datetime(endDate) - pd.DateOffset(months=9)).strftime('%Y-%m-%d')
    
    longterm_results = []
    
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = {executor.submit(process_station, station, start_date_12_months_ago, endDate): station for station in no_data_df['Stations']}
        for future in as_completed(futures):
            station, lb_stats = future.result()
            if lb_stats is not None:
                longterm_results.append({
                    'Station': station,
                    'Logger Battery Mean': lb_stats['mean'],
                    'Logger Battery Mode': lb_stats['mode'],
                    'Logger Battery Mode-Mean Difference (MMD)': lb_stats['mmd'],
                    'Logger Battery Standard Deviation (SD)': lb_stats['std'],
                    'Logger Battery Mean Absolute Deviation (MAD)': lb_stats['mad'],
                    'Logger Battery Coefficient of Variation (CV)': lb_stats['cv'],
                    'Logger Battery Kurtosis': lb_stats['kurtosis'],
                    'Logger Battery Voltage Range (VR)': lb_stats['range'],
                    'Logger Battery Interquartile Range (IQR)': lb_stats['iqr'],
                    'Logger Battery Skewness': lb_stats['skewness'],
                    'Battery Performance Index (%)': lb_stats['battery_performance_index'],
                    'LB-Temperature Correlation': lb_stats['correlation_with_te'],
                    'LB-Solar Radiation Correlation': lb_stats['correlation_with_ra'],
                    'LB-Logger Temperature Correlation': lb_stats['correlation_with_lt'],
                    'LB-Precipitation Correlation': lb_stats['correlation_with_pr'],
                    'LB-Lightning Distance Correlation': lb_stats['correlation_with_ld'],
                    'Environment Effect on Battery (%)': lb_stats['environment_effect_on_battery'],
                    'Detected Issue': lb_stats['detected_issue']
                })
    
    # Save long-term results
    longterm_results_df = pd.DataFrame(longterm_results).sort_values(by='Battery Performance Index (%)', ascending=False)
    longterm_summary_filename = 'longtermDetections.csv'
    longterm_results_df.to_csv(longterm_summary_filename, index=False)
    print(f'Long-term summary saved to file "{longterm_summary_filename}"')
