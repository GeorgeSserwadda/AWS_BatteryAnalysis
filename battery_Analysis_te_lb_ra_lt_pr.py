import TAHMO
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize the API client
api = TAHMO.apiWrapper()
api.setCredentials('user', 'password')

stations = [
    'TA00001', 'TA00002', 'TA00003', 'TA00004', 'TA00005', 'TA00006', 'TA00007',
    'TA00008', 'TA00009', 'TA00010', 'TA00011', 'TA00012', 'TA00013', 'TA00014',
    'TA00015', 'TA00016', 'TA00018', 'TA00019', 'TA00020', 'TA00021', 'TA00023',
    'TA00024', 'TA00025', 'TA00026', 'TA00027', 'TA00028', 'TA00029', 'TA00030',
    'TA00031', 'TA00032', 'TA00033', 'TA00034', 'TA00035', 'TA00036', 'TA00037',
    'TA00039', 'TA00041', 'TA00042', 'TA00043', 'TA00044', 'TA00045', 'TA00046',
    'TA00047', 'TA00049', 'TA00050', 'TA00051', 'TA00052', 'TA00054', 'TA00055',
    'TA00056', 'TA00057', 'TA00061', 'TA00062', 'TA00063', 'TA00064', 'TA00065',
    'TA00066', 'TA00067', 'TA00068', 'TA00069', 'TA00070', 'TA00071', 'TA00072',
    'TA00073', 'TA00074', 'TA00075', 'TA00076', 'TA00077', 'TA00078', 'TA00079',
    'TA00080', 'TA00083', 'TA00084', 'TA00085', 'TA00088', 'TA00089', 'TA00090',
    'TA00092', 'TA00093', 'TA00094', 'TA00096', 'TA00101', 'TA00102', 'TA00103',
    'TA00107', 'TA00108', 'TA00109', 'TA00110', 'TA00111', 'TA00112', 'TA00113',
    'TA00114', 'TA00115', 'TA00116', 'TA00117', 'TA00118', 'TA00119', 'TA00120',
    'TA00121', 'TA00122', 'TA00123', 'TA00124', 'TA00125', 'TA00126', 'TA00127',
    'TA00129', 'TA00130', 'TA00131', 'TA00132', 'TA00133', 'TA00135', 'TA00136',
    'TA00137', 'TA00138', 'TA00140', 'TA00141', 'TA00144', 'TA00147', 'TA00148',
    'TA00149', 'TA00150', 'TA00151', 'TA00153', 'TA00154', 'TA00155', 'TA00156',
    'TA00157', 'TA00158', 'TA00159', 'TA00160', 'TA00161', 'TA00162', 'TA00163',
    'TA00164', 'TA00166', 'TA00168', 'TA00169', 'TA00170', 'TA00171', 'TA00172',
    'TA00173', 'TA00174', 'TA00176', 'TA00178', 'TA00179', 'TA00180', 'TA00181',
    'TA00182', 'TA00183', 'TA00184', 'TA00185', 'TA00186', 'TA00187', 'TA00189',
    'TA00190', 'TA00192', 'TA00194', 'TA00195', 'TA00196', 'TA00197', 'TA00198',
    'TA00199', 'TA00201', 'TA00203', 'TA00205', 'TA00206', 'TA00207', 'TA00208',
    'TA00209', 'TA00210', 'TA00211', 'TA00212', 'TA00213', 'TA00214', 'TA00215',
    'TA00216', 'TA00217', 'TA00218', 'TA00219', 'TA00220', 'TA00221', 'TA00222',
    'TA00223', 'TA00224', 'TA00225', 'TA00226', 'TA00227', 'TA00228', 'TA00229',
    'TA00230', 'TA00231', 'TA00232', 'TA00233', 'TA00234', 'TA00236', 'TA00237',
    'TA00239', 'TA00240', 'TA00241', 'TA00242', 'TA00243', 'TA00244', 'TA00245',
    'TA00246', 'TA00247', 'TA00248', 'TA00249', 'TA00250', 'TA00251', 'TA00252',
    'TA00253', 'TA00254', 'TA00255', 'TA00256', 'TA00257', 'TA00259', 'TA00260',
    'TA00261', 'TA00262', 'TA00264', 'TA00265', 'TA00266', 'TA00267', 'TA00268',
    'TA00269', 'TA00270', 'TA00271', 'TA00272', 'TA00274', 'TA00275', 'TA00276',
    'TA00277', 'TA00278', 'TA00279', 'TA00280', 'TA00281', 'TA00282', 'TA00283',
    'TA00284', 'TA00285', 'TA00286', 'TA00287', 'TA00288', 'TA00292', 'TA00293',
    'TA00294', 'TA00300', 'TA00301', 'TA00302', 'TA00303', 'TA00304', 'TA00305',
    'TA00308', 'TA00309', 'TA00310', 'TA00312', 'TA00313', 'TA00314', 'TA00315',
    'TA00316', 'TA00317', 'TA00319', 'TA00321', 'TA00322', 'TA00323', 'TA00325',
    'TA00326', 'TA00327', 'TA00328', 'TA00329', 'TA00331', 'TA00332', 'TA00333',
    'TA00334', 'TA00335', 'TA00336', 'TA00337', 'TA00338', 'TA00339', 'TA00341',
    'TA00342', 'TA00343', 'TA00344', 'TA00345', 'TA00346', 'TA00347', 'TA00348',
    'TA00349', 'TA00350', 'TA00351', 'TA00353', 'TA00354', 'TA00355', 'TA00356',
    'TA00358', 'TA00359', 'TA00360', 'TA00361', 'TA00362', 'TA00363', 'TA00364',
    'TA00365', 'TA00366', 'TA00367', 'TA00368', 'TA00369', 'TA00370', 'TA00372',
    'TA00374', 'TA00375', 'TA00377', 'TA00379', 'TA00380', 'TA00382', 'TA00383',
    'TA00384', 'TA00385', 'TA00386', 'TA00387', 'TA00388', 'TA00389', 'TA00390',
    'TA00391', 'TA00392', 'TA00393', 'TA00395', 'TA00396', 'TA00397', 'TA00398',
    'TA00399', 'TA00404', 'TA00405', 'TA00406', 'TA00407', 'TA00408', 'TA00409',
    'TA00410', 'TA00413', 'TA00414', 'TA00416', 'TA00429', 'TA00430', 'TA00431',
    'TA00432', 'TA00433', 'TA00435', 'TA00436', 'TA00437', 'TA00438', 'TA00439',
    'TA00440', 'TA00441', 'TA00442', 'TA00444', 'TA00445', 'TA00446', 'TA00447',
    'TA00448', 'TA00451', 'TA00453', 'TA00454', 'TA00457', 'TA00458', 'TA00459',
    'TA00460', 'TA00461', 'TA00462', 'TA00464', 'TA00466', 'TA00467', 'TA00468',
    'TA00470', 'TA00471', 'TA00473', 'TA00480', 'TA00482', 'TA00483', 'TA00484',
    'TA00485', 'TA00486', 'TA00493', 'TA00494', 'TA00495', 'TA00508', 'TA00509',
    'TA00510', 'TA00511', 'TA00512', 'TA00514', 'TA00518', 'TA00519', 'TA00520',
    'TA00522', 'TA00523', 'TA00524', 'TA00525', 'TA00526', 'TA00527', 'TA00528',
    'TA00529', 'TA00531', 'TA00532', 'TA00533', 'TA00534', 'TA00535', 'TA00536',
    'TA00537', 'TA00538', 'TA00539', 'TA00540', 'TA00541', 'TA00542', 'TA00543',
    'TA00544', 'TA00546', 'TA00547', 'TA00549', 'TA00550', 'TA00552', 'TA00554',
    'TA00555', 'TA00556', 'TA00557', 'TA00558', 'TA00559', 'TA00561', 'TA00562',
    'TA00563', 'TA00564', 'TA00565', 'TA00566', 'TA00567', 'TA00568', 'TA00569',
    'TA00570', 'TA00571', 'TA00572', 'TA00574', 'TA00575', 'TA00577', 'TA00578',
    'TA00579', 'TA00580', 'TA00581', 'TA00582', 'TA00583', 'TA00584', 'TA00585',
    'TA00586', 'TA00587', 'TA00588', 'TA00589', 'TA00590', 'TA00591', 'TA00592',
    'TA00593', 'TA00594', 'TA00595', 'TA00596', 'TA00597', 'TA00598', 'TA00599',
    'TA00601', 'TA00602', 'TA00603', 'TA00605', 'TA00606', 'TA00607', 'TA00608',
    'TA00609', 'TA00610', 'TA00611', 'TA00612', 'TA00614', 'TA00615', 'TA00617',
    'TA00618', 'TA00619', 'TA00620', 'TA00621', 'TA00623', 'TA00624', 'TA00625',
    'TA00627', 'TA00628', 'TA00629', 'TA00631', 'TA00632', 'TA00633', 'TA00634',
    'TA00635', 'TA00636', 'TA00637', 'TA00638', 'TA00639', 'TA00640', 'TA00642',
    'TA00643', 'TA00644', 'TA00645', 'TA00646', 'TA00647', 'TA00649', 'TA00650',
    'TA00652', 'TA00654', 'TA00655', 'TA00656', 'TA00657', 'TA00659', 'TA00661',
    'TA00662', 'TA00663', 'TA00664', 'TA00665', 'TA00666', 'TA00668', 'TA00669',
    'TA00670', 'TA00671', 'TA00672', 'TA00673', 'TA00674', 'TA00676', 'TA00677',
    'TA00678', 'TA00679', 'TA00680', 'TA00681', 'TA00682', 'TA00683', 'TA00684',
    'TA00685', 'TA00686', 'TA00687', 'TA00688', 'TA00690', 'TA00691', 'TA00692',
    'TA00693', 'TA00694', 'TA00695', 'TA00697', 'TA00698', 'TA00699', 'TA00700',
    'TA00702', 'TA00703', 'TA00704', 'TA00706', 'TA00707', 'TA00709', 'TA00710',
    'TA00711', 'TA00712', 'TA00713', 'TA00714', 'TA00715', 'TA00716', 'TA00717',
    'TA00718', 'TA00719', 'TA00720', 'TA00721', 'TA00722', 'TA00723', 'TA00724',
    'TA00725', 'TA00726', 'TA00727', 'TA00728', 'TA00729', 'TA00730', 'TA00731',
    'TA00732', 'TA00733', 'TA00734', 'TA00735', 'TA00736', 'TA00737', 'TA00738',
    'TA00739', 'TA00740', 'TA00741', 'TA00742', 'TA00743', 'TA00744', 'TA00745',
    'TA00746', 'TA00747', 'TA00748', 'TA00749', 'TA00750', 'TA00751', 'TA00752',
    'TA00753', 'TA00754', 'TA00755', 'TA00756', 'TA00757', 'TA00758', 'TA00759',
    'TA00760', 'TA00761', 'TA00762', 'TA00763', 'TA00764', 'TA00765', 'TA00766',
    'TA00767', 'TA00768', 'TA00769', 'TA00770', 'TA00771', 'TA00772', 'TA00773',
    'TA00774', 'TA00775', 'TA00776', 'TA00777', 'TA00778', 'TA00779', 'TA00780',
    'TA00781', 'TA00782', 'TA00783', 'TA00784', 'TA00785', 'TA00786', 'TA00787',
    'TA00788', 'TA00789', 'TA00790', 'TA00791', 'TA00792', 'TA00793', 'TA00794',
    'TA00795', 'TA00796', 'TA00797', 'TA00798', 'TA00799', 'TA00800', 'TA00801',
    'TA00802', 'TA00803', 'TA00804', 'TA00805', 'TA00806', 'TA00807', 'TA00808',
    'TA00809', 'TA00810', 'TA00811', 'TA00812', 'TA00813', 'TA00814', 'TA00815',
    'TA00816', 'TA00817', 'TA00818', 'TA00819', 'TA00820', 'TA00821', 'TA00822',
    'TA00823', 'TA00824', 'TA00825', 'TA00826', 'TA00827', 'TA00828', 'TA00829',
    'TA00830', 'TA00831', 'TA00832', 'TA00833', 'TA00834', 'TA00835', 'TA00836',
    'TA00837', 'TA00838', 'TA00839', 'TA00840', 'TA00841', 'TA00842', 'TA00843',
    'TA00844', 'TA00845', 'TA00846', 'TA00847', 'TA00848', 'TA00849', 'TA00850',
    'TA00851', 'TA00852', 'TA00853', 'TA00854', 'TA00855', 'TA00856', 'TA00857',
    'TA00858', 'TA00859', 'TA00860', 'TA00861', 'TA00862', 'TA00863', 'TA00864',
    'TA00865', 'TA00866', 'TA00867', 'TA00868', 'TA00869', 'TA00870', 'TA00871',
    'TA00872', 'TA00873', 'TA00874', 'TA00875', 'TA00876', 'TA00877', 'TA00878',
    'TA00879', 'TA00880', 'TA00881', 'TA00882', 'TA00883', 'TA00884', 'TA00885',
    'TA00886', 'TA00887', 'TA00888', 'TA00889', 'TA00890', 'TA00891', 'TA00892',
    'TA00893', 'TA00894', 'TA00895', 'TA00896', 'TA00897', 'TA00898', 'TA00899',
    'TA00900', 'TA00901', 'TA00902', 'TA00903', 'TA00904', 'TA00905', 'TA00906',
    'TA00907', 'TA00908', 'TA00909', 'TA00910', 'TA00911', 'TA00912', 'TA00913',
    'TA00914', 'TA00915', 'TA00916', 'TA00917', 'TA00918', 'TA00919', 'TA00920',
    'TA00921', 'TA00922', 'TA00923', 'TA00924', 'TA00925', 'TA00926', 'TA00927',
    'TA00928', 'TA00929', 'TA00930', 'TA00931', 'TA00932', 'TA00933', 'TA00934',
    'TA00935', 'TA00936', 'TA00937', 'TA00938', 'TA00939', 'TA00940', 'TA00941',
    'TA00942', 'TA00943', 'TA00944', 'TA00945', 'TA00946', 'TA00947', 'TA00948',
    'TA00949', 'TA00950', 'TA00951', 'TA00952', 'TA00953', 'TA00954', 'TA00955',
    'TA00956', 'TA00957', 'TA00958', 'TA00959', 'TA00960', 'TA00961', 'TA00962',
    'TA00963', 'TA00964', 'TA00965', 'TA00966', 'TA00967', 'TA00968', 'TA00969',
    'TA00970', 'TA00971', 'TA00972', 'TA00973', 'TA00974', 'TA00975', 'TA00976',
    'TA00977', 'TA00978', 'TA00979', 'TA00980', 'TA00981', 'TA00982', 'TA00983',
    'TA00984', 'TA00985', 'TA00986', 'TA00987', 'TA00988', 'TA00989', 'TA00990',
    'TA00991', 'TA00992', 'TA00993', 'TA00994', 'TA00995', 'TA00996', 'TA00997',
    'TA00998', 'TA00999', 'TA01000'
]

variables = ['lb', 'te', 'lt', 'ra', 'pr', 'ld']
startDate = '2024-10-31'
endDate = '2024-11-13'

results = []
no_data_stations = []

# Define function to process each station
def process_station(station):
    try:
        df = api.getMeasurements(station, startDate=startDate, endDate=endDate, variables=variables)
        
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
            if lb_stats['mode'] is not None and lb_stats['mode'] > 50:
                lb_stats['detected_issue'] = "No Issue"
            elif lb_stats['mode'] is not None and lb_stats['mode'] <= 50:
                lb_stats['detected_issue'] = "Hardware Issue (faulty or needs cleaning)"
        elif lb_stats['battery_performance_index'] is not None and 0 < lb_stats['battery_performance_index'] < 10:
            if lb_stats['environment_effect_on_battery'] is not None and lb_stats['environment_effect_on_battery'] >= 10:
                lb_stats['detected_issue'] = "Unstable old Batteries (may work but all need replacing)"
        else:
            lb_stats['detected_issue'] = None

        # Save timeseries data to CSV
        filename = f'{station}_timeseries_{startDate}_to_{endDate}.csv'
        df.index.name = 'Timestamp'
        df.to_csv(filename, na_rep='', date_format='%Y-%m-%d %H:%M')
        print(f'Timeseries saved to file "{filename}"')

        return station, lb_stats

    except Exception as e:
        print(f"Error processing station {station}: {e}")
        return station, None

# Run API requests in parallel
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = {executor.submit(process_station, station): station for station in stations}
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
print(f'Summary of statistics saved to "{summary_filename}"')

# Save list of stations without data
no_data_df = pd.DataFrame(no_data_stations, columns=['Station'])
no_data_filename = f'stations_without_data_{startDate}_to_{endDate}.csv'
no_data_df.to_csv(no_data_filename, index=False)
print(f'List of stations without data saved to "{no_data_filename}"')
