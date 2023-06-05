import pandas as pd

def fill_missing_values(data):
    data['flbmk'].fillna('N', inplace=True)
    data['flg_3dsmk'].fillna('N', inplace=True)
    return data

def create_cross_features(data):
    data['ecfg_stocn'] = data['ecfg'] + data['stocn'].astype('str')
    data['ecfg_scity'] = data['ecfg'] + data['scity'].astype('str')
    data['ovrlt_stocn'] = data['ovrlt'] + data['stocn'].astype('str')
    data['ovrlt_scity'] = data['ovrlt'] + data['stocn'].astype('str')
    return data

def convert_categorical_to_numerical(data, columns):
    for col in columns:
        data[col] = data[col].astype('category').cat.codes
    return data

def process_data(data):
    data = fill_missing_values(data)
    data = create_cross_features(data)
    
    categorical_cols = ['ecfg', 'flbmk', 'flg_3dsmk', 'insfg', 'ovrlt', 'acqic', 'bacno', 'cano', 'csmcu', 'hcefg', 'mcc', 'mchno', 'scity', 'stocn']
    data = convert_categorical_to_numerical(data, categorical_cols)
    
    # loctm data
    data['loctm_int'] = data['loctm'].astype(int)
    data['loctm_str'] = data['loctm_int'].astype(str)
    data['loctm_str'] = data['loctm_str'].apply(lambda x: '0' + x if (len(x) == 5) else ('00' + x if (len(x) == 4) else ('000' + x if (len(x) == 3) else ('0000' + x if (len(x) == 2) else ('00000' + x if (len(x) == 1) else x)))))
    data['hours'] = data['loctm_str'].str[0:2].astype(int)
    data['minutes'] = data['loctm_str'].str[2:4].astype(int)
    data['seconds'] = data['loctm_str'].str[4:6].astype(int)
    data['total_seconds'] = data['locdt'] * 86400 + data['hours'] * 3600 + data['minutes'] * 60 + data['seconds']
    data['time'] = (data['hours'] * 3600 + data['minutes'] * 60 + data['seconds'])  # / 86400

    data['trad_hour'] = data['loctm'] // 10000

    data['morning'] = ((data['trad_hour'] < 12) & (data['trad_hour'] >= 6)).replace([True, False], [1, 0])
    data['afternoon'] = ((data['trad_hour'] < 18) & (data['trad_hour'] >= 12)).replace([True, False], [1, 0])
    data['night'] = ((data['trad_hour'] < 24) & (data['trad_hour'] >= 18)).replace([True, False], [1, 0])
    data['midnight'] = ((data['trad_hour'] < 6) & (data['trad_hour'] >= 0)).replace([True, False], [1, 0])

    # account data
    transaction_count = data.groupby(['bacno'])['cano'].count().rename("transaction_count").reset_index()
    data = data.merge(transaction_count, how='left')

    transaction_count_cano = data.groupby(['bacno', 'cano'])['cano'].count().rename("transaction_count_cano").reset_index()
    data = data.merge(transaction_count_cano, how='left')

    acqic_duplicated_count = data.groupby(['bacno', 'acqic'])['acqic'].count().rename("acqic_duplicated_count").reset_index()
    data = data.merge(acqic_duplicated_count, how='left')

    conam_duplicated_count = data.groupby(['bacno', 'conam'])['conam'].count().rename("conam_duplicated_count").reset_index()
    data = data.merge(conam_duplicated_count, how='left')

    conam_stocn = data.groupby(['csmcu', 'stocn'])['stocn'].count().rename("conam_stocn").reset_index()
    data = data.merge(conam_stocn, on=['csmcu', 'stocn'])

    bacno_mchno = data.groupby(['bacno', 'mchno'])['mchno'].count().rename("bacno_mchno").reset_index()
    data = data.merge(bacno_mchno, on=['bacno', 'mchno'])

    cano_mchno = data.groupby(['cano', 'mchno'])['mchno'].count().rename("cano_mchno").reset_index()
    data = data.merge(cano_mchno, on=['cano', 'mchno'])

    bacno_stocn = data.groupby(['bacno', 'stocn'])['stocn'].count().rename("bacno_stocn").reset_index()
    data = data.merge(bacno_stocn, on=['bacno', 'stocn'])

    cano_stocn = data.groupby(['cano', 'stocn'])['stocn'].count().rename("cano_stocn").reset_index()
    data = data.merge(cano_stocn, on=['cano', 'stocn'])

    bacno_scity = data.groupby(['bacno', 'scity'])['scity'].count().rename("bacno_scity").reset_index()
    data = data.merge(bacno_scity, on=['bacno', 'scity'])

    cano_scity = data.groupby(['cano', 'scity'])['scity'].count().rename("cano_scity").reset_index()
    data = data.merge(cano_scity, on=['cano', 'scity'])

    bacno_flg_3dsmk = data.groupby(['bacno', 'flg_3dsmk'])['flg_3dsmk'].count().rename("bacno_flg_3dsmk").reset_index()
    data = data.merge(bacno_flg_3dsmk, on=['bacno', 'flg_3dsmk'])

    cano_flg_3dsmk = data.groupby(['cano', 'flg_3dsmk'])['flg_3dsmk'].count().rename("cano_flg_3dsmk").reset_index()
    data = data.merge(cano_flg_3dsmk, on=['cano', 'flg_3dsmk'])

    bacno_ecfg_mean = data.groupby(['bacno'])['ecfg'].mean().rename("bacno_ecfg_mean").reset_index()
    data = data.merge(bacno_ecfg_mean, on=['bacno'])

    cano_ecfg_mean = data.groupby(['cano'])['ecfg'].mean().rename("cano_ecfg_mean").reset_index()
    data = data.merge(cano_ecfg_mean, on=['cano'])

    grp = data.groupby(['bacno'])['conam'].min().rename("comsum_min").reset_index()
    data = data.merge(grp, how='left')

    grp = data.groupby(['bacno'])['conam'].max().rename('comsum_max').reset_index()
    data = data.merge(grp, how='left')

    grp = (data.groupby(['bacno'])['txkey'].count() / data['locdt'].max()).reset_index().rename(columns={'txkey': 'acc_trad_ave'})
    data = data.merge(grp, how='left')

    grp = data.groupby(['bacno'])['txkey'].count().reset_index().rename(columns={'txkey': 'acc_trad_total'})
    data = data.merge(grp, how='left')

    grp = data.groupby(['bacno'])['conam'].sum().reset_index().rename(columns={'conam': 'comsum_total'})
    data = data.merge(grp, how='left')

    grp = data.groupby(['bacno'])['conam'].mean().reset_index().rename(columns={'conam': 'comsum_ave'})
    data = data.merge(grp, how='left')

    mean_amount = data.groupby(['bacno'])['conam'].mean().rename("mean_amount").reset_index()
    data = data.merge(mean_amount, how='left')

    data['amtby_mean_amount'] = data['conam'] / data['mean_amount']
    data['amtby_mean_amount'] = data['amtby_mean_amount'].fillna(0)

    mean_amount_cano = data.groupby(['bacno', 'cano'])['conam'].mean().rename("mean_amount_cano").reset_index()
    data = data.merge(mean_amount_cano, how='left')

    data['amtby_mean_amount_cano'] = data['conam'] / data['mean_amount_cano']
    data['amtby_mean_amount_cano'] = data['amtby_mean_amount_cano'].fillna(0)

    median_amount = data.groupby(['bacno'])['conam'].median().rename("median_amount").reset_index()
    data = data.merge(median_amount, how='left')

    data['amtby_median_amount'] = data['conam'] / data['median_amount']
    data['amtby_median_amount'] = data['amtby_median_amount'].fillna(0)

    median_amount_cano = data.groupby(['bacno', 'cano'])['conam'].median().rename("median_amount_cano").reset_index()
    data = data.merge(median_amount_cano, how='left')

    data['amtby_median_amount_cano'] = data['conam'] / data['median_amount_cano']
    data['amtby_median_amount_cano'] = data['amtby_median_amount_cano'].fillna(0)

    std_amount = data.groupby(['bacno'])['conam'].std().rename("std_amount").reset_index()
    data = data.merge(std_amount, how='left')
    
    #time series
    ##30天期以內交易
    day30 = data[data['locdt'] <= 30]
    grp = (day30.groupby(['bacno'])['txkey'].count() / 30).reset_index().rename(columns={'txkey':'comsum_feq30'})
    data = data.merge(grp,how='left')
    data['comsum_feq30'].fillna(0,inplace=True)

    ##30-60天期交易
    day30 = data[(data['locdt'] <= 60) & (data['locdt'] >30 ) ]
    grp = (day30.groupby(['bacno'])['txkey'].count() / 30).reset_index().rename(columns={'txkey':'comsum_feq3060'})
    data = data.merge(grp,how='left')
    data['comsum_feq3060'].fillna(0,inplace=True)

    ##60-90天期交易資訊
    day30 = data[(data['locdt'] <= 90) & (data['locdt'] >60 ) ]
    grp = (day30.groupby(['bacno'])['txkey'].count() / 30).reset_index().rename(columns={'txkey':'comsum_feq6090'})
    data = data.merge(grp,how='left')
    data['comsum_feq6090'].fillna(0,inplace=True)

    ##90-120天期交易資訊
    day30 = data[(data['locdt'] <= 120) & (data['locdt'] >90 ) ]
    grp = (day30.groupby(['bacno'])['txkey'].count() / 30).reset_index().rename(columns={'txkey':'comsum_feq90120'})
    data = data.merge(grp,how='left')
    data['comsum_feq90120'].fillna(0,inplace=True)

    ##60天以內的交易
    day60 = data[data['locdt'] <= 60]
    grp = (day60.groupby(['bacno'])['txkey'].count() / 60).reset_index().rename(columns={'txkey':'comsum_feq60'})
    data = data.merge(grp,how='left')
    data['comsum_feq60'].fillna(0,inplace=True)

    ##30-90天的交易資訊
    day60 = data[(data['locdt'] <= 90) & (data['locdt'] >30 ) ]
    grp = (day60.groupby(['bacno'])['txkey'].count() / 60).reset_index().rename(columns={'txkey':'comsum_feq3090'})
    data = data.merge(grp,how='left')
    data['comsum_feq3090'].fillna(0,inplace=True)

    ##60-120天的交易資訊
    day60 = data[(data['locdt'] <= 120) & (data['locdt'] >60 ) ]
    grp = (day60.groupby(['bacno'])['txkey'].count() / 60).reset_index().rename(columns={'txkey':'comsum_feq60120'})
    data = data.merge(grp,how='left')
    data['comsum_feq60120'].fillna(0,inplace=True)


    grp = data.groupby(['locdt'])['bacno'].count().reset_index().rename(columns={'bacno':'day_trad_num'})
    data = data.merge(grp,how='left')

    grp = data.groupby(['locdt'])['conam'].sum().reset_index().rename(columns={'conam':'day_comsum_total'})
    data = data.merge(grp,how='left')


    frequency = ((data.groupby(['bacno'])['locdt'].max()-data.groupby(['bacno'])['locdt'].min())/data.groupby(['bacno'])['locdt'].count()).rename("frequency").reset_index()
    data = data.merge(frequency,how='left')

    frequency_cano = ((data.groupby(['bacno','cano'])['locdt'].max()-data.groupby(['bacno','cano'])['locdt'].min())/data.groupby(['bacno','cano'])['locdt'].count()).rename("frequency_cano").reset_index()
    data = data.merge(frequency_cano,how='left')

    mean_time = data.groupby(['bacno'])['time'].mean().rename("mean_time").reset_index()
    data = data.merge(mean_time,how='left')

    median_time = data.groupby(['bacno'])['time'].median().rename("median_time").reset_index()
    data = data.merge(median_time,how='left')


    mean_time_cano = data.groupby(['bacno','cano'])['time'].mean().rename("mean_time_cano").reset_index()
    data = data.merge(mean_time_cano,how='left')

    median_time_cano = data.groupby(['bacno','cano'])['time'].median().rename("median_time_cano").reset_index()
    data = data.merge(median_time_cano,how='left')

    data = data.sort_values(by = ['total_seconds'])
    data['total_seconds_diff'] = data.groupby(['bacno'])['total_seconds'].diff()
    data['total_seconds_diff'] = data['total_seconds_diff'].fillna(0)

    data = data.sort_values(by = ['total_seconds'])
    data['total_seconds_diff_cano'] = data.groupby(['bacno','cano'])['total_seconds'].diff()
    data['total_seconds_diff_cano'] = data['total_seconds_diff_cano'].fillna(0)


    data['date_day'] = data['locdt'] % 30

    data['week'] = data['locdt'] % 7

    data['2_week'] = data['locdt'] % 14

    data['month'] = data['locdt'] % 30
    
    ##country data
    data['is_taiwan'] = (data['stocn'] == 102).replace([True,False],[1,0])

    grp = data.groupby(['stocn'])['txkey'].count().reset_index().rename(columns={'txkey':'country_com_num'})
    data = data.merge(grp,how='left')

    grp = data.groupby(['scity'])['txkey'].count().reset_index().rename(columns={'txkey':'city_com_num'})
    data = data.merge(grp,how='left')
        
    return data