import numpy as np
import pandas as pd
from pymsis import msis
import time

def save_bothmsis_sat_df(df, year, sat_choose):

    ''' This function takes a dataframe that contains the satellite ephemeris
    and data and adds the MSIS data directly to it for each satellite data point.'''


    if sat_choose == 'champ':
        den_string = 'D400'
        alt_norm = 400
    elif sat_choose == 'grace':
        den_string = 'D500'
        alt_norm = 500

    
    index_year = df['Date'].dt.year==year
    msis00 = np.empty((np.size(df[index_year]),24))
    msis2 = np.empty((np.size(df[index_year]),24))
    date = []

    start = time.time()
    count = 0
    ii = 0
    for i,val in df[index_year].iterrows():
        lon   = df.Lon[i]
        lat   = df.Lat[i]
        alt   = df.Height[i]
        f107  = df.f107d_dayvals[i]
        f107a = df.f107a_dayvals[i]
        ap    = df.Ap_dayvals[i]
        dates =  df.Date[i]  
        ndates = np.size(dates)

        # (F107, F107a, ap) all need to be specified at the same length as dates
        f107s = [f107]
        f107as = [f107a]
        aps = [[ap]*7]

        output00 = msis.run(dates, lon, lat, alt_norm, f107, f107a, aps, version = 0)
        output2 = msis.run(dates, lon, lat, alt_norm, f107, f107a, aps, version = 2)

        msis00[ii][0] = output00[0,0,0,0][0]
        msis00[ii][1] = output00[0,0,0,0][1]
        msis00[ii][2] = output00[0,0,0,0][2]
        msis00[ii][3] = output00[0,0,0,0][3]
        msis00[ii][4] = output00[0,0,0,0][4]
        msis00[ii][5] = output00[0,0,0,0][5]
        msis00[ii][6] = output00[0,0,0,0][6]
        msis00[ii][7] = output00[0,0,0,0][7]
        msis00[ii][8] = output00[0,0,0,0][8]
        msis00[ii][9] = output00[0,0,0,0][9]
        msis00[ii][10] = output00[0,0,0,0][10]

        msis2[ii][0] = output2[0,0,0,0][0]
        msis2[ii][1] = output2[0,0,0,0][1]
        msis2[ii][2] = output2[0,0,0,0][2]
        msis2[ii][3] = output2[0,0,0,0][3]
        msis2[ii][4] = output2[0,0,0,0][4]
        msis2[ii][5] = output2[0,0,0,0][5]
        msis2[ii][6] = output2[0,0,0,0][6]
        msis2[ii][7] = output2[0,0,0,0][7]
        msis2[ii][8] = output2[0,0,0,0][8]
        msis2[ii][9] = output2[0,0,0,0][9]
        msis2[ii][10] = output2[0,0,0,0][10]

        msis2[ii][11] =    lon   
        msis2[ii][12] =    lat   
        msis2[ii][13] =    alt   
        msis2[ii][14] =    f107  
        msis2[ii][15] =    f107a 
        msis2[ii][16] =    ap    
        msis2[ii][17] =    df.Doy[i] 
        msis2[ii][18] =    df.Hours[i] 
        msis2[ii][19] =    df.LatBin[i] 
        msis2[ii][20] =    df.LocTim[i] 
        msis2[ii][21] =    df.Density[i] 
        msis2[ii][22] =    df[den_string][i] 
        msis2[ii][23] =    df.Year[i]

        msis00[ii][11] =    lon   
        msis00[ii][12] =    lat   
        msis00[ii][13] =    alt   
        msis00[ii][14] =    f107  
        msis00[ii][15] =    f107a 
        msis00[ii][16] =    ap    
        msis00[ii][17] =    df.Doy[i] 
        msis00[ii][18] =    df.Hours[i] 
        msis00[ii][19] =    df.LatBin[i] 
        msis00[ii][20] =    df.LocTim[i] 
        msis00[ii][21] =    df.Density[i] 
        msis00[ii][22] =    df[den_string][i] 
        msis00[ii][23] =    df.Year[i]
        
        date.append(pd.to_datetime(dates))
        ii +=1


        if count == 500:
            end = time.time()
            elapsed = end - start
            print("Total Time:",elapsed,'---', df['Year'][i], df['Doy'][i])
            count = 0
        else:
            count += 1

    variables = ['Rho', 'N2', 'O2', 'O', 'He',
                 'H', 'Ar', 'N', 'AnomalousO', 'NO', 'Temperature',
                'Lon', 'Lat', 'Height', 'f107', 'f107a', 'ap',
                'Doy', 'Hours', 'LatBin', 'LocTim', 'Density', den_string, 'Year'
                ]

    print(df['Year'][i],' ', 'Done with loops')

    df_msis2_make  = pd.DataFrame(msis2  ,columns = variables)
    df_msis00_make = pd.DataFrame(msis00 ,columns = variables)
    df_msis2_make['Date'] =  pd.Series(date)
    df_msis00_make['Date'] =  pd.Series(date)
    print(df['Year'][i],' ', 'Done with making msis dataframes')

    df_msis2_make = df_msis2_make.loc[(df_msis2_make!=0).all(axis=1)]
    df_msis00_make = df_msis00_make.loc[(df_msis00_make!=0).all(axis=1)]
    print(df['Year'][i],' ', 'Removed extra zeros from dataframe')

    df_msis2_make.to_pickle('parallelize/' + str(year) + 'msis00make' + sat_choose  )  
    df_msis00_make.to_pickle('parallelize/' + str(year) + 'msis2make' + sat_choose  )  
    print(df['Year'][i],' ', 'Saved constructed dfs as pickles')

    return


# def sample_msis00_msis2_sat(df, year):

#     ''' This function takes a dataframe that contains the satellite ephemeris
#     and data and adds the MSIS data directly to it for each satellite data point.'''

#     del df['Mlat']
#     del df['Mlon']
#     del df['Mlt']
#     del df['U_rho']
#     del df['Num']
#     del df['NumInterp']
#     del df['D410']


#     df2_champ = df[df['Date'].dt.year==year] # create an identical DF for storing MSIS2.0 data separately
#     df = df[df['Date'].dt.year==year]

#     variables = ['Rho', 'N2', 'O2', 'O', 'He',
#                  'H', 'Ar', 'N', 'AnomalousO', 'NO', 'Temperature']    

#     for i in variables:
#         df[i]=np.nan
#         df2_champ[i]=np.nan

#     start = time.time()
#     count = 0
#     for i,val in df[df['Date'].dt.year==year].iterrows():

#         lon   = df.Lon[i]
#         lat   = df.Lat[i]
#         alt   = df.Height[i]
#         f107  = df.f107d_dayvals[i]
#         f107a = df.f107a_dayvals[i]
#         ap    = df.Ap_dayvals[i]
#         dates =  df.Date[i]  
#         ndates = np.size(dates)

#         # (F107, F107a, ap) all need to be specified at the same length as dates
#         f107s = [f107]
#         f107as = [f107a]
#         aps = [[ap]*7]

#         output00 = msis.run(dates, lon, lat, alt, f107, f107a, aps)
#         output2 = msis.run(dates, lon, lat, alt, f107, f107a, aps, version = 2)

#         df.loc[i , ['Rho']] = output00[0,0,0,0][0]
#         df2_champ.loc[i, ['Rho']] = output2[0,0,0,0][0]

#         df.loc[i , ['N2']] = output00[0,0,0,0][1]
#         df2_champ.loc[i, ['N2']] = output2[0,0,0,0][1]

#         df.loc[i , ['O2']] = output00[0,0,0,0][2]
#         df2_champ.loc[i, ['O2']] = output2[0,0,0,0][2]

#         df.loc[i , ['O']] = output00[0,0,0,0][3]
#         df2_champ.loc[i, ['O']] = output2[0,0,0,0][3]

#         df.loc[i , ['He']] = output00[0,0,0,0][4]
#         df2_champ.loc[i, ['He']] = output2[0,0,0,0][4]

#         df.loc[i , ['H']] = output00[0,0,0,0][5]
#         df2_champ.loc[i, ['H']] = output2[0,0,0,0][5]

#         df.loc[i , ['Ar']] = output00[0,0,0,0][6]
#         df2_champ.loc[i, ['Ar']] = output2[0,0,0,0][6]

#         df.loc[i , ['N']] = output00[0,0,0,0][7]
#         df2_champ.loc[i, ['N']] = output2[0,0,0,0][7]

#         df.loc[i , ['AnomalousO']] = output00[0,0,0,0][8]
#         df2_champ.loc[i, ['AnomalousO']] = output2[0,0,0,0][8]

#         df.loc[i , ['NO']] = output00[0,0,0,0][9]
#         df2_champ.loc[i, ['NO']] = output2[0,0,0,0][9]

#         df.loc[i , ['Temperature']] = output00[0,0,0,0][10]
#         df2_champ.loc[i, ['Temperature']] = output2[0,0,0,0][10]

#         if count == 500:
#             end = time.time()
#             elapsed = end - start
#             print("Total Time:",elapsed, df['Doy'][i])
#             count=0
#         else:
#             count+=1




#     df.to_pickle(str(year) + 'msis00' + 'champ'  )  
#     df2_champ.to_pickle(str(year) + 'msis2' + 'champ'  )  

#     return