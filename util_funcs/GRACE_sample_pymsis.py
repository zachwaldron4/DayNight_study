

import numpy as np
import pandas as pd
from pymsis import msis
import time
import os
import sys  
# sys.path.insert(0, '../')
from scipy.io import loadmat  #allows us to read in .mat files
from datetime import datetime,timedelta
import warnings
warnings.filterwarnings("ignore")





#     ''' This function takes a dataframe that contains the satellite ephemeris
#     and data and adds the MSIS data directly to it for each satellite data point.'''

# YEARS = [2003] #,2003,2004,2005,2006,2007,2008,2009]
# DAYS = np.arange(1,5)
# path_grace = '../data/day_night_data/GRACE_2002_2012/'
# noaa_file = pd.read_pickle('constructed_files/noaa_2002_2010_pickle' )

def make_grace_pymsis_dfs(YEARS, DAYS, path_grace, noaa_file):
    

    noaa = noaa_file.loc[(noaa_file!=0).any(axis=1)]
    msis00 = {}
    msis2 = {}
    variables = ['Rho', 'N2', 'O2', 'O', 'He',
                 'H', 'Ar', 'N', 'AnomalousO', 'NO', 'Temperature',
                'Lon', 'Lat', 'Height', 'f107', 'f107a', 'ap',
                'Doy', 'Hours', 'LatBin', 'LocTim', 'Density', 'D500', 'Year'
                ]
    for var in variables:
        msis00[var] = np.ones(365*1900) * np.nan
        msis2[var]  = np.ones(365*1900) * np.nan

    date_list = []
    tleng = 0
    for iyear,year in enumerate(YEARS):
        start = time.time()
        for iday,day in enumerate(DAYS):



            filename = path_grace + '%d/matlab/Density_graceA_3deg_' % year + str(year)[-2:]  +'_%03d.mat' % day
            status = os.path.exists(filename)
            if status == True:
                data_grace = loadmat(filename)
            elif status == False:
                print('No File:', day,'/', year,'N/A', filename )
                continue
             #                 breakloop = True
#                 df = 0
#                 den_string = 'D500'
#                 alt_norm = 500

            Version = np.transpose(data_grace['Version']['data'][0][0])[0]
            Year = data_grace['Year']['data'][0][0][0][0]
            Doy = data_grace['Doy']['data'][0][0][0][0]
            Hours = np.transpose(data_grace['Sec']['data'][0][0])[0]/3600 #in hours
            Lon = np.transpose(data_grace['Lon']['data'][0][0])[0]
            Lat = np.transpose(data_grace['Lat']['data'][0][0])[0]
            LatBin = np.transpose(data_grace['LatBin']['data'][0][0])[0]
            Height =  np.transpose(data_grace['Height']['data'][0][0])[0]
            LocTim = np.transpose(data_grace['LocTim']['data'][0][0])[0]

            Density = np.transpose(data_grace['Density']['data'][0][0])[0]
            date_index = datetime(year, 1, 1) + timedelta(float(day) - 1) 



            ii = 0 + tleng
            leng2 = np.size(Height)
            for i,val in enumerate(Height):


                lon   = Lon[i]
                lat   = Lat[i]
                f107  = float(noaa['f107d'][date_index])  
                f107a = float(noaa['f107a'][date_index])
                ap    = float(noaa['Ap'][date_index]) 
                dates = datetime(year, 1, 1) + timedelta(float(day) - 1) + timedelta(hours = Hours[i]) 
                ndates = np.size(dates)

                # (F107, F107a, ap) all need to be specified at the same length as dates
                f107s = [f107]
                f107as = [f107a]
                aps = [[ap]*7]

                output00 = msis.run(dates, lon, lat, 500, f107, f107a, aps, version = 0)
                output2 = msis.run(dates, lon, lat, 500, f107, f107a, aps, version = 2)
                output00_sat = msis.run(dates, lon, lat, Height[i], f107, f107a, aps, version = 0)
                output2_sat = msis.run(dates, lon, lat, Height[i], f107, f107a, aps, version = 2)


                rho_sat = Density[i]
                rho_msis00_sat = output00_sat[0,0,0,0][0]
                rho_msis00_500 = output00[0,0,0,0][0] 
                rho_msis2_sat = output2_sat[0,0,0,0][0]
                rho_msis2_500 = output2[0,0,0,0][0] 

                msis00['Rho'][ii] = output00[0,0,0,0][0]
                msis00['N2'][ii] = output00[0,0,0,0][1]
                msis00['O2'][ii] = output00[0,0,0,0][2]
                msis00['O'][ii] = output00[0,0,0,0][3]
                msis00['He'][ii] = output00[0,0,0,0][4]
                msis00['H'][ii] = output00[0,0,0,0][5]
                msis00['Ar'][ii] = output00[0,0,0,0][6]
                msis00['N'][ii] = output00[0,0,0,0][7]
                msis00['AnomalousO'][ii] = output00[0,0,0,0][8]
                msis00['NO'][ii] = output00[0,0,0,0][9]
                msis00['Temperature'][ii] = output00[0,0,0,0][10]

                msis2['Rho'][ii] = output2[0,0,0,0][0]
                msis2['N2'][ii] = output2[0,0,0,0][1]
                msis2['O2'][ii] = output2[0,0,0,0][2]
                msis2['O'][ii] = output2[0,0,0,0][3]
                msis2['He'][ii] = output2[0,0,0,0][4]
                msis2['H'][ii] = output2[0,0,0,0][5]
                msis2['Ar'][ii] = output2[0,0,0,0][6]
                msis2['N'][ii] = output2[0,0,0,0][7]
                msis2['AnomalousO'][ii] = output2[0,0,0,0][8]
                msis2['NO'][ii] = output2[0,0,0,0][9]
                msis2['Temperature'][ii] = output2[0,0,0,0][10]

                msis2['Lon'][ii] =    Lon[i]   
                msis2['Lat'][ii] =    Lat[i]   
                msis2['Height'][ii] =    Height[i]   
                msis2['f107'][ii] =    f107  
                msis2['f107a'][ii] =    f107a 
                msis2['ap'][ii] =    ap    
                msis2['Doy'][ii] =    Doy 
                msis2['Hours'][ii] =    Hours[i] 
                msis2['LatBin'][ii]   =    LatBin[i] 
                msis2['LocTim'][ii]   =    LocTim[i] 
                msis2['Density'][ii]  =    Density[i] 
                msis2['D500'][ii]     =    rho_sat * (rho_msis2_500 / rho_msis2_sat) 
                msis2['Year'][ii]     =    Year

                msis00['Lon'][ii]     =    Lon[i]   
                msis00['Lat'][ii]     =    Lat[i]   
                msis00['Height'][ii]  =    Height[i]   
                msis00['f107'][ii]    =    f107  
                msis00['f107a'][ii]   =    f107a 
                msis00['ap'][ii]      =    ap    
                msis00['Doy'][ii]     =    Doy 
                msis00['Hours'][ii]   =    Hours[i] 
                msis00['LatBin'][ii]  =    LatBin[i] 
                msis00['LocTim'][ii]  =    LocTim[i] 
                msis00['Density'][ii] =    Density[i] 
                msis00['D500'][ii]    =    rho_sat * (rho_msis00_500 / rho_msis00_sat) 
                msis00['Year'][ii]    =    Year

                date_list.append(pd.to_datetime(dates))
                ii +=1

            tleng = tleng + leng2
            end = time.time()
            elapsed = end - start
            print('200',Year, ' / ', Doy, ":-- Total Time: ",elapsed , sep = '')

        print('Done looping through Days')
        df00 = pd.DataFrame(msis00)
        df2 = pd.DataFrame(msis2)

    #     msis00_df = df00.loc[(df00!=np.nan).all(axis=1)]
    #     msis2_df = df2.loc[(df2!=np.nan).all(axis=1)]
        msis00_df = df00[pd.notna(df00).any(axis=1)]
        msis2_df = df2[pd.notna(df2).any(axis=1)]

        msis00_df['Date'] = date_list
        msis2_df['Date'] = date_list
        print('Done')
        
        msis00_df.to_pickle('parallelize/' + str(year) + 'msis00make' + 'grace'  )  
        msis2_df.to_pickle('parallelize/' + str(year) + 'msis2make' + 'grace'  )  
        print('200',Year, ': Saved constructed dfs as pickles' , sep = '')


