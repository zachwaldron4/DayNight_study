import pandas as pd
import numpy as np 
import sys  
# import matplotlib.pyplot as plt
# from matplotlib import rc
from collections import defaultdict
from datetime import datetime,timedelta
import pickle
import os



def LatAverage_DayNightRatios_pymsis(years, days, SAT_CHOOSE, which_msis, path,  file_loc):

    ##############################################################
    # Identify the day-to-night ratio Parameters:
    ##############################################################
    Day_SLT1 = 11.5
    Day_SLT2 = 17.5
    Night_SLT1 = 5.5
    Night_SLT2 = 23.5
    Upper_lat = 42
    Lower_lat = -42
    latbin_bins = np.arange(-90, 90, 3)
    ##############################################################



    noaa = pd.read_pickle('constructed_files/noaa_2002_2010_pickle' )
    if SAT_CHOOSE == 'champ':
        den_string = 'D400'
    elif SAT_CHOOSE == 'grace':
        den_string = 'D500'

    # Naming Conventions
    identifier =  'pymsis'+which_msis+'_'+SAT_CHOOSE   
    filename = 'DNR_' + identifier + '.pkl'
    file_loc_plot =  'pymsis'+which_msis+'_'+SAT_CHOOSE+'/' 


    # Only run the following code if the DNR file does not exist
    if os.path.exists( path  + filename+ 'run anyway' ):
        print('File exists. Hurray!')
        print(path + filename)
        aver_loop = pd.read_pickle(path + filename)  
        pass

    else:
        print('Ooooof gotta make some pickle files')


        #     Define a latitude mask function to act on the dataframe
        def lats_mask_func(df):
            lats_mask =  np.logical_and(df['Lat'] <=  Upper_lat , df['Lat']  >= Lower_lat)
            return(lats_mask)


        mask_latbin_bins = np.arange(Lower_lat, Upper_lat+3, 3)


        sat_df = pd.read_pickle('constructed_files/pyMSIS'+which_msis+'_'+SAT_CHOOSE)
        sat_df = sat_df.reset_index()
        df = sat_df





        aver_loop  = pd.DataFrame(data = {  'Year' : []  ,
                                            'Day'  :  [] ,
                                            'DayHours'   : [] ,
                                            'NightHours' : [] ,
                                            'DateDay' : [] ,
                                            'DateNight': [] ,

                                            'DayLon'  :[],
                                            'NightLon' :[],

                                            'DayLat'  :[],
                                            'NightLat' :[],
                                            'LatBin'  :[],

                                            'DayHeight'  :[],
                                            'NightHeight' :[],

                                            'DayLocTim'  :[],
                                            'NightLocTim'  :[],


                                            'Day_'+ den_string +'_sat'  : [],
                                            'Night_'+ den_string +'_sat'  : [],
                                            'Day_'+ den_string +'_msis'  : [],
                                            'Night_'+ den_string +'_msis'  : [],

                                            'Day_TN'  : [],
                                            'Night_TN'  : [],
                                            'ratio_TN'  : [],

                                            'ratio_'+ den_string +'_msis'  : [],
                                            'ratio_'+ den_string +'_sat'  : [],

                                             })

        # Delete Unnecessary variables that will just take up memory

        del df['He']
        del df['H']
        del df['O']
        del df['N2']
        del df['O2']
        del df['N']
        del df['Ar'] 
        del df['AnomalousO']
        del df['NO']
        del df['f107']
        del df['f107a']
        del df['ap']

    #     del df['Cd']


        averagebin_day = pd.DataFrame(data ={ 'Year'        : [] ,     
                                              'Doy'          : [] ,
                                              'Hours'        : [] ,
                                              'Date'         : [] , 
                                              'Lat'          : [] ,
                                              'Lon'          : [] ,
                                              'Height'       : [],
                                              'LocTim'       : [] ,
                                              'LatBin'       : [] ,
                                              'Density'      : [] ,
                                              den_string         : [] , 
                                              'Rho'          : [] ,
                                              'Temperature'  : [] ,
                                                    } )


        averagebin_night = pd.DataFrame(data ={ 'Year'      : [] ,      
                                              'Doy'          : [] ,
                                              'Hours'        : [] ,
                                              'Date'         : [] , 
                                              'Lat'          : [] ,
                                              'Lon'          : [] ,
                                              'Height'       : [],
                                              'LocTim'       : [] ,
                                              'LatBin'       : [] ,
                                              'Density'      : [] ,
                                              den_string         : [] , 
                                              'Rho'          : [] ,
                                              'Temperature'  : [] ,
                                                    } )

        i = 0

        for iyear,year in enumerate(years):
            year_float = float(str(year)[-1])
            for iday,day in enumerate(days):
                loopindex = np.logical_and(df['Year'] == year_float, df['Doy'] == day )
                loop_df = df[loopindex]
    #             print(loop_df)

                date_index = datetime(year, 1, 1) + timedelta(float(day) - 1) 
                Ap = float(noaa['Ap'][date_index])  
                f107a = float(noaa['f107a'][date_index])
                f107d = float(noaa['f107d'][date_index])
                p107 = float(noaa['p107'][date_index])

                day_mask =  np.logical_and( loop_df['LocTim'] >= Day_SLT1   ,  loop_df['LocTim'] <= Day_SLT2) 
                night_mask = np.logical_or((loop_df['LocTim'] >= Night_SLT2), (loop_df['LocTim'] <= Night_SLT1)) 
                mask_Ap = np.logical_not(float(noaa['Ap'][date_index])>= 15)   

                for ilat, vallat in enumerate(latbin_bins):
                    averagebin_day.loc[ilat] =  loop_df[day_mask].loc[loop_df[day_mask]['LatBin'] == vallat].mean()
                    averagebin_night.loc[ilat] =  loop_df[night_mask].loc[loop_df[night_mask]['LatBin'] == vallat].mean()

                averageday_latmask = averagebin_day[lats_mask_func(averagebin_day) & mask_Ap]
                averagenight_latmask = averagebin_night[lats_mask_func(averagebin_night) & mask_Ap]

                for ii,ival in enumerate(mask_latbin_bins):

            #######################
            #       Date        
            #######################
                    if np.size(averageday_latmask['Hours'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['DateDay']] = np.nan
                    else:
                        aver_loop.loc[i, ['DateDay']] =  pd.to_datetime(datetime(year, 1, 1) + timedelta(days = float(day)-1,  hours = averageday_latmask['Hours'].loc[averageday_latmask['LatBin'] == ival].values[0]  ))
                    if np.size(averagenight_latmask['Hours'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['DateNight']]= np.nan
                    else:
                        aver_loop.loc[i, ['DateNight']]= pd.to_datetime(datetime(year, 1, 1) + timedelta(days = float(day)-1,  hours = averagenight_latmask['Hours'].loc[averagenight_latmask['LatBin'] == ival].values[0] ))

                    aver_loop.loc[i, ['Year']] = year
                    aver_loop.loc[i, ['Day']] = day

                    if np.size(averageday_latmask['Hours'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['DayHours']]= np.nan
                    else:
                        aver_loop.loc[i, ['DayHours']]= averageday_latmask['Hours'].loc[averageday_latmask['LatBin'] == ival].values[0] 
                    if np.size(averagenight_latmask['Hours'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['NightHours']]= np.nan
                    else:
                        aver_loop.loc[i, ['NightHours']]= averagenight_latmask['Hours'].loc[averagenight_latmask['LatBin'] == ival].values[0]
            #######################
            #       Lon        
            #######################
                    if np.size(averageday_latmask['Lon'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['DayLon']] = np.nan
                    else:
                        aver_loop.loc[i, ['DayLon']] = averageday_latmask['Lon'].loc[averageday_latmask['LatBin'] == ival].values[0] 


                    if np.size(averagenight_latmask['Lon'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['NightLon']] = np.nan
                    else:
                        aver_loop.loc[i, ['NightLon']] = averagenight_latmask['Lon'].loc[averagenight_latmask['LatBin'] == ival].values[0]
            #######################
            #       Lat        
            #######################
                    if np.size(averageday_latmask['Lat'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['DayLat']] = np.nan
                    else:
                        aver_loop.loc[i, ['DayLat']] = averageday_latmask['Lat'].loc[averageday_latmask['LatBin'] == ival].values[0] 

                    if np.size(averagenight_latmask['Lat'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['NightLat']] = np.nan
                    else:
                        aver_loop.loc[i, ['NightLat']] = averagenight_latmask['Lat'].loc[averagenight_latmask['LatBin'] == ival].values[0]

            #######################
            #       LatBin        
            #######################
                    aver_loop.loc[i, ['LatBin']] = ival

            #######################
            #       Height        
            #######################
                    if np.size(averageday_latmask['Height'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['DayHeight']] = np.nan
                    else:
                        aver_loop.loc[i, ['DayHeight']] = averageday_latmask['Height'].loc[averageday_latmask['LatBin'] == ival].values[0] 
                    if np.size( averagenight_latmask['Height'].loc[averagenight_latmask['LatBin'] == ival].values)==0:
                        aver_loop.loc[i, ['NightHeight']] = np.nan
                    else:
                        aver_loop.loc[i, ['NightHeight']] = averagenight_latmask['Height'].loc[averagenight_latmask['LatBin'] == ival].values[0]

            #######################
            #       Local Time        
            #######################            
                    if np.size(averageday_latmask['LocTim'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['DayLocTim']] = np.nan
                    else:
                        aver_loop.loc[i, ['DayLocTim']] = averageday_latmask['LocTim'].loc[averageday_latmask['LatBin'] == ival].values[0]                             

                    if np.size(averagenight_latmask['LocTim'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['NightLocTim']] = np.nan
                    else:
                        aver_loop.loc[i, ['NightLocTim']] = averagenight_latmask['LocTim'].loc[averagenight_latmask['LatBin'] == ival].values[0]


            #######################
            #       Temperature        
            #######################            
                    if np.size(averageday_latmask['Temperature'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['Day_TN']] = np.nan
                    else:
                        aver_loop.loc[i, ['Day_TN']] = averageday_latmask['Temperature'].loc[averageday_latmask['LatBin'] == ival].values[0]                             

                    if np.size(averagenight_latmask['Temperature'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['Night_TN']] = np.nan
                    else:
                        aver_loop.loc[i, ['Night_TN']] = averagenight_latmask['Temperature'].loc[averagenight_latmask['LatBin'] == ival].values[0]


            ####################################
            #      Normalized Satellite Density        
            ####################################            
                    if np.size(averageday_latmask[den_string].loc[averageday_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['Day_'+ den_string +'_sat']] = np.nan
                    else:
                        aver_loop.loc[i, ['Day_'+ den_string +'_sat']] = averageday_latmask[den_string].loc[averageday_latmask['LatBin'] == ival].values[0]                             

                    if np.size(averagenight_latmask[den_string].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['Night_'+ den_string +'_sat']] = np.nan
                    else:
                        aver_loop.loc[i, ['Night_'+ den_string +'_sat']] = averagenight_latmask[den_string].loc[averagenight_latmask['LatBin'] == ival].values[0]

            ####################################
            #      Normalized MSIS Density        
            ####################################            
                    if np.size(averageday_latmask['Rho'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['Day_'+ den_string +'_msis']] = np.nan
                    else:
                        aver_loop.loc[i, ['Day_'+ den_string +'_msis']] = averageday_latmask['Rho'].loc[averageday_latmask['LatBin'] == ival].values[0]                             

                    if np.size(averagenight_latmask['Rho'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
                        aver_loop.loc[i, ['Night_'+ den_string +'_msis']] = np.nan
                    else:
                        aver_loop.loc[i, ['Night_'+ den_string +'_msis']] = averagenight_latmask['Rho'].loc[averagenight_latmask['LatBin'] == ival].values[0]

            #########################
            #      Ratios        
            #########################            
                    if np.logical_or(np.size(averagenight_latmask['Temperature'].loc[averagenight_latmask['LatBin'] == ival].values) ==0, np.size(averageday_latmask['Temperature'].loc[averageday_latmask['LatBin'] == ival].values) ==0 ):
                        aver_loop.loc[i, ['ratio_TN']]= np.nan 
                    else:   
                        aver_loop.loc[i, ['ratio_TN']]= float(averageday_latmask['Temperature'].loc[averageday_latmask['LatBin'] == ival].values / averagenight_latmask['Temperature'].loc[averagenight_latmask['LatBin'] == ival].values) 


                    if np.logical_or(np.size(averagenight_latmask['Rho'].loc[averagenight_latmask['LatBin'] == ival].values) ==0, np.size(averageday_latmask['Rho'].loc[averageday_latmask['LatBin'] == ival].values) ==0 ):
                        aver_loop.loc[i, ['ratio_'+ den_string +'_msis']]= np.nan 
                    else:   
                        aver_loop.loc[i, ['ratio_'+ den_string +'_msis']]= float(averageday_latmask['Rho'].loc[averageday_latmask['LatBin'] == ival].values / averagenight_latmask['Rho'].loc[averagenight_latmask['LatBin'] == ival].values) 


                    if np.logical_or(np.size(averagenight_latmask[den_string].loc[averagenight_latmask['LatBin'] == ival].values) ==0, np.size(averageday_latmask[den_string].loc[averageday_latmask['LatBin'] == ival].values) ==0):
                        aver_loop.loc[i, ['ratio_'+ den_string +'_sat']]= np.nan
                    else:
                        aver_loop.loc[i, ['ratio_'+ den_string +'_sat']]=   float(averageday_latmask[den_string].loc[averageday_latmask['LatBin'] == ival].values / averagenight_latmask[den_string].loc[averagenight_latmask['LatBin'] == ival].values)


                    i+=1

                print(year,'/',day, '-- MSIS:', which_msis,'-- Sat:', SAT_CHOOSE)

        aver_loop.to_pickle(path  + filename  )  #+ str(year)
        print('Done making the Pickle files')
    return







# return
# aver_loop

# import pandas as pd
# import numpy as np 
# import sys  
# # import matplotlib.pyplot as plt
# # from matplotlib import rc
# from collections import defaultdict
# from datetime import datetime,timedelta
# import pickle
# import os


# def LatAverage_pymsis(years, days, SAT_CHOOSE, path, norm_alt, file_loc, sat_df):
#     noaa = pd.read_pickle('../data/noaa_2002_2010_pickle' )

#     Day_SLT1 = 11.5
#     Day_SLT2 = 17.5
#     Night_SLT1 = 5.5
#     Night_SLT2 = 23.5
#     Upper_lat = 42
#     Lower_lat = -42
#     latbin_bins = np.arange(-90, 90, 3)


#     identifier =  'pymsis00_champ'   
#     plot_identifier =  "MSISe00 at CHAMP"
#     filename = 'LatAverages' + identifier + '.pkl'

#     file_loc_plot =  'pymsis00_champ/' 

    
#     if os.path.exists( path  + filename ):
#         print('File exists. Hurray!')
#         print(path + filename)
#         aver_loop = pd.read_pickle(path + filename)  
#         pass


#     else:
#         print('Ooooof gotta make some pickle files')


#         def lats_mask_func(df):
#             lats_mask =  np.logical_and(df['Lat'] <=  Upper_lat , df['Lat']  >= Lower_lat)
#             return(lats_mask)


#         latbin_bins = np.arange(-90, 90, 3)
#         mask_latbin_bins = np.arange(Lower_lat, Upper_lat+3, 3)

#         df = sat_df
        
#         del df['Alt_norm']
#         del df['Alt_sat']
#         del df['He']
#         del df['O']
#         del df['N2']
#         del df['O2']
#         del df['N']
#         del df['Ar'] 
#         del df['AnomO'] 
#         del df['Texo']
#         del df['Mlat']
#         del df['Mlon']
#         del df['Mlt']
#         del df['U_rho']
#         del df['Num']
#         del df['NumInterp']
#         del df['Cd']


      
        
#         aver_loop  = pd.DataFrame(data = {  'Year' : []  ,
#                                             'Day'  :  [] ,
#                                             'DayHours'   : [] ,
#                                             'NightHours' : [] ,
#                                             'DateDay' : [] ,
#                                             'DateNight': [] ,

#                                             'DayLon'  :[],
#                                             'NightLon' :[],

#                                             'DayLat'  :[],
#                                             'NightLat' :[],
#                                             'LatBin'  :[],

#                                             'DayHeight'  :[],
#                                             'NightHeight' :[],

#                                             'DayLocTim'  :[],
#                                             'NightLocTim'  :[],


#                                             'Day_D400_champ'  : [],
#                                             'Night_D400_champ'  : [],
#                                             'Day_Rho_400'  : [],
#                                             'Night_Rho_400'  : [],
                                            
#                                             'Day_TN'  : [],
#                                             'Night_TN'  : [],
#                                             'ratio_TN'  : [],
                                          
#                                             'ratio_Rho_400'  : [],
#                                             'ratio_D400_champ'  : [],
                                          

#                                              })

#         averagebin_day = pd.DataFrame(data ={'Year'       : [] , 
#                                               'Day'          : [] ,
#                                               'Hours'        : [] ,
#                                               'Date'         : [] , 
#                                               'Lat'           : [] ,
#                                               'Lon'           : [] ,
#                                               'SLT'           : [] ,
#                                               'LatBin'        : [] ,
#                                               'Density_champ' : [] ,
#                                               'D400_champ'   : [] , 
#                                               'Dmsis_champ'  : [] , 
#                                               'Height'       : [] ,
#                                               'Rho_400'      : [] ,
#                                               'Rho_sat'      : [] ,
#                                               'TN'           : [] ,
#                                               'F107a'        : [] ,
#                                               'F107'         : [] ,
#                                               'Ap'           : []  ,
#                                                     } )
        

#         averagebin_night = pd.DataFrame(data = {'Year'       : [] , 
#                                               'Day'          : [] ,
#                                               'Hours'        : [] ,
#                                               'Date'         : [] , 
#                                               'Lat'           : [] ,
#                                               'Lon'           : [] ,
#                                               'SLT'           : [] ,
#                                               'LatBin'        : [] ,
#                                               'Density_champ' : [] ,
#                                               'D400_champ'   : [] , 
#                                               'Dmsis_champ'  : [] , 
#                                               'Height'       : [] ,
#                                               'Rho_400'      : [] ,
#                                               'Rho_sat'      : [] ,
#                                               'TN'           : [] ,
#                                               'F107a'        : [] ,
#                                               'F107'         : [] ,
#                                               'Ap'           : []  ,
#                                                     } )

#         i = 0

#         for iyear,year in enumerate(years):

            
# #             Index(['Year', 'Day', 'Date', 'F107a', 'F107', 'Ap', 'Alt_norm', 'Alt_sat',
# #        'Lat', 'Lon', 'Rho_400', 'Rho_sat', 'TN', 'SLT', 'SLT_msis', 'Hours',
# #        'LatBin', 'Height', 'Density_champ', 'D400_champ', 'D410_champ',
# #        'Dmsis_champ'],

            
#             for iday,day in enumerate(days):
#                 loopindex = np.logical_and(df['Year'] == year, df['Day'] == day )
# #                 print(loopindex[loopindex == True].shape)
# # #                 print(year, day)
#                 loop_df = df[loopindex]
# #                 print(loop_df.Day)

#                 date_index = datetime(year, 1, 1) + timedelta(float(day) - 1) 
#                 Ap = float(noaa['Ap'][date_index])  # arithmetic mean of 8 Ap values
#                 f107a = float(noaa['f107a'][date_index])
#                 f107d = float(noaa['f107d'][date_index])
#                 p107 = float(noaa['p107'][date_index])

# #                 day_mask =  np.logical_and(champ['LocTim'] >= 10.5, champ['LocTim'] <= 16.5)
# #                 night_mask = np.logical_or((champ['LocTim'] >= 22.5), (champ['LocTim'] <= 4.5)) 
#                 day_mask =  np.logical_and( loop_df['SLT'] >= Day_SLT1   ,  loop_df['SLT'] <= Day_SLT2) 
#                 night_mask = np.logical_or((loop_df['SLT'] >= Night_SLT2), (loop_df['SLT'] <= Night_SLT1)) 


#                 mask_Ap = np.logical_not(float(noaa['Ap'][date_index])>= 15)   

#                 for ilat, vallat in enumerate(latbin_bins):
#                     averagebin_day.loc[ilat] =  loop_df[day_mask].loc[loop_df[day_mask]['LatBin'] == vallat].mean()
#                     averagebin_night.loc[ilat] =  loop_df[night_mask].loc[loop_df[night_mask]['LatBin'] == vallat].mean()
# #                     print(averagebin_day.loc[ilat])
#                 averageday_latmask = averagebin_day[lats_mask_func(averagebin_day) & mask_Ap]
#                 averagenight_latmask = averagebin_night[lats_mask_func(averagebin_night) & mask_Ap]

#                 for ii,ival in enumerate(mask_latbin_bins):

#                     # DATE:

#                     if np.size(averageday_latmask['Hours'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['DateDay']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['DateDay']] =  pd.to_datetime(datetime(year, 1, 1) + timedelta(days = float(day)-1,  hours = averageday_latmask['Hours'].loc[averageday_latmask['LatBin'] == ival].values[0]  ))
#                     if np.size(averagenight_latmask['Hours'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['DateNight']]= np.nan
#                     else:
#                         aver_loop.loc[i, ['DateNight']]= pd.to_datetime(datetime(year, 1, 1) + timedelta(days = float(day)-1,  hours = averagenight_latmask['Hours'].loc[averagenight_latmask['LatBin'] == ival].values[0] ))

#                     aver_loop.loc[i, ['Year']] = year
#                     aver_loop.loc[i, ['Day']] = day

#                     if np.size(averageday_latmask['Hours'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['DayHours']]= np.nan
#                     else:
#                         aver_loop.loc[i, ['DayHours']]= averageday_latmask['Hours'].loc[averageday_latmask['LatBin'] == ival].values[0] 
#                     if np.size(averagenight_latmask['Hours'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['NightHours']]= np.nan
#                     else:
#                         aver_loop.loc[i, ['NightHours']]= averagenight_latmask['Hours'].loc[averagenight_latmask['LatBin'] == ival].values[0]

#                     if np.size(averageday_latmask['Lon'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['DayLon']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['DayLon']] = averageday_latmask['Lon'].loc[averageday_latmask['LatBin'] == ival].values[0] 


#                     if np.size(averagenight_latmask['Lon'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['NightLon']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['NightLon']] = averagenight_latmask['Lon'].loc[averagenight_latmask['LatBin'] == ival].values[0]

#                     if np.size(averageday_latmask['Lat'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['DayLat']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['DayLat']] = averageday_latmask['Lat'].loc[averageday_latmask['LatBin'] == ival].values[0] 

#                     if np.size(averagenight_latmask['Lat'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['NightLat']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['NightLat']] = averagenight_latmask['Lat'].loc[averagenight_latmask['LatBin'] == ival].values[0]

#                     aver_loop.loc[i, ['LatBin']] = ival

                    
  
                    
                    
#                     if np.size(averageday_latmask['Height'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['DayHeight']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['DayHeight']] = averageday_latmask['Height'].loc[averageday_latmask['LatBin'] == ival].values[0] 
#                     if np.size( averagenight_latmask['Height'].loc[averagenight_latmask['LatBin'] == ival].values)==0:
#                         aver_loop.loc[i, ['NightHeight']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['NightHeight']] = averagenight_latmask['Height'].loc[averagenight_latmask['LatBin'] == ival].values[0]

#                     if np.size(averageday_latmask['SLT'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['DayLocTim']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['DayLocTim']] = averageday_latmask['SLT'].loc[averageday_latmask['LatBin'] == ival].values[0]                             

#                     if np.size(averagenight_latmask['SLT'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['NightLocTim']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['NightLocTim']] = averagenight_latmask['SLT'].loc[averagenight_latmask['LatBin'] == ival].values[0]


                        
                        
#                     if np.size(averageday_latmask['TN'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['Day_TN']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['Day_TN']] = averageday_latmask['TN'].loc[averageday_latmask['LatBin'] == ival].values[0]                             

#                     if np.size(averagenight_latmask['TN'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['Night_TN']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['Night_TN']] = averagenight_latmask['TN'].loc[averagenight_latmask['LatBin'] == ival].values[0]

                        
#                     if np.logical_or(np.size(averagenight_latmask['TN'].loc[averagenight_latmask['LatBin'] == ival].values) ==0, np.size(averageday_latmask['TN'].loc[averageday_latmask['LatBin'] == ival].values) ==0 ):
#                         aver_loop.loc[i, ['ratio_TN']]= np.nan 
#                     else:   
#                         aver_loop.loc[i, ['ratio_TN']]= float(averageday_latmask['TN'].loc[averageday_latmask['LatBin'] == ival].values / averagenight_latmask['TN'].loc[averagenight_latmask['LatBin'] == ival].values) #averageday_latmask


#                     if np.size(averageday_latmask['D400_champ'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['Day_D400_champ']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['Day_D400_champ']] = averageday_latmask['D400_champ'].loc[averageday_latmask['LatBin'] == ival].values[0]                             

#                     if np.size(averagenight_latmask['D400_champ'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['Night_D400_champ']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['Night_D400_champ']] = averagenight_latmask['D400_champ'].loc[averagenight_latmask['LatBin'] == ival].values[0]


                        
#                     if np.size(averageday_latmask['Rho_400'].loc[averageday_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['Day_Rho_400']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['Day_Rho_400']] = averageday_latmask['Rho_400'].loc[averageday_latmask['LatBin'] == ival].values[0]                             

#                     if np.size(averagenight_latmask['Rho_400'].loc[averagenight_latmask['LatBin'] == ival].values) == 0:
#                         aver_loop.loc[i, ['Night_Rho_400']] = np.nan
#                     else:
#                         aver_loop.loc[i, ['Night_Rho_400']] = averagenight_latmask['Rho_400'].loc[averagenight_latmask['LatBin'] == ival].values[0]

                        
                        
#                     if np.logical_or(np.size(averagenight_latmask['Rho_400'].loc[averagenight_latmask['LatBin'] == ival].values) ==0, np.size(averageday_latmask['Rho_400'].loc[averageday_latmask['LatBin'] == ival].values) ==0 ):
#                         aver_loop.loc[i, ['ratio_Rho_400']]= np.nan 
#                     else:   
#                         aver_loop.loc[i, ['ratio_Rho_400']]= float(averageday_latmask['Rho_400'].loc[averageday_latmask['LatBin'] == ival].values / averagenight_latmask['Rho_400'].loc[averagenight_latmask['LatBin'] == ival].values) #averageday_latmask

                    
#                     if np.logical_or(np.size(averagenight_latmask['D400_champ'].loc[averagenight_latmask['LatBin'] == ival].values) ==0, np.size(averageday_latmask['D400_champ'].loc[averageday_latmask['LatBin'] == ival].values) ==0):
#                         aver_loop.loc[i, ['ratio_D400_champ']]= np.nan
#                     else:
#                         aver_loop.loc[i, ['ratio_D400_champ']]=   float(averageday_latmask['D400_champ'].loc[averageday_latmask['LatBin'] == ival].values / averagenight_latmask['D400_champ'].loc[averagenight_latmask['LatBin'] == ival].values)

                    
#                     i+=1



#                 print(year,'/',day)

            
#         aver_loop.to_pickle(path + str(year) + filename  )  
        
        
        
    
    


    
#     return
# # aver_loop