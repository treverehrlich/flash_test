import pandas as pd
import numpy as np
import time
import math
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from davinci.services.auth import get_secret

# Weather-related functions to get forecasts or history for given locations or routes

def getWeatherByLocationDay(df,zipcode_field,date_field, zip_length):
    """
    Get the weather data for given location(s) for any date back to 2021

    :param df: weather data
        'zipcode': string
        'date': datetime
    :type df: pd.DataFrame

    :param zipcode_field: name of the field in the df containing the zip code
    :type zipcode_field: string

    :param date_field: name of the field in the df containing the date
    :type date_field: string

    :param zip_length: are we using 3 or 5 digit zips
    :type zip_length: int

    :rtype: pd.DataFrame
    """

    print("Preloading bulk zips, lats/longs, and weather dataframes...")

    # load coordinates for the zip codes
    coords_df = _zip_latlong_bulk(zip_length)
    grid_df = _get_grid_bulk()
    wx_df = _get_weather_bulk()

    print("Processing/joining...")
    df = pd.merge(df,coords_df,left_on=[zipcode_field],right_on=['zipcode'], how='left')
    df = pd.merge(df,grid_df,left_on=['lat','lng'],right_on=['lat','lng'], how='left')

    # merge in the weather for those coordinates
    df = pd.merge(df,wx_df,left_on=['name',date_field],right_on=['name','datetime'], how='left')

    # cleanup unneeded/duplicate columns
    df = df.drop(['name','datetime','lat','lng','zipcode'],axis = 1)

    return df

def getWeatherAtOriginDestination(df,o_zipcode_field,o_date_field,d_zipcode_field,d_date_field,zip_length):
    """
    Get the weather data for a given origin (o) location and date, and also for the destination (d) location and date
    Not worried about any weather inbetween these points

    :param df: weather data
        'o_zipcode': string
        'o_date': datetime
        'd_zipcode': string
        'd_date': datetime

    :param o_zipcode_field: name of the field in the df containing the origin zip code
    :type o_zipcode_field: string

    :param o_date_field: name of the field in the df containing the origin date
    :type o_date_field: string

    :param d_zipcode_field: name of the field in the df containing the destination zip code
    :type d_zipcode_field: string

    :param d_date_field: name of the field in the df containing the destination date
    :type d_date_field: string

    :type df: pd.DataFrame

    :rtype: pd.DataFrame
        o_zipcode, o_date, d_zipcode,d_date,o_tempmax,o_tempmin....d_tempmax,d_tempmin...
    """

    time_s = time.time()

    print("Preloading bulk zips, lats/longs, and weather dataframes...")

    # pull in supporting df's
    coords_df = _zip_latlong_bulk(zip_length)
    grid_df = _get_grid_bulk()
    wx_df = _get_weather_bulk()

    print(f"Bulk df's loaded; {int(time.time() - time_s)}s so far...")

    # ORIGIN WEATHER

    # load coordinates for the zip codes
    df = pd.merge(df,coords_df,left_on=[o_zipcode_field],right_on=['zipcode'], how='left')

    print(f"Orig: Merged coords for zip codes; {int(time.time() - time_s)}s so far...")

    # join those coordinates to the grid
    df = pd.merge(df,grid_df,left_on=['lat','lng'],right_on=['lat','lng'], how='left')
    df = df.rename(columns={'name': 'o_name','lat': 'o_lat', 'lng': 'o_lng'})

    print(f"Orig: Merged coords to weather grid; {int(time.time() - time_s)}s so far...")

    # rename all weather df columns with 'o_'
    for index, column_name in enumerate(wx_df.columns):
        wx_df.rename(columns={column_name: f'o_{column_name}'}, inplace=True)

    # join in the weather data
    df = pd.merge(df,wx_df,left_on=['o_name',o_date_field],right_on=['o_name','o_datetime'], how='left')

    print(f"Orig: Joined to the weather data; {int(time.time() - time_s)}s so far...")

    # cleanup
    df = df.drop(['zipcode','o_lat','o_lng','o_name','o_datetime'], axis=1)

    # DESTINATION WEATHER

    # load coordinates for the zip codes
    df = pd.merge(df,coords_df,left_on=[d_zipcode_field],right_on=['zipcode'], how='left')

    print(f"Dest: Merged coords for zip codes; {int(time.time() - time_s)}s so far...")

    # join those coordinates to the grid
    df = pd.merge(df,grid_df,left_on=['lat','lng'],right_on=['lat','lng'], how='left')
    df = df.rename(columns={'name': 'd_name','lat': 'd_lat', 'lng': 'd_lng'})

    print(f"Dest: Merged coords to weather grid; {int(time.time() - time_s)}s so far...")

    # rename everything with 'd_'
    for index, column_name in enumerate(wx_df.columns):
        wx_df.rename(columns={column_name: f'd_{column_name[2:]}'}, inplace=True)

    # join in the weather data
    df = pd.merge(df,wx_df,left_on=['d_name',d_date_field],right_on=['d_name','d_datetime'], how='left')

    print(f"Dest: Joined to the weather data; {int(time.time() - time_s)}s so far...")

    # cleanup
    df = df.drop(['zipcode','d_lat','d_lng','d_name','d_datetime'], axis=1)

    return df

def getWeatherAlongLanes(df,travel_id_field,origin_zipcode_field,origin_date_field,dest_zipcode_field,zip_length,tolerance=1):
    """

    Note: do not pass any other fields in the df besides the ones listed above; aggregate functions are applied to the df, etc.  
    The travel_id will be returned with the dataframe, allowing easy joining to the returned weather data

    Get the aggregated weather data for each day of travel along the respective route
    For a given route, return the weather grid cells traveled through or near on each respective day of a single or multi-day trip
    This function may be especially useful for large/ad-hoc data modeling tasks.  

    :param df: weather data
        'travel_id': string
        'origin_zipcode': string
        'origin_date': datetime
        'dest_zipcode': string
    :type df: pd.DataFrame

    :param travel_id_field: name of the field in the df containing the travel_id trip identifier
    :type travel_id_field: string

    :param origin_zipcode_field: name of the field in the df containing the origin zip code
    :type origin_zipcode_field: string

    :param origin_date_field: name of the field in the df containing the origin date
    :type origin_date_field: string

    :param dest_zipcode_field: name of the field in the df containing the destination zip code
    :type dest_zipcode_field: string

    :param zip_length: are we using 3 or 5 digit zips
    :type zip_length: int

    :param tolerance: how close to the route should the grid cell be? This is in degrees, where 1 is roughly 60 miles
    :type tolerance: float

    :rtype:  pd.DataFrame
    """

    # rename df columns to have consistent internal names
    df = df.rename(columns={travel_id_field : 'travel_id', origin_zipcode_field: 'origin_zipcode', origin_date_field: 'origin_date',dest_zipcode_field: 'dest_zipcode'})
    #_nullCheck(df,['travel_id','origin_zipcode','origin_date','dest_zipcode'])

    # pull in supporting df's
    print("Preloading bulk zips, lats/longs, calendars and weather dataframes...")
    coords_df = _zip_latlong_bulk(zip_length)
    grid_df = _get_grid_bulk()
    cal_df = _get_Kenco_calendar_bulk()
    cal_df['Date'] = pd.to_datetime(cal_df['Date'])
    wx_df = _get_weather_bulk()
    wx_df = wx_df[['name','datetime','temp','humidity','precip','snow','windgust','windspeed','cloudcover','visibility']]

    print("Beginning processing...")

    # load coordinates for the zip codes
    print("Merging zipcodes to lat/long...")
    df = pd.merge(df,coords_df,left_on=['origin_zipcode'],right_on=['zipcode'], how='left')
    df = df.rename(columns={'lat': 'origin_lat', 'lng': 'origin_lng'})

    df = pd.merge(df,coords_df,left_on=['dest_zipcode'],right_on=['zipcode'], how='left')
    df = df.rename(columns={'lat': 'dest_lat', 'lng': 'dest_lng'})
    df = df.drop(columns={'zipcode_x','zipcode_y'})

    # Find the overall flat-plane, straightline travel distance
    print("Calculating route distances...")
    df['distance'] = df.apply(lambda row: _getDistanceBetweenTwoPoints(row), axis = 1)

    # Find out how many days this route should take
    print("Estimating service days...")
    df['service_days'] = df.apply(lambda row: _getTravelDays(row), axis = 1)

    daily_df = pd.DataFrame()

    # Get the list of daily route segments
    print("Getting daily route segments...")

    counter = 0
    for index, row in df.iterrows():
        new_df = _getDailyRoutes(row)
        new_df['service_date'] =  _getServicesDates(new_df,cal_df,row['service_days'],row['origin_date'])
        daily_df = pd.concat([daily_df,new_df], ignore_index=True,sort=False)
        if (counter % 1000 == 0 and counter > 0): print(f'{counter} routes processed so far...')
        counter += 1

    print(f'Processed {counter} routes total.')

    # get the daily segment distance
    print("Estimating daily distances")
    daily_df['distance'] = daily_df.apply(lambda row: _getDistanceBetweenTwoPoints(row), axis = 1)

    # For each day's route, get the list of grid cells along that route
    daily_route_grids_df = pd.DataFrame()

    print("Finding which lat/long degree grid coordinates will be encountered that day...")

    counter = 0
    for index, row in daily_df.iterrows():
        # only do this if there's a valid distance;
        new_df.drop(new_df.index, inplace=True)
        if (~np.isnan(row['distance'])):
            new_df = _getGridAlongRoute(row,tolerance)
        daily_route_grids_df = pd.concat([daily_route_grids_df,new_df], ignore_index=True,sort=False)
        if (counter % 1000 == 0 and counter > 0): print(f'{counter} daily route segments processed so far...')
        counter += 1

    print(f'Processed {counter} daily route segments total.')

    # at this point, we have daily_route_grids_df filled with every route's daily lat/long coords that will be encountered

    # get the weather grid point names (from weather_grid table)
    print("Merging in the grid...")
    df_complete = pd.merge(daily_route_grids_df,grid_df,left_on=['origin_lng','origin_lat'],right_on=['lng','lat'], how='left')
    df_complete = df_complete.drop(columns={'lat','lng'})

    # finally merge in the weather for the locations passed thru, for the given service date of the travel
    print("Joining the daily grid points encountered with the weather data...")
    df_complete = pd.merge(df_complete,wx_df,left_on=['name','service_date'],right_on=['name','datetime'], how='left')
    df_complete = df_complete.drop(columns={'origin_lng','origin_lat','name','datetime'})
    df_complete = df_complete.sort_values(['travel_id', 'service_day_num'], ascending=[True, True])

    print("Finally, aggregating the daily weather....")
    group_obj = df_complete.groupby(['travel_id','service_day_num','origin_date','origin_zipcode','dest_zipcode','service_date'])

    df_aggregate = group_obj.agg(
        temp_min=('temp','min'),
        temp_max=('temp','max'),
        temp_avg=('temp','mean'),
        humidity_min=('humidity','min'),
        humidity_max=('humidity','max'),
        humidity_avg=('humidity','mean'),
        precip_min=('precip','min'),
        precip_max=('precip','max'),
        precip_avg=('precip','mean'),
        snow_min=('snow','min'),
        snow_max=('snow','max'),
        snow_avg=('snow','mean'),
        windgust_min=('windgust','min'),
        windgust_max=('windgust','max'),
        windgust_avg=('windgust','mean'),
        windspeed_min=('windspeed','min'),
        windspeed_max=('windspeed','max'),
        windspeed_avg=('windspeed','mean'),
        cloudcover_min=('cloudcover','min'),
        cloudcover_max=('cloudcover','max'),
        cloudcover_avg=('cloudcover','mean'),
        visibility_min=('visibility','min'),
        visibility_max=('visibility','max'),
        visibility_avg=('visibility','mean')
    )

    print("*** Processing route weather complete! ***")

    return df_aggregate

def _zip_latlong_bulk(zip_length):

    """
    Returns a dataframe of ALL zips from the EDW zip code repository in order to find the lat/long coordinate
    Returns the latitude and longitude, rounded to the nearest degree

    The zip field must consistently be either 5 digit zips, or the first three digits (typically used to indicate trucking lanes).
    If the first line has a three-digit zip, assume all are that format, else assume all are 5 digits

    :param zip_length: are we using 3 or 5 digit zips
    :type zip_length: int

    :rtype: pd.DataFrame

    """

    coords_query = """
            SELECT LEFT(zip,{zip_length}) as zipcode, ROUND(AVG(CONVERT(float,Lat)),0) AS lat, ROUND(AVG(CONVERT(float,Long)),0) AS lng
            FROM (
                SELECT RIGHT('00'+CAST(ZipCode AS VARCHAR(5)),5) as zip,Lat,Long,State,City
                FROM {db}.[dim].[COMMON_ZipCode]
                WHERE Lat IS NOT NULL and Long IS NOT NULL
                ) as z
            GROUP BY LEFT(zip,{zip_length})
            """.format(zip_length=zip_length,db=get_secret("EDW_SQL_DATABASE",doppler=True))

    mssql_engine = _get_sqalchemy_engine("SQL")
    with mssql_engine.begin() as mssql_conn:
        coords_df = pd.read_sql_query(coords_query, mssql_conn)

    return coords_df

def _nullCheck(df,field_list):
    """ Before processing, check all columns of the input df to make sure none of the fields are null

    :param df: the dataframe passed into this codebase weather function
    :type df: pd.DataFrame

    """

    df = df[field_list].replace('', np.nan)
    na_count = df[field_list].isna().sum().sum()

    if (na_count > 0):
        print(f'Found {na_count} nulls in the dataframe; please fix then retry.')
        raise Exception("Sorry, nulls have been identified in the dataframe. Please clean up.")


def _getServicesDates(new_df,cal_df,service_days,origin_date):
    """ For the given dataframe which contains one routes' daily segments, get the corresponding dates
    for the required number of days

    Business rules: don't run on holidays, allow runs on weekends, but dropoff can't be on a weekend

    :param new_df: the given route for which we need the business dates
    :type new_df: pd.DataFrame

    :param cal_df: Kenco's calendar df
    :type cal_df: pd.DataFrame

    :param service_days: number of dates that are needed
    :type service_days: int

    :param origin_date: date from which the subsequent dates are calculated
    :type origin_date: datetime

    :rtype: array

    """

    df = cal_df[(cal_df.Date >= origin_date) & (cal_df.IsHoliday == 0)].head(service_days)

    if (df.iloc[-1]['IsWeekday'] == 0): # delivery date was a weekend, so find next potential day
        new_df = cal_df[(cal_df.Date > df.iloc[-1]['Date']) & (cal_df.IsWeekday == 1) & (cal_df.IsHoliday == 0)].head(1)
        new_date = new_df.iloc[0]['Date']
        df['Date'].iloc[-1] = new_date

    return df['Date'].values



def _getGridAlongRoute(row,tolerance=1):

    """
    For a given route segment, return the grid cells traveled over that entire route

    :param df: all the day routes
    :type df: pd.DataFrame

    :param tolerance: how close to the route should the grid points be? This is in degrees, where 1 is roughly 60 miles
    :type tolerance: float

    :rtype:  pd.DataFrame
    """

    # Conveniently, each degree of distance from above is also the size of each weather grid cell.
    # We next break the line into that many pieces, and then find the location of each piece
    # which should tell us which grid cells the line has passed through

    # can re-use to get daily routes to get a x/y for each segment
    segment_df = _getDailyRoutes(row,False)

    # Round the coordinates to ints, which should correspond to the weather grid cells
    segment_df.origin_lng = segment_df.origin_lng.round()
    segment_df.origin_lat = segment_df.origin_lat.round()

    # drop unneeded data since we aren't using these as routes
    segment_df.drop(['dest_lng','dest_lat'], axis=1, inplace=True)

    # add the day number to correspond with the route day
    segment_df['service_day_num'] = row['service_day_num']
    segment_df['service_date'] = row['service_date']

    segment_df = segment_df.drop_duplicates()

    return segment_df


def _get_grid_bulk():

    """ Returns a df with list of weather grid, with lat and long

    :rtype: pd.DataFrame

    """

    grid_query = """SELECT [name],[lat],[long] as [lng] FROM {db}.[dbo].[weather_grid]""".format(db=get_secret("EDW_MISC_STAGING_DB",doppler=True))

    mssql_engine = _get_sqalchemy_engine("SQL")
    with mssql_engine.begin() as mssql_conn:
        grid_df = pd.read_sql_query(grid_query, mssql_conn)

    return grid_df

def _get_Kenco_calendar_bulk():

    """ Returns a df with list of Kenco grid, with lat and long

    :rtype: pd.DataFrame

    """

    cal_query = """SELECT [Date]
      ,[IsHoliday]
      ,[IsWeekday]
    FROM {db}.[dim].[COMMON_Date] ORDER BY [Date] ASC
    """.format(db=get_secret("EDW_SQL_DATABASE",doppler=True))

    mssql_engine = _get_sqalchemy_engine("SQL")
    with mssql_engine.begin() as mssql_conn:
        cal_df = pd.read_sql_query(cal_query, mssql_conn)

    return cal_df

def _get_weather_bulk():

    """ Returns a df with list of weather grid, with lat and long

    :rtype: pd.DataFrame
    """

    wx_query = """SELECT [name]
      ,[datetime]
      ,[tempmax]
      ,[tempmin]
      ,[temp]
      ,[feelslikemax]
      ,[feelslikemin]
      ,[feelslike]
      ,[dew]
      ,[humidity]
      ,[precip]
      ,[precipprob]
      ,[precipcover]
      ,[preciptype]
      ,[snow]
      ,[snowdepth]
      ,[windgust]
      ,[windspeed]
      ,[winddir]
      ,[sealevelpressure]
      ,[cloudcover]
      ,[visibility]
      ,[solarradiation]
      ,[solarenergy]
      ,[uvindex]
      ,[severerisk]
      ,[sunrise]
      ,[sunset]
      ,[moonphase]
      ,[conditions]
      ,[description]
      ,[icon]
      ,[stations] FROM {db}.[dbo].[weather_data]""".format(db=get_secret("EDW_MISC_STAGING_DB",doppler=True))

    mssql_engine = _get_sqalchemy_engine("SQL")
    with mssql_engine.begin() as mssql_conn:
        wx_df = pd.read_sql_query(wx_query, mssql_conn)

    wx_df['datetime'] = pd.to_datetime(wx_df['datetime'], format='%Y-%m-%d')

    return wx_df

def _getDistanceBetweenTwoPoints(row):

    """
    For a set of origin/destination lat/long points, return mathematical flat plane distance; not miles

    :param df: the routes
    :type df: pd.DataFrame

    :rtype: pd.DataFrame
    """

    # the calculations in the functions below don't like negative numbers which are found in US longitude
    x1 = abs(row['origin_lng'])
    y1 = abs(row['origin_lat'])
    x2 = abs(row['dest_lng'])
    y2 = abs(row['dest_lat'])

    return math.sqrt((x2-x1)**2 + (y2-y1)**2)


def _getTravelDays(row):

    """
    For a given distance, return the number of days it should take a trucker to cover those miles
    Due to government regulations, using an average of 600 miles per day

    :param dist: distance in flat-plan geographical degrees
    :type dist: float

    :return: estimated number of days of travel
    :rtype: int

    """

    #First we convert geographical degrees to miles
    # a latitude degree is approx 69 miles; a longitude degree is 54 miles
    # so approximating to 60 miles per degree

    dist = row['distance']*60

    if dist < 550:
        return 1
    elif dist < 1100:
        return 2
    elif dist < 1650:
        return 3
    elif dist < 2200:
        return 4
    elif dist < 2750:
        return 5
    else:
        return 6


def _getDailyRoutes(row,daily=True):
    """
    For a given route, break it into segments equal to the number of days required to travel the route
    Return a new dataframe - a row per day of the route

    :param df: routes
    :type df: pd.DataFrame

    :param daily: if true, then divide the segments by number of days; otherwise, we're re-using using this function to get the grid x/y encountered in a single day
    :type daily: boolean (default: True)

    :rtype: pd.DataFrame

    Example output

    """

    # This function is multi-purpose; can be used to divide the distance into multiple segments, one for each day
    # We also use it to find the number of grid lat/long degrees, so if the daily flag is false, we divide the
    # distance by the distance, which returns 1, which means a granularity of one degree, which is the level of
    # weather detail we're after

    if daily == True:
        #date_tag = row['origin_date']
        divide_by = row['service_days']

    else:
        divide_by = int(row['distance'])
        #date_tag = row['date']

    x1 = abs(row['origin_lng'])
    y1 = abs(row['origin_lat'])
    x2 = abs(row['dest_lng'])
    y2 = abs(row['dest_lat'])

    # get the strightline distance between the two points
    x_diff = abs(x1-x2)
    y_diff = abs(y1-y2)

    # divide that distance by number of days

    # if the from/to are in the same lat/long grid, the distance will be zero, triggering errors
    # so set divide_by to 1, and the offset will be 0 since we're not traversing any route
    if (divide_by == 0):
       divide_by = 1
       daily_x = 0
       daily_y = 0
    else:
        daily_x = x_diff/divide_by
        daily_y = y_diff/divide_by


    # are we moving in a positive or negative direction?
    xDirIsPositive = True if (x1 - x2) < 0 else False
    yDirIsPositive = True if (y1 - y2) < 0 else False

    new_x = x1
    new_y = y1

    df = pd.DataFrame()

    # for each day, accumulate which lat/longs should be traveled that day
    day_range = range(0,divide_by)
    for i in day_range:

        start_x = new_x
        start_y = new_y

        new_x = new_x + daily_x if xDirIsPositive else new_x - daily_x
        new_y = new_y + daily_y if yDirIsPositive else new_y - daily_y

        # adding to list of routes; notice adding back in the "-" for US
        new_row = pd.DataFrame({'travel_id' : row['travel_id']
            , 'service_day_num' : i+1
            , 'origin_date' : row['origin_date']
            , 'origin_zipcode' : row['origin_zipcode']
            , 'origin_lng' : -start_x
            , 'origin_lat' : start_y
            , 'dest_zipcode' : row['dest_zipcode']
            , 'dest_lng' : -new_x
            , 'dest_lat' : new_y }, index=[0])
        df = pd.concat([df.loc[:],new_row]).reset_index(drop=True)

    return df


def _get_sqalchemy_engine(project,db=get_secret("EDW_MISC_STAGING_DB",doppler=True)):

   """ For pandas projects where a db connection is required
    we can use the SQAlchemy engine to easily fill a data frame

   :param project: specify the secret group of params
   :type project str

   :param db: specify the database name
   :type db: str

   :return: SQAlchemy engine
   """

   server=get_secret(project+'_SERVER',doppler=True)
   user=get_secret(project+'_USER',doppler=True)
   password=get_secret(project+'_PASSWORD',doppler=True)

   connection_string = URL.create(
      'mssql+pyodbc',
      username=user,
      password=password,
      host=server,
      port=1433,
      database=db,
      query=dict(driver='ODBC Driver 17 for SQL Server'))
   engine = create_engine(connection_string,fast_executemany=True)

   return engine


