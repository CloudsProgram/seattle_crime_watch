
import requests
import json
import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
from dbt.cli.main import dbtRunner, dbtRunnerResult


from dotenv import load_dotenv

#this loads .env's env variable
load_dotenv(override=True) 

# reads the env variable for google credentials
with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'r') as creds:
    json_creds = json.load(creds)
    gcp_project_name = json_creds['project_id']


@task(log_prints=True)
def convert_to_df(result:list) -> pd.DataFrame:
    """converts the list into pandas dataframe, then specify types for each field"""
    df = pd.DataFrame.from_records(result)
    df = df.astype({
                    'report_number':'str', 
                    'offense_id':'str',
                    'offense_start_datetime':'datetime64[ms]',
                    'offense_end_datetime':'datetime64[ms]',
                    'report_datetime':'datetime64[ms]',
                    'group_a_b':'str',
                    'crime_against_category':'str',
                    'offense_parent_group':'str',
                    'offense':'str',
                    'offense_code':'str',
                    'precinct':'str', 
                    'sector':'str',
                    'beat':'str',
                    'mcpp':'str',
                    '_100_block_address':'str',
                    'longitude':'float64',
                    'latitude':'float64'
                    })
    
    print(f"rows: {len(df)}")
    return df
    






@task(log_prints=True)
def write_gcs(df:pd.DataFrame, path:Path) -> None:
    """This Write saves the dataframe as parquet and then upload to GCS Bucket"""

    # make path posix to avoid any directory error
    path = Path(path).as_posix()

    #prefect gcs bucket block loaded
    gcs_block = GcsBucket.load("seattle-crime-gcs") 

    #upload the df as parquet into GCS Bucket
    gcs_block.upload_from_dataframe(
        df=df,
        to_path=path,
        serialization_format='parquet'
    )

@task(log_prints=True)
def update_txt_record(this_year_month:str) -> None:
    """update text document for future reference"""


    # operation to get the latest updated year_month as string
    last_ingested_year_month_datetime =  datetime.strptime(this_year_month, "%Y-%m") - relativedelta(months=1)
    last_ingested_year_month_str = last_ingested_year_month_datetime.strftime("%Y-%m")

    # Will be update the follinwg values into the txt
    last_updated_month = last_ingested_year_month_str
    initial_data_loaded="True"


    # Open the text file in read mode
    with open('update_notes.txt', 'r') as file:
        # Read the lines from the file
        lines = file.readlines()

    # Update the variable values in the lines list
    for i in range(len(lines)):
        line = lines[i]
        if line.startswith('initial_data_loaded'):
            lines[i] = f'initial_data_loaded={initial_data_loaded}\n'
        elif line.startswith('last_updated_month'):
            lines[i] = f'last_updated_month={last_updated_month}\n'

    # Open the text file in write mode to overwrite the changes
    with open('update_notes.txt', 'w') as file:
        # Write the modified lines back to the file
        file.writelines(lines)
    

@task(log_prints=True)
def extract_historical_months(results_per_page:int, year:int, month:int) -> list:
    """Do API calls to get the data for the iterated month"""
    
    cur_iter_year_month = f"{year}-{month}"

    date_object = datetime.strptime(cur_iter_year_month, '%Y-%m') # convert str into date object
    next_month_date = date_object + relativedelta(months=1) # add a month
    next_iter_year_month= next_month_date.strftime('%Y-%m')  # convert it to str and get its "year-month"
    
    print(f"for Year:{year} Month:{month}")

    # this conducts API call to get the data
    response = requests.get(
    f"https://data.seattle.gov/resource/tazs-3rd5.json?"
    f"&$limit={results_per_page}"
    f"&$where=report_datetime >= '{cur_iter_year_month}' and report_datetime < '{next_iter_year_month}'" #ex: datetime >= 2023-04 and < 2023-05
    f"&$order=report_datetime ASC"  
    )

    return response.json()    

@flow(log_prints=True)
def load_historical_data(this_year:str, this_month:str, results_per_page:int):
    """
    Load all historical records from 2007 ~  most recent month of this year (not including current month)
    """
    # I use 2023 for test purpose, once tests are complete, will use 2007
    years = list(range(2022, int(this_year)+1)) # list from [2007 ~ current year]
    months =  list(range(1, 13)) # month 1 ~ 12, tweaks for testing purposes

    for year in years:
        for month in months:
            #Stops when I reach to today's year-month
            if str(year) == this_year and str(month) == this_month.lstrip('0'):
                break

            # API call to get this iterated year month's data
            result = extract_historical_months(results_per_page, year, month)

            df = convert_to_df(result)

            # for better month naming purpose, instead of "2023-4", I want "2023-04"
            if month < 10:
                month= f"0{month}"

            path = Path(f"seattle_crime_data/seattle_crime_{year}-{month}.parquet")

            # write to GCS Bucket
            write_gcs(df, path)



@task(log_prints=True)
def extract_cur_year_month(results_per_page:int, cur_year_month:datetime) -> list:
    """Do API calls to get the data for the iterated month"""

    cur_year_month = datetime.strptime(cur_year_month, "%Y-%m") #convert str date into datetime

    next_month_date = cur_year_month + relativedelta(months=1) # compute to get next month
    cur_year_month = cur_year_month.strftime('%Y-%m') #convert current iterated dattime into str, ex "2023-04"
    next_year_month= next_month_date.strftime('%Y-%m') #convert current iterated datetime's next month dattime into str, ex "2023-05"

    # this conducts API call to get the data
    response = requests.get(
        f"https://data.seattle.gov/resource/tazs-3rd5.json?"
        #f"$select=report_number,offense_id,report_datetime,crime_against_category,offense_parent_group,offense,offense_code,mcpp"
        f"&$limit={results_per_page}"
        f"&$where=report_datetime >= '{cur_year_month}' and report_datetime < '{next_year_month}'" #ex: datetime >= 2023-04 and < 2023-05
        f"&$order=report_datetime ASC"   
    )

    return response.json()


@task(log_prints=True)
def missing_months_check(last_updated_month: datetime, current_date: datetime) -> list[datetime]:
    """Check the missing months"""

    # Initialize the start month as the month after the last update
    start_month = last_updated_month + relativedelta(months=1)

    # Initialize an empty list to store the months
    months_to_update = []

    # Iterate through each month until the current date
    while start_month < current_date:
        months_to_update.append(start_month.strftime('%Y-%m'))
        start_month += relativedelta(months=1)

    # Print the list of months to update
    print(months_to_update)

    return months_to_update


@flow(log_prints=True)
def load_missing_months(results_per_page:int, this_year_month:str, last_updated_month:str):
    """
    Get missing months, and iterate through them to upload each missing month as Parquet onto GCS bucket
    """

    last_updated_month = datetime.strptime(last_updated_month , "%Y-%m")#convert str date to datetime
    current_date = datetime.strptime(this_year_month, '%Y-%m') #convert str date to datetime

    #check missing months that require upload to gcs and return a list of str that hold "year-month" for each missing month, 
    # Ex: Today is June 2023, and we look at the last_updated_month being 2023-03, this means we are missing  "2023-04" and "2023-05"
    # then months_to_update = ["2023-04","2023-05"]
    months_to_update = missing_months_check(last_updated_month, current_date) 


    for cur_year_month in months_to_update: #iterate through all missing months and upload each month's data to GCS bucket
        result = extract_cur_year_month(results_per_page, cur_year_month) # conduct API call to retrive the month's data
        df = convert_to_df(result)

        path= Path(f"seattle_crime_data/seattle_crime_{cur_year_month}.parquet")
        write_gcs(df, path) #upload data to GCS bucket


@flow(log_prints=True)
def process_data(initial_data_loaded, last_updated_month, this_year_month, this_year, this_month):
    """
    Based on initial_data_loaded, we either load all historical data, or just missing months
    And then we update the txt file to keep track of latest ingested month.
    """
    
    # results_per_page allows us to retrive upper limit row amount per API call.
    # Since each request only returns rows for that month, and knowing that monthly 
    # crime report won't likely surpass 10,000; I will use 50,000 rows return per request for enough
    # coverage
    results_per_page = 50000


    # Loads all missing months' data
    if initial_data_loaded==True:
        load_missing_months(results_per_page, this_year_month, last_updated_month)
        update_txt_record(this_year_month)


    # Loads historical data till/include last month     
    elif initial_data_loaded==False:
        load_historical_data(this_year, this_month, results_per_page)
        update_txt_record(this_year_month)


@flow(log_prints=True)
def stage_bq():
    """Stage data in BigQuery from GCS"""

    #Create External table
    bq_ext_tbl = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{gcp_project_name}.seattle_crime_staging.external_seattle_crime_data`
            OPTIONS (
                format = 'PARQUET',
                uris = ['gs://seattle_crime_data_lake_{gcp_project_name}/seattle_crime_data/seattle_crime_*.parquet']
            )
        """
    
    # seattle-crime-bq came from the block that I created via prefect
    with BigQueryWarehouse.load("seattle-crime-bq") as warehouse:
        operation = bq_ext_tbl
        warehouse.execute(operation)
    

    #Create partitioned+Clustered table
    bq_partitioned_clustered_tbl = f"""
            CREATE OR REPLACE TABLE `{gcp_project_name}.seattle_crime_staging.seattle_crime_data_partitioned_clustered`
            PARTITION BY DATE_TRUNC(report_datetime,Month)
            CLUSTER BY  mcpp, offense_parent_group AS
            SELECT * FROM `{gcp_project_name}.seattle_crime_staging.external_seattle_crime_data`;
        """

    with BigQueryWarehouse.load("seattle-crime-bq") as warehouse:
        operation =  bq_partitioned_clustered_tbl
        warehouse.execute(operation)
    






@task(log_prints=True)
def date_time_manipulation() -> tuple[str,str,str]:
    """
    This func helps use get today's date in 3 parts in str:
    "year-month"
    "year" 
    "month"
    """
    #section 1:
    today = date.today() 
    extract_today = datetime.strptime(str(today), '%Y-%m-%d')
    this_year_month = extract_today.strftime('%Y-%m') #ex: "2023-05"


    #section 2:
    this_year = extract_today.strftime('%Y') #ex: "2023"
    this_month = extract_today.strftime('%m') #ex: "05"

    return this_year_month, this_year, this_month

@flow(log_prints=True)
def last_ingestion_check() -> tuple[bool, str]:
    """
    # Read txt file to retrieve
    # initial_data_loaded: See if there were any data uploaded previously.
    # last_updated_month: See when was the last updated month.

    # those 2 variables will eventually be used to determine various future operations
    """

    filename = "update_notes.txt"  

    # Initialize variables
    initial_data_loaded = False
    last_updated_month = None

    # Read the contents of the file
    with open(filename, "r") as file:
        lines = file.readlines()

    # Process each line and extract variable values
    for line in lines:
        if "initial_data_loaded" in line:
            value = line.split("=")[1].strip()
            initial_data_loaded = value
            if initial_data_loaded=="False": #Do this b/c python can't change "False" str into a falsy bool.
                initial_data_loaded = False
            else:
                initial_data_loaded= True

        elif "last_updated_month" in line:
            value = line.split("=")[1].strip()
            last_updated_month = value

    return initial_data_loaded, last_updated_month

@flow(name="dbt modelling", log_prints=True)
def dbt_model():
    """This allows me to run DBT(data build tool) which will conduct transformation and establish the fact table """
    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    # my intended cli command is:
    # dbt run --project-dir ./dbt/seattle_crime --profiles-dir ./dbt/seattle_crime
    cli_args = ["run","--project-dir","./dbt/seattle_crime", "--profiles-dir", "./dbt/seattle_crime"]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)
 

    # # inspect the results
    # for r in res.result:
    #     print(f"{r.node.name}: {r.status}")

@flow(name="Process-Data-Parent", log_prints=True)
def parent_process_data():
    # """
    # This func checks previous updates by reading update_notes.txt (tracks the most recent update record), to see if 
    # further operations are needed.
    # If updates are needed, then it will run process_data() to extract data and upload it to GCP bucket
    # and then load it into GCP BigQuery with stage_bq().
    # lastly, dbt_model() will 
    # do transformation on the data and establish new staging table and fact table in BigQuery
    # """

    # Read txt file to retrieve: 
    # Var1: Was thre an initial data upload? Var2: If there is, what was the last updated month, if there isn't then return None
    initial_data_loaded, last_updated_month = last_ingestion_check()


    #if today is May 2023, will return "2023-05", "2023", "05"
    this_year_month, this_year, this_month = date_time_manipulation()

    # Check to see if everything is already up to date, if yes: end all operations.
    # If there is was an initial upload according to the txt, then check to see if the latest update from txt record
    # is in fact the latest (latest = last month of today, ex: today is May 2023, then the latest update is Apirl 2023)
    # if it is, terminate subsequent operations. B/c things are all updated already, no need to continue
    # if not, continute operations.
    if initial_data_loaded==True: 
        # Operation to get previous "year-month", ex: today is "2023-5", this will give "2023-04"
        previous_year_month_datetime =  datetime.strptime(this_year_month, "%Y-%m") - relativedelta(months=1)
        previous_year_month_str = previous_year_month_datetime.strftime("%Y-%m")

        if previous_year_month_str == last_updated_month: #if things are already updated
            print("Data already updated previously, ending all operations. Good Bye!")
            return #end operations
        


    #Conducts extraction of monthly data, get them in parquet form and upload into GCP bucket.
    #Then update update_notes.txt to indicate we did the operation.
    process_data(initial_data_loaded, last_updated_month, this_year_month, this_year, this_month )

    #This loads data from GCS bucket into BigQuery Data Warehouse
    stage_bq()

    #This conducts tranformation and establishes fact tables in BigQuery
    dbt_model()
    
    

if __name__ == "__main__":
    parent_process_data()
    
    