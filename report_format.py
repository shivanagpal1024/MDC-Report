import pandas as pd
import numpy as np
import configparser as cp
import sys
import time
import datetime
import os
import smtplib
from report.report_logger import get_logger


config_dict = {}
log = get_logger("root")


def check_for_configuration():  # reading the configuration property file
    log.info("Reading configuration file")
    config = cp.ConfigParser()
    config.read("report.properties")

    error_list = []

    for key, value in config['config'].items():
        if value is None or value.strip() == '':
            log.error(f'value for {key} not found')
            error_list.append(f'{key} \"{value}\" not found or incorrect\n')
        else:
            config_dict[key] = value

    if len(error_list) != 0:
        error_string = "\n".join(error_list)
        msg = f"something went wrong while reading configuration={error_list}"
        msg2 = f"something went wrong while reading configuration={error_string}"
        log.error(msg)
        log.error(msg2)
        # mail_sub = 'Report Table Migration Failed'
        # mail_body = 'Hi Team, \n\nThe reporting table migration has failed while reading the configuration files. \n\nPlease Check and run again. \n \n \nThanks & Regards \nMigration job'
        # send_mail(mail_sub, mail_body)
        sys.exit(-1)
    log.info("configuration file read successfully")


def get_file_path():
    log.info("getting the file path...")
    input_directory = config_dict['directory']
    filename = get_filename()
    directory = input_directory
    file_path = os.path.join(directory, filename)
    log.info(f'filepath retrived successfully as {file_path}...!!!')
    return file_path


def get_filename():
    log.info('framing the conventional filename...')
    from dateutil.relativedelta import relativedelta
    today = datetime.date.today()
    two_m_ago = today - relativedelta(months=2)
    mon = two_m_ago.strftime("%b")
    yr = two_m_ago.strftime('%y')
    filename = f'MDC Data {mon} {yr} (With Label Name).CSV'
    log.info(f'filename framed successfully as {filename}....!!!!')
    return filename


def get_CapitationProcessPeriod():
    return time.strftime('%Y%m')


def final_processing():

    check_for_configuration()

    # reading the source csv report
    log.info('process of reading the source MDC file started ..!!!')
    df = pd.read_csv(get_file_path(), usecols=range(0, 34))
    output_directory = config_dict['directory']
    target_filename = config_dict['target_xls']
    target_file_path = os.path.join(output_directory, target_filename)
    writer = pd.ExcelWriter(target_file_path, engine='xlsxwriter')

    # Reading the  list of PHSCompanyNumber
    PHS_list = config_dict['pcn_list']
    log.info("starting the filtering and formatting process of the report as per the list ")
    for _ in PHS_list.split(','):
        PHS = _
        # filtering on the basis of required filter columns
        newdf = df.query('PHSCompanyNumber== @PHS & CapitationProcessPeriod == @get_CapitationProcessPeriod()')
        # if the filtered dataframe is empty , skip that PHS and continue the process
        print(newdf)
        if newdf.empty:
            log.info(f'resultant DataFrame corresponding to PHSCompanyNumber {_} is empty and hence skipping this PHS')
            continue

        # extracting the required columns on the filtered report
        columns = ['ProviderID', 'MemberID', 'NationalDrugCode', 'Label Name', 'ProductLine', 'AmountPaid']
        # grouping the dataframe by ProviderID col
        df1 = pd.DataFrame(newdf, columns=columns).sort_values(['ProviderID'], ascending=True)

        # function to accommodate the CO and SH values from the original dataframe
        try:
            df2 = df1.groupby(['ProviderID']).apply(custom_fun)

        except Exception as e:
            log.error(f'error occurred while applying custom function ----{str(e)}')

        log.info('custom function applied successfully ...!!')

        # introducing Grand total column
        df2['Grand Total'] = df2['CO'].fillna(0) + df2['SH'].fillna(0)
        log.info('Grand Total column introduced successfully...!!!')

        # Rearranging the columns as per the requirement
        df2 = df2[['ProviderID', 'MemberID', 'NationalDrugCode', 'Label Name', 'CO', 'SH', 'Grand Total']]

        # getting the sum of CO,SH and Grand Total Column by applying aggregate
        sum_df = df2.groupby(['ProviderID']).agg({'CO': 'sum', 'SH': 'sum', 'Grand Total': 'sum'})
        log.info('Aggregate function applied successfully ...!!')

        # populating blank values in remaining columns as per the desired format
        a = ['MemberID', 'NationalDrugCode', 'Label Name']
        for i in a:
            sum_df[i] = np.nan
        sum_df = sum_df[['MemberID', 'NationalDrugCode', 'Label Name', 'CO', 'SH', 'Grand Total']]
        sum_df = sum_df.groupby('ProviderID')

        # applying method to insert summation values at the end of each provider
        log.info("applying method to insert summation values at the end of each provider")

        def method(d):
            return d.append(sum_df.get_group(d.name).head(1), ignore_index=True).reset_index(drop=True)

        try:
            df2 = df2.groupby('ProviderID').apply(method)
        except Exception as e:
            log.error(f"something wrong with inserting the total provider value ---{str(e)}")

        # Rearranging the columns as per the requirement
        df2 = df2[['ProviderID', 'MemberID', 'NationalDrugCode', 'Label Name', 'CO', 'SH', 'Grand Total']]
        log.info("Rearranging the columns as per the requirement completed ...!!!!")

        # converting modified df to excel tab  dropping the index and freezing the top row
        log.info("converting modified df to excel tab  dropping the index and freezing the top row")
        tab = f'PHS{_}'
        workbook = writer.book
        worksheet = workbook.add_worksheet(tab)
        writer.sheets[tab] = worksheet
        worksheet.freeze_panes(1, 0)
        df2.to_excel(writer, sheet_name=tab, index=False)

    writer.save()
    log.info("process completed successfully ..!!!")
    sys.exit(0)


def custom_fun(dataframe):
    for i in dataframe.itertuples():
        if i.ProductLine == 'CO':
            dataframe.at[i.Index, 'CO']=i.AmountPaid
            dataframe.at[i.Index, 'SH']= None

        else:
            dataframe.at[i.Index, 'SH']=i.AmountPaid
            dataframe.at[i.Index, 'CO']= None

    dataframe.drop(['ProductLine', 'AmountPaid'], axis=1, inplace=True)
    return dataframe


if __name__ == '__main__':
    final_processing()
