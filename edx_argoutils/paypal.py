"""
Utility methods for dealing with PayPal reports.
"""
import csv
import datetime
import fnmatch
import json
import logging

from paramiko import SFTPClient, Transport
from edx_argoutils.s3 import get_s3_path_for_date, list_object_keys_from_s3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
log = logging.getLogger("load_paypal_to_snowflake")


def check_paypal_report(sftp_connection, remote_filename, check_column_name):
    """
    PayPal includes a row count metadata row to validate the report.
    - The type of a row is the first column of the row.
    - The number of rows with type 'SB' (the rows we care about) should equal the
      value of the single row of type 'SF'.
    - If there are 3 rows of type 'SB' then the second column of the 'SF' row should be 3.
    """
    report_file = sftp_connection.open(remote_filename, 'r')
    # First 3 lines are un-needed header
    file_read = report_file.readlines()[3:]
    reader = csv.DictReader(file_read, delimiter=',')

    # "CH" here is the column name of the first column in the report, it's effectively nonsense but consistent.
    #   It stands for "column header".
    # "SB" is the row type of rows we care about. It stands for "section body".
    # "SF" is the row type for summaries. It stands for "section footer".
    sb_count, sf_count = 0, 0
    for row in reader:
        if row['CH'] == 'SB':
            sb_count += 1
        if row['CH'] == 'SF':
            sf_count = int(row[check_column_name])
            break
    if sb_count != sf_count:
        raise Exception('Paypal row counts do not match for {}! Rows found: {}, Rows expected: {}'.format(
            remote_filename,
            sb_count,
            sf_count
        ))


def format_paypal_report(sftp_connection, remote_filename, date):
    """
    Removes unnecessary header / metadata rows that will mess up Snowflake loading.
    """
    report_file = sftp_connection.open(remote_filename, 'r')
    # First 3 lines are un-needed header
    file_read = report_file.readlines()[3:]
    reader = csv.DictReader(file_read, delimiter=',')

    output = []
    for row in reader:
        # "CH" here is the column name of the first column in the report, it's effectively nonsense but consistent.
        #   It stands for "column header".
        # "SB" is the row type of rows we care about. It stands for "section body".
        if row['CH'] == 'SB':
            row['report_date'] = date
            output.append(row)

    return json.dumps(output)


def get_paypal_filename(date, prefix, connection, remote_path):
    """
    Get remote filename. Sample remote filename: DDR-20190822.01.008.CSV
    """
    log.info(connection)
    date_string = date.strftime('%Y%m%d')
    pattern = (prefix, date_string, 'CSV')
    remote_filepattern = "*".join(pattern)
    for file in connection.listdir(remote_path):
        # Ignore any file containing `_TEST` in its name.
        if fnmatch.fnmatch(file, remote_filepattern) and '_TEST' not in file:
            return file
    return None


class RemoteFileNotFoundError(Exception):
    pass


def fetch_paypal_report(
        date: str,
        paypal_credentials: dict,
        paypal_report_prefix: str,
        paypal_report_check_column_name: str,
        s3_bucket: str,
        s3_path: str,
        overwrite: bool,
        host: str = None,
        port: str = None,
        remote_path: str = None,
        aws_credentials: dict = None,
):
    log.info("Pulling Paypal report for {}".format(date))

    if not overwrite:
        # If we're not overwriting and the file already exists, raise a skip
        date_path = get_s3_path_for_date(date)
        s3_key = s3_path + date_path

        log.info("Checking for existence of: {}".format(s3_key))

        existing_file = list_object_keys_from_s3(s3_bucket, s3_key, aws_credentials)

        if existing_file:
            log.info(
                'File {} already exists and we are not overwriting. Skipping.'.format(s3_key)
            )
            return
        else:
            log.info("File not found, continuing download for {}.".format(date))

    transport = Transport(host, port)
    transport.connect(
        username=paypal_credentials.get('username'),
        password=paypal_credentials.get('password')
    )
    sftp_connection = SFTPClient.from_transport(transport)

    query_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    remote_filename = get_paypal_filename(query_date, paypal_report_prefix, sftp_connection, remote_path)

    try:
        if remote_filename:
            sftp_connection.chdir(remote_path)
            check_paypal_report(sftp_connection, remote_filename, paypal_report_check_column_name)
            formatted_report = format_paypal_report(sftp_connection, remote_filename, date)
            return date, formatted_report
        else:
            raise RemoteFileNotFoundError("Remote File Not found for date: {0}".format(date))
    finally:
        sftp_connection.close()
