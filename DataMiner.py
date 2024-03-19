"""

                           CISCO SAMPLE CODE LICENSE
                                  Version 1.1
                Copyright (c) 2018 Cisco and/or its affiliates

   These terms govern this Cisco Systems, Inc. ('Cisco'), example or demo
   source code and its associated documentation (together, the 'Sample
   Code'). By downloading, copying, modifying, compiling, or redistributing
   the Sample Code, you accept and agree to be bound by the following terms
   and conditions (the 'License'). If you are accepting the License on
   behalf of an entity, you represent that you have the authority to do so
   (either you or the entity, 'you'). Sample Code is not supported by Cisco
   TAC and is not tested for quality or performance. This is your only
   license to the Sample Code and all rights not expressly granted are
   reserved.

   1. LICENSE GRANT: Subject to the terms and conditions of this License,
      Cisco hereby grants to you a perpetual, worldwide, non-exclusive, non-
      transferable, non-sublicensable, royalty-free license to copy and
      modify the Sample Code in source code form, and compile and
      redistribute the Sample Code in binary/object code or other executable
      forms, in whole or in part, solely for use with Cisco products and
      services. For interpreted languages like Java and Python, the
      executable form of the software may include source code and
      compilation is not required.

   2. CONDITIONS: You shall not use the Sample Code independent of, or to
      replicate or compete with, a Cisco product or service. Cisco products
      and services are licensed under their own separate terms and you shall
      not use the Sample Code in any way that violates or is inconsistent
      with those terms (for more information, please visit:
      www.cisco.com/go/terms).

   3. OWNERSHIP: Cisco retains sole and exclusive ownership of the Sample
      Code, including all intellectual property rights therein, except with
      respect to any third-party material that may be used in or by the
      Sample Code. Any such third-party material is licensed under its own
      separate terms (such as an open source license) and all use must be in
      full accordance with the applicable license. This License does not
      grant you permission to use any trade names, trademarks, service
      marks, or product names of Cisco. If you provide any feedback to Cisco
      regarding the Sample Code, you agree that Cisco, its partners, and its
      customers shall be free to use and incorporate such feedback into the
      Sample Code, and Cisco products and services, for any purpose, and
      without restriction, payment, or additional consideration of any kind.
      If you initiate or participate in any litigation against Cisco, its
      partners, or its customers (including cross-claims and counter-claims)
      alleging that the Sample Code and/or its use infringe any patent,
      copyright, or other intellectual property right, then all rights
      granted to you under this License shall terminate immediately without
      notice.

   4. LIMITATION OF LIABILITY: CISCO SHALL HAVE NO LIABILITY IN CONNECTION
      WITH OR RELATING TO THIS LICENSE OR USE OF THE SAMPLE CODE, FOR
      DAMAGES OF ANY KIND, INCLUDING BUT NOT LIMITED TO DIRECT, INCIDENTAL,
      AND CONSEQUENTIAL DAMAGES, OR FOR ANY LOSS OF USE, DATA, INFORMATION,
      PROFITS, BUSINESS, OR GOODWILL, HOWEVER CAUSED, EVEN IF ADVISED OF THE
      POSSIBILITY OF SUCH DAMAGES.

   5. DISCLAIMER OF WARRANTY: SAMPLE CODE IS INTENDED FOR EXAMPLE PURPOSES
      ONLY AND IS PROVIDED BY CISCO 'AS IS' WITH ALL FAULTS AND WITHOUT
      WARRANTY OR SUPPORT OF ANY KIND. TO THE MAXIMUM EXTENT PERMITTED BY
      LAW, ALL EXPRESS AND IMPLIED CONDITIONS, REPRESENTATIONS, AND
      WARRANTIES INCLUDING, WITHOUT LIMITATION, ANY IMPLIED WARRANTY OR
      CONDITION OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NON-
      INFRINGEMENT, SATISFACTORY QUALITY, NON-INTERFERENCE, AND ACCURACY,
      ARE HEREBY EXCLUDED AND EXPRESSLY DISCLAIMED BY CISCO. CISCO DOES NOT
      WARRANT THAT THE SAMPLE CODE IS SUITABLE FOR PRODUCTION OR COMMERCIAL
      USE, WILL OPERATE PROPERLY, IS ACCURATE OR COMPLETE, OR IS WITHOUT
      ERROR OR DEFECT.

   6. GENERAL: This License shall be governed by and interpreted in
      accordance with the laws of the State of California, excluding its
      conflict of laws provisions. You agree to comply with all applicable
      United States export laws, rules, and regulations. If any provision of
      this License is judged illegal, invalid, or otherwise unenforceable,
      that provision shall be severed and the rest of the License shall
      remain in full force and effect. No failure by Cisco to enforce any of
      its rights related to the Sample Code or to a breach of this License
      in a particular situation will act as a waiver of such rights. In the
      event of any inconsistencies with any other terms, this License shall
      take precedence.


Author: Steve Hartman
co=Author: Nick DiNofio
AWS S3 function: Oscar Bustos
Contact: stehartm@cisco.com
Contact: ndinofi@cisco.com
Contact: obustosm@cisco.com
Description:
    This is a sample python script that will extract all the PX Cloud data
for all customers for a single partner. It will gather the JSON data and convert
it to CSV for injecting into any platform that can digest a CSV file.

Reference SDK: https://developer.cisco.com/docs/px-cloud/#!partner-and-customer-data-api

Version Features and enhancements
1. Added progress indicators to the function call to API
2. Removed some counter variables to follow best practice of looping
3. Refactored all variables to follow camelCase naming conventions
4. Moved sensitive config data to a config.ini file
    If no file is found, a template is created automatically in the same folder as the script.
    If no AWS S3 credentials are found within the config.ini file, then S3 uploads are by-passed
5. This version covers the follow API end points: ( * denote code completed for this report)
    A. Customers *
    B. Contracts *
    C. Contract Details *
    E. Partner Offers *
    F. Partner Offer Sessions *
    G. Customer Lifecycle *
    H. Success Tracks *
    I. Customer Data Reports which includes: ( * denotes code is completed for this report in this version)
        A. Assets *
        B. Hardware *
        C. Software *
        D. Security Advisories *
        E. Field Notices *
        F. Priority Bugs *
        G. Purchased Licenses *
        H. Licenses with Assets *
    J. Optimal Software Version
        A. Software Groups *
        B. Software Group suggestions *
        C. Software Group suggestions-Assets *
        D. Software Group suggestions-Bug List *
        E. Software Group suggestions-Field Notices *
        F. Software Group suggestions-Advisories *
    K. Automated Fault Management
        A. Faults *
        B. Fault Summary *
        C. Affected Assets *
    L. Regulatory Compliance Check
        A. Compliance Violations *
        B. Assets violating compliance rule *
        C. Policy Rule Details *
        D. Compliance Suggestions *
        E. Assets with violations *
        F. Asset Violations *
        G. Obtained *
    M. Risk Mitigation Check
        A. Crash Risk Assets *
        B. Crash Risk Factors *
        C. Similar Assets *
        D. Assets Crashed in last 1d, 7d, 15d, 90d *
        E. Asset Crash History *
6. Added the ability to upload to AWS S3 storage
7. Added a flag for 3 levels of debug output
    0 = exceptions only (default)
    1 = exceptions and status messages only
    3 = exceptions, status messages and API response payload
8. Added the ability to export data in raw JSON files as well as CSV's
9. Moved the following parameters to a new section labled 'settings' in the config.ini
    A. wait_time = 1  # Used for setting a global wait timer (minimum requirement is 1)
    B. pxc_fault_days = '30'  # Used for Making API Request for number of days in report (default is 30)
    C. max_items = '50'  # Used for the maximum number of items to return in report (max and default is 50)
    D. debug_level = 0  # Used for setting a debug level (0,1,2) default is 0
    E. apiTimeout = 10  # Timeout value for API calls in seconds default is 10
    F. testLoop = 1  # test the code by running through the entire sequence x times... (default is 1)
    G. outputFormat = 1  # generate output in the form of Both, JSON or CSV. 1=Both 2=JSON 3=CSV
10. Changed the CSV File names to be consistent between all the CSV's
11. Added decriptions of the file naming conventions used for the CSV's and JSON files
12. Fixed bug with Lifecycle func not producing JSON output

"""
# adding Cisco DataMiner Folder to system path
import sys

import concurrent.futures
from requests.exceptions import Timeout
import random
import requests
import json
import csv
import os
import shutil
import zipfile
import time
import math
import base64
from configparser import ConfigParser
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
from datetime import datetime
import logging
import argparse
from configparser import ConfigParser

'''
Notes on Imports
concurrent.futures for making Multithreaded calls
Timeout for capturing exception if timeout value is exceeded.
random for generating random numbers
requests for making API calls
json for parsing json data
csv for parsing csv data
os for file/folder management
shutil for removing entire folders
zipfile for testing/extracting zip files
time for setting a sleep/wait timer
math for rounding up to whole numbers
base64 for converting to and from base64 encryption
configparser to read and write configuration files
boto3 for accessing the AWS S3 storage
datetime for posting time stamps in the logging
logging for writing the log files via Python logger
'''

# PXCloud settings
# =======================================================================
# Generic URL Variables
pxc_token_url = 'https://id.cisco.com/oauth2/aus1o4emxorc3wkEe5d7/v1/token'
urlProtocol = 'https://'
pxc_url_base = ''
urlHost = 'api-cx.cisco.com/'
urlLink = 'px/v1/'
urlLinkSandbox = 'sandbox/px/v1/'
grant_type = 'client_credentials'
pxc_cache_control = 'no-cache'
environment = ''

# Customer and Analytics Insights URL Variables
pxc_url_lifecycle = '/lifecycle'
pxc_url_software_groups = '/insights/software/softwareGroups'
pxc_url_software_group_suggestions = pxc_url_software_groups + '/suggestions'
pxc_url_software_group_suggestions_assets = pxc_url_software_groups + '/assets'
pxc_url_software_group_suggestions_bugs = pxc_url_software_group_suggestions + '/bugs'
pxc_url_software_group_suggestions_field_notices = pxc_url_software_group_suggestions + '/fieldNotices'
pxc_url_software_group_suggestions_security_advisories = pxc_url_software_group_suggestions + '/securityAdvisories'
pxc_url_automated_fault_management_faults = '/insights/faults'
pxc_url_automated_fault_management_fault_summary = '/summary'
pxc_url_automated_fault_management_affected_assets = '/affectedAssets'
pxc_url_compliance_violations = '/insights/compliance/violations'
pxc_url_compliance_violations_assets = '/insights/compliance/violations/assets'
pxc_url_compliance_policy_rule_details = '/insights/compliance/policyRuleDetails'
pxc_url_compliance_suggestions = '/insights/compliance/suggestions'
pxc_url_compliance_assets_with_violations = '/insights/compliance/assetsWithViolations'
pxc_url_compliance_asset_violations = '/insights/compliance/assetViolations'
pxc_url_compliance_optin = '/insights/compliance/optIn'
pxc_url_crash_risk = '/insights/crashRisk/asset'
pxc_url_crash_risk_assets = pxc_url_crash_risk + 's'
pxc_url_crash_risk_factors = '/riskFactors'
pxc_url_crash_risk_similar_assets = '/similarAssets'
pxc_url_crash_risk_assets_last_crashed = pxc_url_crash_risk + 'sCrashed'
pxc_url_crash_risk_asset_crash_history = '/crashHistory'

# Data File Variables
codeVersion = str("2.0.0-d")
configFile = "config.ini"

csv_output_dir = "outputcsv/"
json_output_dir = "outputjson/"
log_output_dir = "outputlog/"
temp_dir = "temp/"

# Variables
allCustomers = (csv_output_dir + 'All_Customers.csv')
customers = (csv_output_dir + 'Customers.csv')
contracts = (csv_output_dir + 'Contracts.csv')
contractsWithCustomers = (csv_output_dir + 'Contracts_With_Customer_Names.csv')
L2_contracts = (csv_output_dir + 'L2_contracts.csv')
OSV_AFM_List = (csv_output_dir + 'OSV_AFM_List.csv')
contractDetails = (csv_output_dir + 'Contract_Details.csv')
partnerOffers = (csv_output_dir + 'Partner_Offers.csv')
partnerOfferSessions = (csv_output_dir + 'Partner_Offer_Sessions.csv')
assets = (csv_output_dir + 'Assets.csv')
hardware = (csv_output_dir + 'Hardware.csv')
software = (csv_output_dir + 'Software.csv')
purchasedLicenses = (csv_output_dir + 'Purchased_Licenses.csv')
licenses = (csv_output_dir + 'Licenses_with_assets.csv')
lifecycle = (csv_output_dir + 'Lifecycle.csv')
securityAdvisories = (csv_output_dir + 'Security_Advisories.csv')
fieldNotices = (csv_output_dir + 'Field_Notices.csv')
priorityBugs = (csv_output_dir + 'Priority_Bugs.csv')
successTracks = (csv_output_dir + 'Success_Track.csv')
SWGroups = (csv_output_dir + 'Software_Groups.csv')
SWGroupSuggestionsTrend = (csv_output_dir + 'Software_Group_Suggestions_Trend.csv')
SWGroupSuggestionSummaries = (csv_output_dir + 'Software_Group_Suggestions_Summaries.csv')
SWGroupSuggestionsReleases = (csv_output_dir + 'Software_Group_Suggestions_Releases.csv')
SWGroupSuggestionAssets = (csv_output_dir + 'Software_Group_Suggestion_Assets.csv')
SWGroupSuggestionsBugList = (csv_output_dir + 'Software_Group_Suggestion_Bug_List.csv')
SWGroupSuggestionsFieldNotices = (csv_output_dir + 'Software_Group_Suggestion_Field_Notices.csv')
SWGroupSuggestionsAdvisories = (csv_output_dir + 'Software_Group_Suggestion_Security_Advisories.csv')
AFMFaults = (csv_output_dir + 'Automated_Fault_Management_Faults.csv')
AFMFaultSummary = (csv_output_dir + 'Automated_Fault_Management_Fault_Summary.csv')
AFMFaultAffectedAssets = (csv_output_dir + 'Automated_Fault_Management_Fault_Affected_Assets.csv')
AFMFaultHistory = (csv_output_dir + 'Automated_Fault_Management_Fault_History.csv')
RCCComplianceViolations = (csv_output_dir + 'Regulatory_Compliance_Violations.csv')
RCCAssetsViolatingComplianceRule = (csv_output_dir + 'Regulatory_Compliance_Assets_violating_Compliance_Rule.csv')
RCCPolicyRuleDetails = (csv_output_dir + 'Regulatory_Compliance_Policy_Rule_Details.csv')
RCCComplianceSuggestions = (csv_output_dir + 'Regulatory_Compliance_Suggestions.csv')
RCCAssetsWithViolations = (csv_output_dir + 'Regulatory_Compliance_Assets_With_Violations.csv')
RCCAssetViolations = (csv_output_dir + 'Regulatory_Compliance_Asset_Violations.csv')
RCCObtained = (csv_output_dir + 'Regulatory_Compliance_Obtained.csv')
CrashRiskAssets = (csv_output_dir + 'Crash_Risk_Assets.csv')
CrashRiskFactors = (csv_output_dir + 'Crash_Risk_Factors.csv')
CrashRiskSimilarAssets = (csv_output_dir + 'Crash_Risk_Similar_Assets.csv')
CrashRiskAssetsLastCrashed = (csv_output_dir + 'Crash_Risk_Assets_Last_Crashed.csv')
CrashRiskAssetCrashHistory = (csv_output_dir + 'Crash_Risk_Asset_Crash_History.csv')
features = ['features', 'fingerprint', 'softwares_features']
timePeriods = [1, 7, 15, 90]

# Configuration File Variables
pxc_client_id = ''  # Used to store the PX Cloud Client ID
pxc_client_secret = ''  # Used to store the PX Cloud Client Secret
s3access_key = ''  # Used to store the Amazon Web Services S3 (Simple Storage Service) Key
s3access_secret = ''  # Used to store the Amazon Web Services S3 (Simple Storage Service) Secret
s3bucket_folder = ''  # Used to store the Amazon Web Services S3 (Simple Storage Service) Bucket Folder
s3bucket_name = ''  # Used to store the Amazon Web Services S3 (Simple Storage Service) Bucket Name

# Data Initializer Variables
data_payload = {}  # Used for Making API Request
token = ''  # Used for Making API Requests
tokenStartTime = 0  # time start counter

# the below of function completed flags
gTRecords = 0
customersFlag = 0
contractswithcustomernamesFlag = 0
contractsFlag = 0
software_groupsFlag = 0
compliance_violationsFlag = 0
assets_with_violationsFlag = 0
crash_risk_assetsFlag = 0
afm_faultsFlag = 0
software_group_suggestionsFlag = 0

# the following settings are set in the config.ini 'settings' section. Below are default placeholders for those values.
wait_time = 1  # Used for setting a global wait timer (minimum requirement is 1)
pxc_fault_days = '30'  # Used for Making API Request for number of days in report (default is 30)
max_items = '50'  # Used for the maximum number of items to return in report (max and default is 50)
debug_level = 0  # Used for setting a debug level (0 = Low,1 = Medium,2 = High) default is 0
log_to_file = 1  # Enable looging output
apiTimeout = 10  # Timeout value for API calls in seconds default is 10
testLoop = 1  # test the code by running through the entire sequence x times. Default is 1
outputFormat = 1  # generate output in the form of Both, JSON or CSV. 1=Both 2=JSON 3=CSV
useProductionURL = 1  # if true, use production URL, if false, use sandbox URL (1=True 2=False) Default is 1
STList = []  # List of success tracks supported (38396885, 50949451, 50320048, 40485321, 40317380)
insightList = []  # list of Success Track ID which support Insights (38396885, 40485321)
'''
Begin defining functions
=======================================================================
'''
def init_logger(log_level):
    # Check if the specified log level is valid
    #   CRITICAL: Indicates a very serious error, typically leading to program termination.
    #   ERROR:    Indicates an error that caused the program to fail to perform a specific function.
    #   WARNING:  Indicates a warning that something unexpected happened
    #   INFO:     Provides confirmation that things are working as expected
    #   DEBUG:    Provides info useful for diagnosing problems
    if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        print("Invalid log level. Please use one of: DEBUG, INFO, WARNING, ERROR, CRITICAL")
        sys.exit(1)

    if log_to_file == 1:
        print('Logging is enabled\n')
        # Setup log storage - incase needed
        if os.path.isdir(log_output_dir):
            shutil.rmtree(log_output_dir)
            os.mkdir(log_output_dir)
        else:
            os.mkdir(log_output_dir)

        # Set up logging based on the parsed log level
        logging.basicConfig(filename=log_output_dir + 'PCX.log', level=log_level,
                            format='%(levelname)s:%(funcName)s: %(message)s')
    else:
        print('Logging is disabled\n')


# Function explain usage
def usage():
    print(f"Usage: python3 {sys.argv[0]} <customer> -log=<LOG_LEVEL>")
    print(f"Args:")
    print(f"   Optional named section for customer auth credentials.\n")
    sys.exit()

def token_time_check():
    # Check Token lifespan and if older than 110 mins, generate a new one.
    checkTime = time.time()
    tokenTime = math.ceil(int(checkTime - tokenStartTime) / 60)
    if debug_level > 0:
        logging.debug(f'Token time is :{tokenTime} minutes')
    if tokenTime > 110:
        get_pxc_token()

def location_ready_status(location, headers, report):
    # query the API for a ready status to collect the report data
    while True:
        logging.debug(f'Checking {report} Report ready status for {location}')
        token_time_check()
        try:
            response = requests.get(location, headers=headers, allow_redirects=False, timeout=apiTimeout)
            response.raise_for_status()
            logging.debug(f'HTTP Code:{response.status_code}')
            if response.status_code == 303:
                logging.debug(f'Downloading {report} Report at {location}')
                break
            pause = int(response.json().get('suggestedNextPollTimeInMins', 1) + (random.random() * 3))
            logging.debug(f'API Requested {pause} seconds for {report} Report to complete...')
            time.sleep(pause)
        except Timeout:
            logging.debug(f'Time out error getting token')
    return location


def proccess_sandbox_files(filename, customerid, reportid, json_filename):
    # The function is used to collect the file when accessing the sandbox
    with zipfile.ZipFile(filename, mode='r') as archive:
        for jsonfile in archive.namelist():
            logging.debug('Correcting Sandbox file name')
            os.rename((temp_dir + jsonfile), (temp_dir + customerid + reportid + json_filename))


def get_dir_size(path='.'):
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total


# Function to load configuration from config.ini and continue or create a template if not found and exit
def load_config(customer):
    global pxc_client_id
    global pxc_client_secret
    global s3access_key
    global s3access_secret
    global s3bucket_folder
    global s3bucket_name
    global wait_time
    global pxc_fault_days
    global max_items
    global debug_level
    global log_to_file
    global apiTimeout
    global testLoop
    global outputFormat
    global useProductionURL
    global STList
    global insightList

    config = ConfigParser()
    if os.path.isfile(configFile):
        print(f'********************** CISCO PX Cloud Data Miner *********************')
        print(f'********************* Minning Data for {customer} ********************')
        print(f"**********************************************************************\n")
        print(f"Config.ini file was found, continuing...")
        config.read(configFile)

        # customer name cant be 'settings'
        if customer == 'settings':
            print(f"\nError: Customer name name can not be the resertved string 'settings'")
            usage()

        # check to see if credentials exist for a named customer.
        # default customer config is 'credentials'
        if not customer in config:
            print(f"\nError: Credentials for Customer {customer} not found in config.ini")
            usage()

        # [credentials]
        pxc_client_id = (config[customer]['pxc_client_id'])
        pxc_client_secret = (config[customer]['pxc_client_secret'])
        s3access_key = (config[customer]['s3access_key'])
        s3access_secret = (config[customer]['s3access_secret'])
        s3bucket_folder = (config[customer]['s3bucket_folder'])
        s3bucket_name = (config[customer]['s3bucket_name'])

        # [settings]
        wait_time = int((config['settings']['wait_time']))
        pxc_fault_days = str((config['settings']['pxc_fault_days']))
        max_items = (config['settings']['max_items'])
        debug_level = int((config['settings']['debug_level']))
        log_to_file = int((config['settings']['log_to_file']))
        apiTimeout = int(config['settings']['apiTimeout'])
        testLoop = int((config['settings']['testLoop']))
        outputFormat = int((config['settings']['outputFormat']))
        useProductionURL = int(config['settings']['useProductionURL'])
        STList = (config['settings']['STList'])
        insightList = (config['settings']['insightList'])

    else:
        print('Config.ini not found!!!!!!!!!!!!Creating config.ini...')
        print('NOTE: you must edit the config.ini file with your information... Exiting...')
        config.add_section('credentials')
        config.set('credentials', 'pxc_client_id', '')
        config.set('credentials', '# PX Cloud API Client ID  Default is blank', '')
        config.set('credentials', 'pxc_client_secret', '')
        config.set('credentials', '# PX Cloud API Client Secret  Default is blank', '')
        config.set('credentials', 's3access_key', '')
        config.set('credentials', '# AWS S3 Access Key Default is blank', '')
        config.set('credentials', 's3access_secret', '')
        config.set('credentials', '# AWS S3 Client Secret  Default is blank', '')
        config.set('credentials', 's3bucket_folder', '')
        config.set('credentials', '# AWS S3 Bucket Folder  Default is blank', '')
        config.set('credentials', 's3bucket_name', '')
        config.set('credentials', '# AWS S3 Bucket Name  Default is blank', '')
        config.add_section('settings')
        config.set('settings', '# Time to wait between errors in seconds, default', '1')
        config.set('settings', 'wait_time', '1')
        config.set('settings', '# The number of days to retrieve fault data for. This value can be 1, 7, 15, 30. '
                               'default', '30')
        config.set('settings', 'pxc_fault_days', '30')
        config.set('settings', '# The maximum number of items to return per API call maximum is 50, default ', '50')
        config.set('settings', 'max_items', '50')
        config.set('settings', '# Used for setting a debug level (0 = Low, 1 = Medium, 2 = High), default', '0')
        config.set('settings', 'debug_level', '0')
        config.set('settings', '# Used for enabling logging, default', '1')
        config.set('settings', 'log_to_file', '1')
        config.set('settings', '# Timeout value for API calls in seconds default', '10')
        config.set('settings', 'apiTimeout', '10')
        config.set('settings', '# test the code by running through the entire sequence x times..., default', '1')
        config.set('settings', 'testLoop', '1')
        config.set('settings', '# generate output in the form of Both, JSON or CSV. 1=Both 2=JSON 3=CSV, default', '1')
        config.set('settings', 'outputFormat', '1')
        config.set('settings', '# use production url (1=true 0=false) if false, use sandbox url, default', '1')
        config.set('settings', 'useProductionURL', '1')
        config.set('settings', '# List of success track IDs currently supported, default',
                   '38396885, 50949451, 50320048, 40485321, 40317380')
        config.set('settings', 'STList', '38396885, 50949451, 50320048, 40485321, 40317380')
        config.set('settings', '# List of success track IDs currently supported for insights, default',
                   '38396885, 40485321')
        config.set('settings', 'insightList', '38396885, 40485321')
        with open(configFile, 'w') as configfile:
            config.write(configfile)
        exit()


def s3_storage():
    # Function to upload all files in the output folders to AWS S3.
    if s3access_key:
        logging.debug('Uploading data to S3')
        # Connect to S3 Service
        client_s3 = boto3.client('s3', aws_access_key_id=s3access_key, aws_secret_access_key=s3access_secret, )
        # Upload Files to S3 Bucket
        data_file_folder = os.path.join(os.getcwd(), csv_output_dir)
        for file in os.listdir(data_file_folder):
            if not file.startswith('~'):
                try:
                    logging.debug('Uploading file {0}...'.format(file))
                    client_s3.upload_file(os.path.join(data_file_folder, file),
                                          s3bucket_name,
                                          (s3bucket_folder + '{}').format(file))
                    logging.debug('Upload Successful')
                except ClientError as e:
                    logging.debug('Credential is incorrect')
                    logging.debug(e)
                except Exception as e:
                    logging.debug(e)
                except FileNotFoundError:
                    logging.debug('The file was not found')
                except NoCredentialsError:
                    logging.debug('Credentials not available')
    else:
        logging.debug('Uploading data to S3 was SKIPPED')


def temp_storage():
    # Function to generate or clear output and temp folders for use.
    if os.path.isdir(csv_output_dir):
        shutil.rmtree(csv_output_dir)
        os.mkdir(csv_output_dir)
    else:
        os.mkdir(csv_output_dir)
    if os.path.isdir(json_output_dir):
        shutil.rmtree(json_output_dir)
        if outputFormat == 1 or outputFormat == 2:
            os.mkdir(json_output_dir)
    else:
        if outputFormat == 1 or outputFormat == 2:
            os.mkdir(json_output_dir)
    if os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir)
        os.mkdir(temp_dir)
    else:
        os.mkdir(temp_dir)


def get_pxc_token():
    global token
    global tokenStartTime
    print("Getting PX Cloud Token")
    url = (
            pxc_token_url + "?grant_type=" +
            grant_type + "&client_id=" +
            pxc_client_id + "&client_secret=" +
            pxc_client_secret + "&cache-control=" +
            pxc_cache_control + "&scope=" +
            pxc_scope
    )
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    # Make a request to API
    for i in range(10):
        i += 1
        logging.debug(f'attempt number {i} for Token Request')
        try:
            response = requests.request("POST", url, headers=headers, timeout=apiTimeout)
            reply = json.loads(response.text)
            if response.status_code == 200:
                try:
                    token = (reply["access_token"])
                    if i > 1:
                        logging.debug(f'Token retry # {i} was successful')
                    else:
                        logging.debug('Token request was successful')
                        break
                except KeyError:
                    token = ""
                    pass
            else:
                logging.debug('Token request failed')
                break
        except Timeout:
            logging.debug(f'Time out error getting token')
    tokenStartTime = time.time()
    if len(token) > 0:
        logging.debug("Retrieved a valid token")
    else:
        print("Unable to retrieve a valid token\n"
              "Check config.ini and ensure your API Keys and if your using the Sandbox or Production for accuracy")
        print(f"Client ID: {pxc_client_id}")
        print(f"Client Secret: {pxc_client_secret}")
        print(f"Production APIs? : {useProductionURL}")
        exit()


def get_json_reply(url, tag, report):
    # Function to get the raw API JSON data from PX Cloud
    tries = 1
    response = []
    while True:
        try:
            token_time_check()
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            max_reply_attempts = 10
            reply_attempts = 0
            logging.debug(f'Querying data for {report}')
            while reply_attempts < max_reply_attempts:
                # Make a request to API
                for i in range(10):
                    i += 1
                    logging.debug(f'Request attempt number {i}')
                    try:
                        response = requests.request('GET', url, headers=headers, data=data_payload,
                                                    verify=True, timeout=apiTimeout)
                        if i > 1:
                            logging.debug(f'API request retry # {i} was successful')
                        break
                    except Timeout:
                        if i == 10:
                            logging.debug('Error: too many timeouts... exiting')
                            exit()
                        logging.debug(f'Time out error getting resonse')
                # If not rate limited, break out of while loop and continue
                if response.status_code != 429:
                    logging.debug(f'Querying:')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'Request Body: {data_payload}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    if reply_attempts == 0:
                        logging.debug('Success')
                    break
                # If rate limited, wait and try again
                logging.debug(f'Rate Limit Exceeded for {report}! Retrying...')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'URLTag:{tag}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
                reply_attempts += 1
                pause = random.random()
                logging.debug(f'sleeping for {pause} seconds')
                time.sleep(pause)
            if reply_attempts > 0:
                logging.debug(f'Retry was successful for {report}')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'URLTag:{tag}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
            reply = json.loads(response.text)
            items = reply[tag]
            logging.debug(f'API reply is : {reply}')
            try:
                if response.status_code == 200:
                    if len(items) > 0:
                        if tries >= 2:
                            logging.debug('Retry Successful! Continuing.')
                        return items
            finally:
                if response.status_code == 200:
                    return items
        except Exception as Error:
            if response.text.__contains__('Customer admin has not provided access.'):
                if debug_level == 2:
                    logging.debug(f'Response Body:{response.content}')
                if tag == 'items':
                    logging.debug('Customer admin has not provided access....')
                    break
            elif response.text.__contains__('Not found'):
                if debug_level == 2:
                    logging.debug(f'Response Body:{response.content}')
                if tag == 'items':
                    logging.debug('No Data Found....Skipping')
                break
            elif response.status_code > 399:
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'URLTag:{tag}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
                logging.debug('Error retrieving API response')
                break
            if tag == 'items':
                logging.debug(f'Response Body:{response.content}')
                logging.debug(f'Attempt # {tries} Failed getting:{Error} Retrying in{wait_time} seconds')
                time.sleep(wait_time)
            if tries >= 10:
                break
        finally:
            tries += 1


def get_pxc_customers():
    # Function to get all the Customers from PX Cloud
    # The objective of this API is to provide the list of all the customers
    # CSV Naming Convention: Customer.csv
    # JSON Naming Convention: Customers_Page_{page}_of_{total}.json
    logging.debug('********************** Running Customer Report ***********************')
    tRecords = 0
    global customersFlag
    customersFlag = 0
    print('Started Running Customer Report')
    totalCount = (get_json_reply(url=f'{pxc_url_customers}?max={max_items}',
                                 tag='totalCount', report='Customer Report'))
    logging.debug(f'Total Customers from totalCount: {totalCount}')
    if not totalCount:
        logging.debug('No Customers found...., exiting....')
        exit()
    pages = math.ceil(totalCount / int(max_items))
    page = 0
    customerTotal = 0
    with open(allCustomers, 'w', encoding='utf-8', newline='') as target:
        CSV_Header = 'customerId,customerName,customerGUName,successTrackId,successTrackAccess'
        writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
        writer.writerow(CSV_Header.split())
    while page < pages:
        off_set = (page * int(max_items))
        logging.debug(f'off-set is {off_set} and max is {max_items}')
        url = f'{pxc_url_customers}?offset={off_set}&max={max_items}'
        items = (get_json_reply(url, tag='items', report='Customer Report'))
        page += 1
        for item in items:
            customerNameTemp = str(item['customerName'].replace('"', ','))
            customerName = customerNameTemp.replace(',', ' ')
            customerId = str(item['customerId'])
            customerGUName = item['customerGUName'].replace(',', ' ')
            successTrack = item['successTracks']
            logging.debug(f'Found Customer {customerName}')
            if not successTrack:
                successTrack = [{'id': 'N/A', 'access': False}]
            for track in successTrack:
                successTrackId = str(track['id'])
                trackAccess = str(track['access'])
                with open(allCustomers, 'a', encoding='utf-8', newline='') as target:
                    writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
                    CSV_Data = f'{customerId},{customerName},{customerGUName},{successTrackId},{trackAccess}'
                    writer.writerow(CSV_Data.split())
                    customerTotal += 1
        if outputFormat == 1 or outputFormat == 2:
            if items is not None:
                if len(items) > 0:
                    with open(json_output_dir +
                              'Customers' +
                              '_Page_' + str(page) +
                              '_of_' + str(pages) +
                              '.json', 'w') as json_file:
                        json.dump(items, json_file)
                    logging.debug(f'Saving {json_file.name}')
    logging.debug(f'Collected {customerTotal} customer records')
    with open(customers, 'w', encoding='utf-8', newline='') as target:
        CSV_Header = 'customerId,customerName,successTrackId,successTrackAccess'
        writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
        writer.writerow(CSV_Header.split())
    with open(allCustomers, 'r') as target:
        customerList = csv.DictReader(target)
        for row in customerList:
            customerName = row['customerName']
            customerId = row['customerId']
            successTrackId = row['successTrackId']
            successTrackAccess = row['successTrackAccess']
            if successTrackAccess == 'True' and successTrackId in STList:
                with open(customers, 'a', encoding='utf-8', newline='') as output:
                    writer = csv.writer(output, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
                    CSV_Data = f'{customerId},{customerName},{successTrackId},{successTrackAccess}'
                    writer.writerow(CSV_Data.split())
                    tRecords += 1
    customersFlag = 1
    print(f'Total customers records {customerTotal}')
    logging.debug(f'Total customers records {customerTotal}')


def get_pxc_contracts():
    # Function to get the Contract List from PX Cloud
    # This API will fetch list of partner contracts transacted with Cisco.
    # CSV Naming Convention: Contract.csv
    # JSON Naming Convention: Contracts_Page_{page}_of_{total}.json
    logging.debug('********************** Running Contract Report ***********************\n')
    global contractsFlag
    contractsFlag = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Contract Report')
    totalCount = (get_json_reply(url=f'{pxc_url_contracts}?max={max_items}',
                                 tag='totalCount', report='Contract Report'))
    pages = math.ceil(totalCount / int(max_items))
    page = 0
    with open(contracts, 'w', encoding='utf-8', newline='') as target:
        CSV_Header = 'customerName,contractNumber,cuid,cavid,contractStatus,contractValue,currency,serviceLevel,' \
                     'startDate,endDate,currencySymbol,onboardedstatus'
        writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
        writer.writerow(CSV_Header.split())
        while page < pages:
            off_set = (page * int(max_items))
            url = f'{pxc_url_contracts}?offset={off_set}&max={max_items}'
            items = (get_json_reply(url, tag='items', report='Contract Report'))
            page += 1
            if items is not None:
                for item in items:
                    if item['onboardedstatus']:
                        contractNumber = str(item['contractNumber'])
                        try:
                            cuid = str(item['cuid'])
                        except KeyError:
                            cuid = 'N/A'
                            pass
                        try:
                            cavid = str(item['cavid'])
                        except KeyError:
                            cavid = 'N/A'
                            pass
                        try:
                            customerNameTemp = str(item['customerName'].replace('"', ','))
                        except KeyError:
                            customerNameTemp = 'N/A'
                            pass
                        contractStatus = str(item['contractStatus'])
                        if str(item['contractValue']) == 'None':
                            contractValue = '0.00'
                        elif str(item['contractValue']) != 'None':
                            contractValue = str(item['contractValue'])
                        customerName = customerNameTemp.replace(',', ' ')
                        currency = str(item['currency'])
                        serviceLevel = str(item['serviceLevel'].replace(',', '|'))
                        startDate = str(item['startDate'])
                        endDate = str(item['endDate'])
                        currencySymbol = str(item['currencySymbol'])
                        onboardedstatus = str(item['onboardedstatus'])
                        logging.debug(f'Found contract number {contractNumber} for {customerName}')
                        CSV_Data = f'{customerName},{contractNumber},{cuid},{cavid},{contractStatus},' \
                                   f'{contractValue},{currency},{serviceLevel},{startDate},{endDate},' \
                                   f'{currencySymbol},{onboardedstatus}'
                        writer.writerow(CSV_Data.split())
            if outputFormat == 1 or outputFormat == 2:
                if items is not None:
                    if len(items) > 0:
                        with open(json_output_dir +
                                  'Contracts' +
                                  '_Page_' + str(page) +
                                  '_of_' + str(pages) +
                                  '.json', 'w') as json_file:
                            json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
    logging.debug('Extracting Unique Contract Entries')
    # Remove duplicate contract IDs from contract list and store in unique_contracts.csv
    with open(contracts, 'r') as infile, open(csv_output_dir + 'unique_contracts.csv', 'a', newline='') as outfile:
        # this list will hold unique contracts numbers,
        contractNumbers = []
        numOfDups = 0
        totalNum = 0
        # read input file into a dictionary
        results = csv.DictReader(infile)
        writer = csv.writer(outfile)
        # write column headers to output file
        writer.writerow(['customerName', 'contractNumber'])
        for result in results:
            totalNum += 1
            customerName = result.get('customerName')
            contractNumber = result.get('contractNumber')
            # if value already exists in the list, skip writing the whole row to output file
            if contractNumber in contractNumbers:
                numOfDups += 1
                continue
            writer.writerow([customerName, contractNumber])
            # add the value to the list to be skipped subsequently
            contractNumbers.append(contractNumber)
        logging.debug('========== Contracts Summary ==========')
        logging.debug(f'Total Contracts: {totalNum}')
        logging.debug(f'Duplicated Contracts: {numOfDups}')
        logging.debug(f'Unique Contracts: {totalNum - numOfDups}')
    contractsFlag += 1
    print(f'Total contracts found {totalNum}')
    logging.debug(f'Total contracts found {totalNum}')


def get_pxc_contractswithcustomernames():
    # Function to get the Contract List with Customers Names included from PX Cloud
    # This API will fetch list of partner contracts transacted with Cisco with some different
    #  fields then the Contract Report above.
    # CSV Naming Convention: Contracts_With_Customer_Names.csv
    # JSON Naming Convention: ContractsWithNames_Page_{page}_of_{total}.json
    logging.debug('************ Running Contracts With Customer Names Report ************\n')
    global contractswithcustomernamesFlag
    contractswithcustomernamesFlag = 0
    print('Started Running Contracts With Customer Names Report')
    totalCount = (get_json_reply(url=f'{pxc_url_contractswithcustomers}?max={max_items}',
                                 tag='totalCount', report='Contract With Customer Names Report'))
    if totalCount is not None:
        pages = math.ceil(totalCount / int(max_items))
        page = 0
        with open(contractsWithCustomers, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerName,customerId,contractNumber,contractStatus,contractValue,customerGUName,' \
                         'successTrackId,serviceLevel,coverageStartDate,coverageEndDate,onboardedstatus'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
            while page < pages:
                off_set = (page * int(max_items))
                url = f'{pxc_url_contractswithcustomers}?offset={off_set}&max={max_items}'
                items = (get_json_reply(url, tag='items', report='Contract With Customer Names Report'))
                page += 1
                if items is not None:
                    for item in items:
                        customerDetails = (item['customerDetails'])
                        customerNameTemp = str(customerDetails[0]['customerName'])
                        customerName = str(customerNameTemp.replace(',', ' '))
                        customerId = str(customerDetails[0]['customerId'])
                        contractNumber = str(item['contractNumber'])
                        contractStatus = str(item['contractStatus'])
                        try:
                            contractValue = str(item['contractValue'])
                        except KeyError:
                            contractValue = ''
                        try:
                            customerGUName = str(customerDetails[0]['customerGUName'].replace('?', ' '))
                        except KeyError:
                            customerGUName = customerName
                        try:
                            serviceLevel = str(item['serviceLevel'].replace(',', '|'))
                        except KeyError:
                            serviceLevel = ''
                        coverageStartDate = str(item['coverageStartDate'])
                        coverageEndDate = str(item['coverageEndDate'])
                        try:
                            onboardedstatus = str(item['onboardedStatus'])
                        except KeyError:
                            onboardedstatus = str(item['onboardedstatus'])
                        try:
                            successTrackIds = (item['successTrackIds'])
                        except KeyError:
                            successTrackIds = ''
                        for successTrackId in successTrackIds:
                            logging.debug(f'Found contract number {contractNumber} for {customerGUName}')
                            CSV_Data = f'{customerName},{customerId},{contractNumber},{contractStatus},' \
                                       f'{contractValue},{customerGUName},{successTrackId},{serviceLevel},' \
                                       f'{coverageStartDate},{coverageEndDate},{onboardedstatus}'
                            writer.writerow(CSV_Data.split())
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir +
                                      'ContractsWithNames' +
                                      '_Page_' + str(page) +
                                      '_of_' + str(pages) +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                                logging.debug(f'Saving {json_file.name}')
    # Filter records from contract list for unique customer IDs with Level2 ST ID's
    logging.debug('Filtering records from contract list for unique customer IDs')
    with open(contractsWithCustomers, 'r') as infile, open(L2_contracts, 'a', newline='') as outfile:
        # this list will hold unique contracts numbers,
        customerIds = []
        numOfDups = 0
        totalNum = 0
        # read input file into a dictionary
        results = csv.DictReader(infile)
        writer = csv.writer(outfile)
        # write column headers to output file
        writer.writerow(['customerId', 'customerName', 'successTrackId', 'serviceLevel'])
        for result in results:
            customerName = result.get('customerName')
            customerId = result.get('customerId')
            successTrackId = result.get('successTrackId')
            serviceLevel = result.get('serviceLevel')
            # filter by serviceLevel with L2 in the name
            if customerId and serviceLevel.find('L2') != -1:
                totalNum += 1
                # if customer ID already exists in the list, skip writing the whole row to output file
                if customerId in customerIds:
                    numOfDups += 1
                    continue
                else:
                    writer.writerow([customerId, customerName, successTrackId, serviceLevel])
                    # add the value to the list to be skipped subsequently
                    customerIds.append(customerId)
        logging.debug('========== Contracts With Customer Names Summary ==========')
        logging.debug(f'Total Customers: {totalNum}')
        logging.debug(f'Duplicated Customers: {numOfDups}')
        logging.debug(f'Unique Customers: {totalNum - numOfDups}')
    logging.debug('Contracts With Customer Names Report Completed!')
    logging.debug('Building OSV and AFM search list')
    tRecords = 0
    with open(L2_contracts, 'r') as list1Input, open(OSV_AFM_List, 'a', newline='') as outfile:
        list1 = csv.DictReader(list1Input)
        writer = csv.writer(outfile)
        writer.writerow(['customerId', 'customerName', 'successTrackId', 'serviceLevel', 'successTrackAccess'])
        for row1 in list1:
            customerId = row1['customerId']
            customerName = row1['customerName']
            successTrackId = row1['successTrackId']
            serviceLevel = row1['serviceLevel']
            with open(customers, 'r') as list2Input:
                list2 = csv.DictReader(list2Input)
                for row2 in list2:
                    customerId2 = row2['customerId']
                    successTrackId2 = row2['successTrackId']
                    successTrackAccess = row2['successTrackAccess']
                    if customerId == customerId2 and successTrackId == successTrackId2:
                        writer.writerow([customerId, customerName, successTrackId2, serviceLevel, successTrackAccess])
                        tRecords += 1
    contractswithcustomernamesFlag += 1
    time.sleep(1)
    logging.debug(f'Total Success Track Level 2 records: {tRecords}')
    logging.debug('OSV and AFM search list build Completed!')
    print(f'Total Contracts With Customer Names records {totalNum}')
    print(f'Total Success Track Level 2 records: {tRecords}')


def split_csv(source_filepath, dest_folder, split_file_prefix, records_per_file):
    if records_per_file <= 0:
        raise Exception('records_per_file must be > 0')
    with open(source_filepath, 'r') as source:
        reader = csv.reader(source)
        headers = next(reader)
        file_idx = 0
        records_exist = True
        while records_exist:
            i = 0
            target_filename = f'{split_file_prefix}_{file_idx}.csv'
            target_filepath = os.path.join(dest_folder, target_filename)
            with open(target_filepath, 'w', newline='') as target:
                writer = csv.writer(target)
                while i < records_per_file:
                    if i == 0:
                        writer.writerow(headers)
                    try:
                        writer.writerow(next(reader))
                        i += 1
                    except StopIteration:
                        records_exist = False
                        break
            if i == 0:
                # we only wrote the header, so delete that file
                os.remove(target_filepath)
            file_idx += 1


def csv_count_rows(file, chunks):
    with open(file) as f:
        nb_lines = sum(1 for _ in f)
    row_limit = round(nb_lines / chunks)
    split_csv(file, 'temp/', 'split', row_limit)


def get_pxc_contracts_details_part1():
    # Function to get the Contract List from PX Cloud
    # This API will fetch list of partner contract line items transacted with Cisco.
    # CSV Naming Convention: Contract_Details.csv
    # JSON Naming Convention: {Customer Name}_Contract_Details_{ContractNumber}_Page_{page}_of_{total}.json
    logging.debug('********************** Running Contract Details **********************\n')
    tRecords = 0
    while contractsFlag == 0:
        logging.debug(f'Waiting on Contracts List to finish')
        time.sleep(1)
    print('Started Running Contract Details Report Part 1')
    token_time_check()
    if outputFormat == 1 or outputFormat == 3:
        with open('temp/CD_0.csv', 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerName,customerGUName,customerHQName,contractNumber,productId,serialNumber,' \
                         'contractStatus,componentType,serviceLevel,coverageStartDate,coverageEndDate,' \
                         'installationQuantity,instanceNumber'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open('temp/split_0.csv', 'r') as target:
        contractList = csv.DictReader(target)
        for row in contractList:
            customerName = row['customerName'].replace(',', ' ')
            contractNumber = row['contractNumber']
            logging.debug(f'Collecting contract details for {customerName}')
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            url = f'{pxc_url_contracts_details}?contractNumber={contractNumber}'
            logging.debug(f'Scanning {customerName} for contract Number: {contractNumber}')
            max_contractDetailsPart1_attempts = 10
            contractDetailsPart1_attempts = 0
            while contractDetailsPart1_attempts < max_contractDetailsPart1_attempts:
                # Make a request to API
                for i in range(10):
                    i += 1
                    logging.debug(f'Request attempt number {i}')
                    try:
                        response = requests.request('GET', url, headers=headers, data=data_payload,
                                                    verify=True, timeout=apiTimeout)
                        if i > 1:
                            logging.debug(f'API request retry # {i} was successful')
                        break
                    except Timeout:
                        logging.debug(f'Time out error getting resonse')
                # If not rate limited, break out of while loop and continue
                if response.status_code != 429:
                    logging.debug(f'Querying:')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'Request Body: {data_payload}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    if contractDetailsPart1_attempts == 0:
                        logging.debug('Success')
                    break
                # If rate limited, wait and try again
                logging.debug('Rate Limit Exceeded for Contract Details Report! Retrying...')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
                contractDetailsPart1_attempts += 1
                pause = random.random()
                logging.debug(f'sleeping for {pause} seconds')
                time.sleep(pause)
            if contractDetailsPart1_attempts > 0:
                logging.debug(f'Retry was successful for Contract Details Report')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
            reply = json.loads(response.text)
            logging.debug(f'URL Request: {url}')
            logging.debug(f'Request Body: {data_payload}')
            logging.debug(f'HTTP Code:{response.status_code}')
            logging.debug(f'Review API Headers:{response.headers}')
            logging.debug(f'Response Body:{response.content}')
            totalCount = reply['totalCount']
            if totalCount > 0:
                logging.debug(f'Details found for contract {contractNumber}')
                pages = math.ceil(totalCount / int(max_items))
                logging.debug(f'Total Pages: {pages}')
                logging.debug(f'Total Records: {totalCount}')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
                page = 0
                while page < pages:
                    off_set = (page * int(max_items))
                    url = f'{pxc_url_contracts_details}?contractNumber={contractNumber}' \
                          f'&offset={off_set}&max={max_items}'
                    items = (get_json_reply(url, tag='items', report='Contract Details Report'))
                    if items:
                        if outputFormat == 1 or outputFormat == 2:
                            if items is not None:
                                if len(items) > 0:
                                    json_file = (json_output_dir + customerName.replace('/', '-') +
                                                 '_Contract_Details_' + contractNumber +
                                                 '_Page_' + str(page + 1) +
                                                 '_of_' + str(pages) +
                                                 '.json')
                                    if not os.path.isfile(json_file):
                                        with open(json_file, 'w') as fileTarget:
                                            json.dump(items, fileTarget)
                                            logging.debug(f'Saving {json_file}')
                                    else:
                                        logging.debug('Duplcates found and being ignored...')
                        if outputFormat == 1 or outputFormat == 3:
                            if items is not None:
                                if len(items) > 0:
                                    for item in items:
                                        customerGUName = str(item['customerGUName'].replace(',', ' '))
                                        customerHQName = str(item['customerHQName'].replace(',', ' '))
                                        productId = str(item['productId'].replace(',', ' '))
                                        try:
                                            serialNumber = str(item['serialNumber'])
                                        except KeyError:
                                            serialNumber = 'N/A'
                                            pass
                                        contractStatus = str(item['contractStatus'])
                                        componentType = str(item['componentType'].replace(',', ' '))
                                        serviceLevel = str(item['serviceLevel'].replace(',', ' '))
                                        coverageStartDate = str(item['coverageStartDate'])
                                        coverageEndDate = str(item['coverageEndDate'])
                                        installationQuantity = str(item['installationQuantity'])
                                        instanceNumber = str(item['instanceNumber'])
                                        with open('temp/CD_0.csv', 'a', encoding='utf-8', newline='') \
                                                as details_target:
                                            writer = csv.writer(details_target,
                                                                delimiter=' ',
                                                                quotechar=' ',
                                                                quoting=csv.QUOTE_MINIMAL)
                                            CSV_Data = f'{customerName},{customerGUName},{customerHQName},' \
                                                       f'{contractNumber},{productId},{serialNumber},' \
                                                       f'{contractStatus},{componentType},{serviceLevel},' \
                                                       f'{coverageStartDate},{coverageEndDate},' \
                                                       f'{installationQuantity},{instanceNumber}'
                                            writer.writerow(CSV_Data.split())
                                            tRecords += 1
                    page += 1
                logging.debug(f'Collected Details for contract number {contractNumber}')
            else:
                logging.debug(f'No details found for contract number {contractNumber}')
    print(f'Total Contract Details records for Part 1 {tRecords}')
    logging.debug(f'Total Contract Details records for Part 1 {tRecords}')


def get_pxc_contracts_details_part2():
    # Function to get the Contract List from PX Cloud
    # This API will fetch list of partner contract line items transacted with Cisco.
    # CSV Naming Convention: Contract_Details.csv
    # JSON Naming Convention: {Customer Name}_Contract_Details_{ContractNumber}_Page_{page}_of_{total}.json
    logging.debug('********************** Running Contract Details **********************\n')
    tRecords = 0
    while contractsFlag == 0:
        logging.debug(f'Waiting on Contracts List to finish')
        time.sleep(1)
    print('Started Running Contract Details Report Part 2')
    token_time_check()
    if outputFormat == 1 or outputFormat == 3:
        with open('temp/CD_1.csv', 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerName,customerGUName,customerHQName,contractNumber,productId,serialNumber,' \
                         'contractStatus,componentType,serviceLevel,coverageStartDate,coverageEndDate,' \
                         'installationQuantity,instanceNumber'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open('temp/split_1.csv', 'r') as target:
        contractList = csv.DictReader(target)
        for row in contractList:
            customerName = row['customerName'].replace(',', ' ')
            contractNumber = row['contractNumber']
            logging.debug(f'Collecting contract details for {customerName}')
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            url = f'{pxc_url_contracts_details}?contractNumber={contractNumber}'
            logging.debug(f'Scanning {customerName} for contract Number: {contractNumber}')
            max_contractDetailsPart2_attempts = 10
            contractDetailsPart2_attempts = 0
            while contractDetailsPart2_attempts < max_contractDetailsPart2_attempts:
                # Make a request to API
                for i in range(10):
                    i += 1
                    logging.debug(f'Request attempt number {i}')
                    try:
                        response = requests.request('GET', url, headers=headers, data=data_payload,
                                                    verify=True, timeout=apiTimeout)
                        if i > 1:
                            logging.debug(f'API request retry # {i} was successful')
                        break
                    except Timeout:
                        logging.debug(f'Time out error getting resonse')
                # If not rate limited, break out of while loop and continue
                if response.status_code != 429:
                    logging.debug(f'Querying:')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'Request Body: {data_payload}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    if contractDetailsPart2_attempts == 0:
                        logging.debug('Success')
                    break
                # If rate limited, wait and try again
                logging.debug('Rate Limit Exceeded for Contract Details Report! Retrying...')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
                contractDetailsPart2_attempts += 1
                pause = random.random()
                logging.debug(f'sleeping for {pause} seconds')
                time.sleep(pause)
            if contractDetailsPart2_attempts > 0:
                logging.debug(f'Retry was successful for Contract Details Report')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
            reply = json.loads(response.text)
            logging.debug(f'URL Request: {url}')
            logging.debug(f'Request Body: {data_payload}')
            logging.debug(f'HTTP Code:{response.status_code}')
            logging.debug(f'Review API Headers:{response.headers}')
            logging.debug(f'Response Body:{response.content}')
            totalCount = reply['totalCount']
            if totalCount > 0:
                logging.debug(f'Details found for contract {contractNumber}')
                pages = math.ceil(totalCount / int(max_items))
                logging.debug(f'Total Pages: {pages}')
                logging.debug(f'Total Records: {totalCount}')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
                page = 0
                while page < pages:
                    off_set = (page * int(max_items))
                    url = f'{pxc_url_contracts_details}?contractNumber={contractNumber}' \
                          f'&offset={off_set}&max={max_items}'
                    items = (get_json_reply(url, tag='items', report='Contract Details Report'))
                    if items:
                        if outputFormat == 1 or outputFormat == 2:
                            if items is not None:
                                if len(items) > 0:
                                    json_file = (json_output_dir + customerName.replace('/', '-') +
                                                 '_Contract_Details_' + contractNumber +
                                                 '_Page_' + str(page + 1) +
                                                 '_of_' + str(pages) +
                                                 '.json')
                                    if not os.path.isfile(json_file):
                                        with open(json_file, 'w') as fileTarget:
                                            json.dump(items, fileTarget)
                                            logging.debug(f'Saving {json_file}')
                                    else:
                                        logging.debug('Duplcates found and being ignored...')
                        if outputFormat == 1 or outputFormat == 3:
                            if items is not None:
                                if len(items) > 0:
                                    for item in items:
                                        customerGUName = str(item['customerGUName'].replace(',', ' '))
                                        customerHQName = str(item['customerHQName'].replace(',', ' '))
                                        productId = str(item['productId'].replace(',', ' '))
                                        try:
                                            serialNumber = str(item['serialNumber'])
                                        except KeyError:
                                            serialNumber = 'N/A'
                                            pass
                                        contractStatus = str(item['contractStatus'])
                                        componentType = str(item['componentType'].replace(',', ' '))
                                        serviceLevel = str(item['serviceLevel'].replace(',', ' '))
                                        coverageStartDate = str(item['coverageStartDate'])
                                        coverageEndDate = str(item['coverageEndDate'])
                                        installationQuantity = str(item['installationQuantity'])
                                        instanceNumber = str(item['instanceNumber'])
                                        with open('temp/CD_1.csv', 'a', encoding='utf-8', newline='') \
                                                as details_target:
                                            writer = csv.writer(details_target,
                                                                delimiter=' ',
                                                                quotechar=' ',
                                                                quoting=csv.QUOTE_MINIMAL)
                                            CSV_Data = f'{customerName},{customerGUName},{customerHQName},' \
                                                       f'{contractNumber},{productId},{serialNumber},' \
                                                       f'{contractStatus},{componentType},{serviceLevel},' \
                                                       f'{coverageStartDate},{coverageEndDate},' \
                                                       f'{installationQuantity},{instanceNumber}'
                                            writer.writerow(CSV_Data.split())
                                            tRecords += 1
                    page += 1
                logging.debug(f'Collected Details for contract number {contractNumber}')
            else:
                logging.debug(f'No details found for contract number {contractNumber}')
    print(f'Total Contract Details records for Part 2 {tRecords}')
    logging.debug(f'Total Contract Details records for Part 2 {tRecords}')


def get_pxc_contracts_details_part3():
    # Function to get the Contract List from PX Cloud
    # This API will fetch list of partner contract line items transacted with Cisco.
    # CSV Naming Convention: Contract_Details.csv
    # JSON Naming Convention: {Customer Name}_Contract_Details_{ContractNumber}_Page_{page}_of_{total}.json
    logging.debug('********************** Running Contract Details **********************\n')
    tRecords = 0
    while contractsFlag == 0:
        logging.debug(f'Waiting on Contracts List to finish')
        time.sleep(1)
    print('Started Running Contract Details Report Part 3')
    token_time_check()
    if outputFormat == 1 or outputFormat == 3:
        with open('temp/CD_2.csv', 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerName,customerGUName,customerHQName,contractNumber,productId,serialNumber,' \
                         'contractStatus,componentType,serviceLevel,coverageStartDate,coverageEndDate,' \
                         'installationQuantity,instanceNumber'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open('temp/split_2.csv', 'r') as target:
        contractList = csv.DictReader(target)
        for row in contractList:
            customerName = row['customerName'].replace(',', ' ')
            contractNumber = row['contractNumber']
            logging.debug(f'Collecting contract details for {customerName}')
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            url = f'{pxc_url_contracts_details}?contractNumber={contractNumber}'
            logging.debug(f'Scanning {customerName} for contract Number: {contractNumber}')
            max_contractDetailsPart3_attempts = 10
            contractDetailsPart3_attempts = 0
            while contractDetailsPart3_attempts < max_contractDetailsPart3_attempts:
                # Make a request to API
                for i in range(10):
                    i += 1
                    logging.debug(f'Request attempt number {i}')
                    try:
                        response = requests.request('GET', url, headers=headers, data=data_payload,
                                                    verify=True, timeout=apiTimeout)
                        if i > 1:
                            logging.debug(f'API request retry # {i} was successful')
                        break
                    except Timeout:
                        logging.debug(f'Time out error getting resonse')
                # If not rate limited, break out of while loop and continue
                if response.status_code != 429:
                    logging.debug(f'Querying:')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'Request Body: {data_payload}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    if contractDetailsPart3_attempts == 0:
                        logging.debug('Success')
                    break
                # If rate limited, wait and try again
                logging.debug('Rate Limit Exceeded for Contract Details Report! Retrying...')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
                contractDetailsPart3_attempts += 1
                pause = random.random()
                logging.debug(f'sleeping for {pause} seconds')
                time.sleep(pause)
            if contractDetailsPart3_attempts > 0:
                logging.debug(f'Retry was successful for Contract Details Report')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
            reply = json.loads(response.text)
            logging.debug(f'URL Request: {url}')
            logging.debug(f'Request Body: {data_payload}')
            logging.debug(f'HTTP Code:{response.status_code}')
            logging.debug(f'Review API Headers:{response.headers}')
            logging.debug(f'Response Body:{response.content}')
            totalCount = reply['totalCount']
            if totalCount > 0:
                logging.debug(f'Details found for contract {contractNumber}')
                pages = math.ceil(totalCount / int(max_items))
                logging.debug(f'Total Pages: {pages}')
                logging.debug(f'Total Records: {totalCount}')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'Request Body: {data_payload}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
                page = 0
                while page < pages:
                    off_set = (page * int(max_items))
                    url = f'{pxc_url_contracts_details}?contractNumber={contractNumber}' \
                          f'&offset={off_set}&max={max_items}'
                    items = (get_json_reply(url, tag='items', report='Contract Details Report'))
                    if items:
                        if outputFormat == 1 or outputFormat == 2:
                            if items is not None:
                                if len(items) > 0:
                                    json_file = (json_output_dir + customerName.replace('/', '-') +
                                                 '_Contract_Details_' + contractNumber +
                                                 '_Page_' + str(page + 1) +
                                                 '_of_' + str(pages) +
                                                 '.json')
                                    if not os.path.isfile(json_file):
                                        with open(json_file, 'w') as fileTarget:
                                            json.dump(items, fileTarget)
                                            logging.debug(f'Saving {json_file}')
                                    else:
                                        logging.debug('Duplcates found and being ignored...')
                        if outputFormat == 1 or outputFormat == 3:
                            if items is not None:
                                if len(items) > 0:
                                    for item in items:
                                        customerGUName = str(item['customerGUName'].replace(',', ' '))
                                        customerHQName = str(item['customerHQName'].replace(',', ' '))
                                        productId = str(item['productId'].replace(',', ' '))
                                        try:
                                            serialNumber = str(item['serialNumber'])
                                        except KeyError:
                                            serialNumber = 'N/A'
                                            pass
                                        contractStatus = str(item['contractStatus'])
                                        componentType = str(item['componentType'].replace(',', ' '))
                                        serviceLevel = str(item['serviceLevel'].replace(',', ' '))
                                        coverageStartDate = str(item['coverageStartDate'])
                                        coverageEndDate = str(item['coverageEndDate'])
                                        installationQuantity = str(item['installationQuantity'])
                                        instanceNumber = str(item['instanceNumber'])
                                        with open('temp/CD_2.csv', 'a', encoding='utf-8', newline='') \
                                                as details_target:
                                            writer = csv.writer(details_target,
                                                                delimiter=' ',
                                                                quotechar=' ',
                                                                quoting=csv.QUOTE_MINIMAL)
                                            CSV_Data = f'{customerName},{customerGUName},{customerHQName},' \
                                                       f'{contractNumber},{productId},{serialNumber},' \
                                                       f'{contractStatus},{componentType},{serviceLevel},' \
                                                       f'{coverageStartDate},{coverageEndDate},' \
                                                       f'{installationQuantity},{instanceNumber}'
                                            writer.writerow(CSV_Data.split())
                                            tRecords += 1
                    page += 1
                logging.debug(f'Collected Details for contract number {contractNumber}')
            else:
                logging.debug(f'No details found for contract number {contractNumber}')
    print(f'Total Contract Details records for Part 3 {tRecords}')
    logging.debug(f'Total Contract Details records for Part 3 {tRecords}')


def merge_files_with_os(csvfile1, csvfile2, csvfile3, merged_file):
    outputFile = csv.writer(open(merged_file,  'w', encoding='utf-8', newline=''))
    firstfile = True
    for file in [csvfile1, csvfile2, csvfile3]:
        with open(file, 'r') as rawfile:
            reader = csv.reader(rawfile)
            for idx, row in enumerate(reader):
                if idx == 0 and not firstfile:
                    continue
                outputFile.writerow(row)
        firstfile = False


def get_pxc_partner_offers():
    # Function to get the Partner Offers from PX Cloud
    # This API will fetch all the offers created by the Partners.
    # CSV Naming Convention: Partner_Offers.csv
    # JSON Naming Convention: Partner_Offers.json
    logging.debug('******************* Running Partner Offers Report ********************\n')
    tRecords = 0
    print('Started Running Partner Offers Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(partnerOffers, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,offerId,offerType,title,description,duration,accTimeRequiredHours,imageFileName,' \
                         'customerRating,status,userFirstName,userLastName,userEmailId,createdBy,createdOn,' \
                         'modifiedBy,modifiedOn,language,mappingId,successTrackId,successTrackName,usecaseId,usecase,' \
                         'pitstopId,pitstop,mappingChecklistId,checklistId,checklist,publishedToAllCustomers,' \
                         'customerEntryId,companyName'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    items = (get_json_reply(url=pxc_url_partner_offers, tag='items', report='Partner Offers'))
    if not items:
        logging.debug('No Data Found For Partner Offers.')
    if outputFormat == 1 or outputFormat == 3:
        if items is not None:
            if len(items) > 0:
                for item in items:
                    offerId = str(item['offerId']).replace(',', ' ')
                    offerType = str(item['offerType']).replace(',', ' ')
                    title = str(item['title']).replace(',', ' ')
                    description = str(item['description']).replace(',', ' ')
                    duration = str(item['duration'])
                    accTimeRequiredHours = str(item['accTimeRequiredHours'])
                    try:
                        imageFileName = str(item['imageFileName']).replace(',', ' ')
                    except KeyError:
                        imageFileName = 'None'
                        pass
                    customerRating = str(item['customerRating'])
                    status = str(item['status']).replace(',', ' ')
                    userFirstName = str(item['userFirstName']).replace(',', ' ')
                    userLastName = str(item['userLastName']).replace(',', ' ')
                    userEmailId = str(item['userEmailId']).replace(',', ' ')
                    createdBy = str(item['createdBy']).replace(',', ' ')
                    createdOn = str(item['created'])
                    modifiedBy = item['offerType'].replace(',', ' ')
                    modifiedOn = str(item['modified'])
                    language = str(item['language']).replace(',', ' ')
                    mappings = item['mapping']
                    publishedToAllCustomers = str(item['publishedToAllCustomers'])
                    customerList = item['customerList']
                    logging.debug(f'Found Partner Offer ID {offerId}')
                    for customer in customerList:
                        customerEntryId = str(customer['customerEntryId']).replace(',', ' ')
                        customerId = str(customer['customerId']).replace(',', ' ')
                        companyName = str(customer['companyName']).replace(',', ' ')
                        for mapping in mappings:
                            mappingId = str(mapping['mappingId']).replace(',', ' ')
                            successTrackId = str(mapping['successTrackId']).replace(',', ' ')
                            successTrackName = str(mapping['successTrackName']).replace(',', ' ')
                            usecaseId = str(mapping['usecaseId']).replace(',', ' ')
                            usecase = str(mapping['usecase']).replace(',', ' ')
                            pitstopId = str(mapping['pitstopId']).replace(',', ' ')
                            pitstop = str(mapping['pitstop']).replace(',', ' ')
                            checklists = mapping['checklists']
                            for row in checklists:
                                mappingChecklistId = str(row['mappingChecklistId'])
                                checklistId = str(row['checklistId'])
                                try:
                                    checklist = str(row['checklist'])
                                except KeyError:
                                    checklist = 'N/A'
                                    pass
                                with open(partnerOffers, 'a', encoding='utf-8', newline='') as target:
                                    writer = csv.writer(target,
                                                        delimiter=' ',
                                                        quotechar=' ',
                                                        quoting=csv.QUOTE_MINIMAL)
                                    CSV_Data = f'{customerId},{offerId},{offerType},{title},{description},{duration},' \
                                               f'{accTimeRequiredHours},{imageFileName},{customerRating},{status},' \
                                               f'{userFirstName},{userLastName},{userEmailId},{createdBy},' \
                                               f'{createdOn},{modifiedBy},{modifiedOn},{language},{mappingId},' \
                                               f'{successTrackId},{successTrackName},{usecaseId},{usecase},' \
                                               f'{pitstopId},{pitstop},{mappingChecklistId},{checklistId},' \
                                               f'{checklist},{publishedToAllCustomers},{customerEntryId},{companyName}'
                                    writer.writerow(CSV_Data.split())
                                    tRecords += 1
        else:
            logging.debug('No Partner Offer Data Found .... Skipping')
    if outputFormat == 1 or outputFormat == 2:
        if items is not None:
            if len(items) > 0:
                with open(json_output_dir + 'Partner_Offers.json', 'w') as json_file:
                    json.dump(items, json_file)
                logging.debug(f'Saving {json_file.name}')
    print(f'Total Partner Offers records {tRecords}')
    logging.debug(f'Total Partner Offers records {tRecords}')


def get_pxc_partner_offer_sessions():
    # Function to get the Partner Offer Sessions from PX Cloud
    # This API will fetch all the active and inactive sessions of all the Offers created by the Partners.
    # CSV Naming Convention: Partner_Offer_Sessions.csv
    # JSON Naming Convention: Partner_Offer_Sessions.json
    logging.debug('**************** Running Partner Offers Sessions Report **************\n')
    tRecords = 0
    print('Started Running Partner Offers Sessions Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(partnerOfferSessions, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,companyName,offerId,sessionId,timezone,status,attendeeId,ccoId,' \
                         'attendeeUserEmail,attendeeUserFullName,noOfAttendees,preferredSlot,businessOutcome,' \
                         'reasonForInterest,createdDate,modifiedDate'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    items = (get_json_reply(url=pxc_url_partner_offers_sessions, tag='items', report='Partner Offer Sessions'))
    if not items:
        logging.debug('No Partner Offers Sessions Data Found.')
    if outputFormat == 1 or outputFormat == 3:
        if items is not None:
            if len(items) > 0:
                for item in items:
                    offerId = item['offerId'].replace(',', ' ')
                    sessions = item['sessions']
                    for session in sessions:
                        sessionId = session['sessionId'].replace(',', ' ')
                        timezone = session['timezone'].replace(',', ' ')
                        status = session['status'].replace(',', ' ')
                        noOfAttendees = str(session['noOfAttendees'])
                        try:
                            preferredSlot = session['preferredSlot'].replace(',', ' ')
                        except KeyError:
                            preferredSlot = 'N/A'
                            pass
                        try:
                            businessOutcome = session['businessOutcome'].replace(',', ' ')
                        except KeyError:
                            businessOutcome = 'N/A'
                            pass
                        try:
                            reasonForInterest = session['reasonForInterest'].replace(',', ' ')
                        except KeyError:
                            reasonForInterest = 'N/A'
                            pass
                        createdDate = str(session['createdDate'])
                        modifiedDate = str(session['modifiedDate'])
                        attendeeList = session['attendeeList']
                        if not attendeeList:
                            attendeeList = [{'attendeeId': 'N/A',
                                             'ccoId': 'N/A',
                                             'attendeeUserEmail': 'N/A',
                                             'attendeeUserFullName': 'N/A',
                                             'customerId': 'N/A',
                                             'companyName': 'N/A'}]
                        for attendee in attendeeList:
                            attendeeId = attendee['attendeeId'].replace(',', ' ')
                            try:
                                ccoId = attendee['ccoId'].replace(',', ' ')
                            except KeyError:
                                ccoId = 'N/A'
                                pass
                            attendeeUserEmail = attendee['attendeeUserEmail'].replace(',', ' ')
                            attendeeUserFullName = attendee['attendeeUserFullName'].replace(',', ' ')
                            customerId = attendee['customerId'].replace(',', ' ')
                            companyName = attendee['companyName'].replace(',', ' ')
                            with open(partnerOfferSessions, 'a', encoding='utf-8', newline='') as target:
                                writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
                                logging.debug(f'Found Partner Offer Sessions for Session ID {sessionId}')
                                CSV_Data = f'{customerId},{companyName},{offerId},{sessionId},{timezone},{status},' \
                                           f'{attendeeId},{ccoId},{attendeeUserEmail},{attendeeUserFullName},' \
                                           f'{noOfAttendees},{preferredSlot},{businessOutcome},{reasonForInterest},' \
                                           f'{createdDate},{modifiedDate}'
                                writer.writerow(CSV_Data.split())
                                tRecords += 1
        else:
            logging.debug('No Partner Offers Sessions Data Found .... Skipping')
    if outputFormat == 1 or outputFormat == 2:
        if items is not None:
            if len(items) > 0:
                with open(json_output_dir + 'Partner_Offer_Sessions.json', 'w') as json_file:
                    json.dump(items, json_file)
                logging.debug(f'Saving {json_file.name}')
    logging.debug(f'Total Partner Offers Sessions records {tRecords}')
    print(f'Total Partner Offers Sessions records {tRecords}')


def get_pxc_successtracks():
    # Function to get the Success Track data from PX Cloud
    # This API will get customer success tracks which will provide the usecase details.
    # CSV Naming Convention: SuccessTracks.csv
    # JSON Naming Convention: SuccessTracks.json
    logging.debug('******************** Running Success Tracks Report *******************\n')
    tRecords = 0
    print('Started Running Success Tracks Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(successTracks, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'successTracksId,successTrackName,useCaseName,useCaseId'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    tries = 1
    while True:
        items = (get_json_reply(url=pxc_url_successTracks, tag='items', report='Success Tracks'))
        try:
            if len(items) > 0:
                for item in items:
                    successTrackName = item['successTrack'].replace(',', ' ')
                    successTracksId = item['id'].replace(',', ' ')
                    useCases = item['usecases']
                    for useCase in useCases:
                        useCaseName = useCase['name'].replace(',', ' ')
                        useCaseId = useCase['id'].replace(',', ' ')
                        if outputFormat == 1 or outputFormat == 3:
                            with open(successTracks, 'a', encoding='utf-8', newline='') as target:
                                writer = csv.writer(target,
                                                    delimiter=' ',
                                                    quotechar=' ',
                                                    quoting=csv.QUOTE_MINIMAL)
                                logging.debug(f'Found Success Track {successTrackName} for use case {useCaseName}')
                                CSV_Data = f'{successTracksId},{successTrackName},{useCaseName},{useCaseId}'
                                writer.writerow(CSV_Data.split())
                                tRecords += 1
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + 'SuccessTracks.json',
                                      'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
            else:
                logging.debug('Error')
                raise Exception('Error')
        except Exception as Error:
            logging.debug(f'{Error} Retrying....')
            logging.debug(f'Pausing for {wait_time} seconds before retrying...')
            time.sleep(wait_time * tries)  # increase the wait with the number of retries
            if tries >= 2:
                break
        else:
            break
        finally:
            tries += 1
    print(f'Total Success Tracks Records {tRecords}')
    logging.debug(f'Total Success Tracks Records {tRecords}')


def pxc_assets_reports():
    # Function to request, download and process Asset report data from PX Cloud
    # Assets report gives the list of all assets (products like network element and other devices) owned by customer.
    # CSV Naming Convention: Assets.csv
    # JSON Naming Convention: {Customer ID}_Assets_{UniqueReportID}.json
    logging.debug('*********************** Running Asset Report *************************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Asset Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(assets, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,successTrackId,useCaseId,assetId,assetName,productFamily,productType,' \
                         'serialNumber,productId,ipAddress,productDescription,softwareType,softwareRelease,role,' \
                         'location,coverageStatus,lastScan,endOfLifeAnnounced,endOfSale,lastShip,' \
                         'endOfRoutineFailureAnalysis,endOfNewServiceAttach,endOfServiceContractRenewal,ldosDate,' \
                         'connectionStatus,managedBy,contractNumber,coverageEndDate,coverageStartDate,supportType,' \
                         'advisories,assetType,criticalSecurityAdvisories,addressLine1,addressLine2,addressLine3,' \
                         'licenseStatus,licenseLevel,profileName,hclStatus,ucsDomain,hxCluster,subscriptionId,' \
                         'subscriptionStartDate,subscriptionEndDate'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        count = 0
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            successTrackId = row['successTrackId']
            customerName = row['customerName']
            if successTrackId in STList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}/reports'
                asset_payload = json.dumps({"reportName": "Assets", "successTrackId": successTrackId})
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                max_assets_attempts = 10
                assets_attempts = 0
                logging.debug(f'Requesting Asset data for {customerName} on Success Track ID {successTrackId}')
                while assets_attempts < max_assets_attempts:
                    # Make a request to API
                    response = requests.request('POST', url, headers=headers, data=asset_payload,
                                                verify=True, timeout=apiTimeout)
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        logging.debug(f'Querying with {asset_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if assets_attempts == 0:
                            logging.debug(f'Successfuly posted POST request for {asset_payload}')
                        break
                    # If rate limited, wait and try again
                    logging.debug(f'Rate Limit Exceeded for Asset Report! Retrying...')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    assets_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if assets_attempts > 0:
                    logging.debug(f'Retry was successful for Asset Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                if response.text.__contains__('Customer admin has not provided access.'):
                    logging.debug(f'{customerName} admin has not provided access for {successTrackId}')
                if not (response.text.__contains__('Customer admin has not provided access.')):
                    try:
                        if bool(response.headers['location']) is True:
                            pass
                        else:
                            raise KeyError
                    except KeyError:
                        logging.debug(f'\nException Thrown for Customer:{customerId} on Success Track {successTrackId} '
                                      f'\n{response} Headers:{headers} in function pxc_assets_reports')
                        while response.status_code >= 400:
                            logging.debug(f'Pausing for {wait_time} seconds before re-request...')
                            time.sleep(wait_time)
                            logging.debug('Resuming...')
                            logging.debug('Making API Call again with the following...')
                            logging.debug(f'URL: {url}')
                            logging.debug(f'Data Payload:{asset_payload}')
                            response = requests.request('POST', url, headers=headers, data=asset_payload,
                                                        verify=True, timeout=apiTimeout)
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(f'Review for Customer:{customerId} on Success Track{successTrackId} '
                                          f'Report Failed to Download')
                    location = response.headers['location']
                    logging.debug(location)
                    count += 1
                    logging.debug(f'Asset {count}:{location}')
                    location_ready_status(location, headers, 'Assets')
                    json_filename = (location.split('/')[-1] + '.json')
                    tries = 1
                    while True:
                        # time.sleep(wait_time * tries)
                        filename = (temp_dir + customerId + '_Assets_' + location.split('/')[-1] + '.zip')
                        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                        max_assets_attempts = 10
                        assets_attempts = 0
                        logging.debug(f'Requesting Asset data for {customerName} on Success Track ID {successTrackId}')
                        while assets_attempts < max_assets_attempts:
                            # Make a request to API
                            response = requests.request('GET', location, headers=headers,
                                                        verify=True, timeout=apiTimeout)
                            # If not rate limited, break out of while loop and continue
                            if response.status_code != 429:
                                logging.debug(f'Querying Location:{location}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if assets_attempts == 0:
                                    logging.debug(f'Successfuly posted GET request for {asset_payload}')
                                break
                            # If rate limited, wait and try again
                            logging.debug(f'Rate Limit Exceeded for Asset Report! Retrying...')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            assets_attempts += 1
                            pause = random.random()
                            logging.debug(f'sleeping for {pause} seconds')
                            time.sleep(pause)
                        if assets_attempts > 0:
                            logging.debug(f'Retry was successful for Asset Report')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Location Request: {location}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Wait Time:{tries}')
                        try:
                            with open(filename, 'wb') as file:
                                file.write(response.content)
                            with open(filename, 'rb') as file:
                                if file.read() and response.status_code == 200:
                                    with zipfile.ZipFile(filename, 'r') as zip_file:
                                        zip_file.extractall(temp_dir)
                                        if tries >= 2:
                                            if debug_level == 2:
                                                logging.debug(f'Success on retry!')
                                else:
                                    logging.debug('Download Failed')
                                    raise Exception('File Corrupted')
                        except Exception as FileCorruptError:
                            logging.debug(f'Deleting file...{filename} - {FileCorruptError} - Retrying....')
                            logging.debug(f'Pausing for {wait_time} seconds before retrying...')
                            time.sleep(wait_time * tries)  # increase the wait with the number of retries
                            if tries >= 3:
                                break
                        else:
                            break
                        finally:
                            tries += 1
                    if useProductionURL == 0:
                        proccess_sandbox_files(filename, customerId, '_Assets_', json_filename)
                    input_json_file = str(temp_dir + customerId + '_Assets_' + json_filename)
                    logging.debug(f'Reading file:{input_json_file}')
                    if not os.path.isfile(input_json_file):
                        logging.debug('File Not Found, Skipping.....')
                    if os.path.isfile(input_json_file):
                        with open(input_json_file, 'r', encoding='utf-8') as inputTarget:
                            data = json.load(inputTarget)
                            customerId = data['metadata']['customerId']
                            totalCount = data['metadata']['totalCount']
                            items = data['items']
                        if outputFormat == 1 or outputFormat == 3:
                            if totalCount:
                                logging.debug(f'Found Asset Data on Success Track {successTrackId}')
                                for item in items:
                                    assetId = str(item['assetId']).replace(',', '_')
                                    assetName = str(item['assetName'])
                                    productFamily = str(item['productFamily']).replace(',', ' ')
                                    productType = str(item['productType']).replace(',', ' ')
                                    serialNumber = str(item['serialNumber'])
                                    productId = str(item['productId'])
                                    ipAddress = str(item['ipAddress'])
                                    productDescription = str(item['productDescription']).replace(',', ' ')
                                    softwareType = str(item['softwareType']).replace(',', ' ')
                                    softwareRelease = str(item['softwareRelease'])
                                    try:
                                        role = str((item['role']))
                                    except KeyError:
                                        role = 'N/A'
                                        pass
                                    assetLocation = str(item['location']).replace(',', ' ')
                                    coverageStatus = str(item['coverageStatus'])
                                    try:
                                        lastScan = str(item['lastScan'])
                                        if lastScan == 'None':
                                            lastScan = ''
                                    except KeyError:
                                        lastScan = ''
                                        pass
                                    successTrack = item['successTrack']
                                    endOfLifeAnnounced = str(item['endOfLifeAnnounced'])
                                    if endOfLifeAnnounced == 'None':
                                        endOfLifeAnnounced = ''
                                    endOfSale = str(item['endOfSale'])
                                    if endOfSale == 'None':
                                        endOfSale = ''
                                    lastShip = str(item['lastShip'])
                                    if lastShip == 'None':
                                        lastShip = ''
                                    endOfRoutineFailureAnalysis = str(item['endOfRoutineFailureAnalysis'])
                                    if endOfRoutineFailureAnalysis == 'None':
                                        endOfRoutineFailureAnalysis = ''
                                    endOfNewServiceAttach = str(item['endOfNewServiceAttach'])
                                    if endOfNewServiceAttach == 'None':
                                        endOfNewServiceAttach = ''
                                    endOfServiceContractRenewal = str(item['endOfServiceContractRenewal'])
                                    if endOfServiceContractRenewal == 'None':
                                        endOfServiceContractRenewal = ''
                                    ldosDate = str(item['ldosDate'])
                                    if ldosDate == 'None':
                                        ldosDate = ''
                                    connectionStatus = str(item['connectionStatus'])
                                    try:
                                        managedBy = str(item['managedBy'])
                                    except KeyError:
                                        managedBy = 'N/A'
                                        pass
                                    contractNumber = str(item['contractNumber'])
                                    coverageEndDate = str(item['coverageEndDate'])
                                    if coverageEndDate == 'None':
                                        coverageEndDate = ''
                                    coverageStartDate = str(item['coverageStartDate'])
                                    if coverageStartDate == 'None':
                                        coverageStartDate = ''
                                    supportType = str(item['supportType'])
                                    advisories = str(item['advisories'])
                                    assetType = str(item['assetType'])
                                    try:
                                        criticalSecurityAdvisories = str(item['criticalSecurityAdvisories'])
                                    except KeyError:
                                        criticalSecurityAdvisories = ''
                                        pass
                                    try:
                                        addressLine1 = str(item['addressLine1']).replace(',', ' ')
                                    except KeyError:
                                        addressLine1 = ''
                                        pass
                                    try:
                                        addressLine2 = str(item['addressLine2']).replace(',', ' ')
                                    except KeyError:
                                        addressLine2 = ''
                                        pass
                                    try:
                                        addressLine3 = str(item['addressLine3']).replace(',', ' ')
                                    except KeyError:
                                        addressLine3 = ''
                                        pass
                                    try:
                                        licenseStatus = str(item['licenseStatus'])
                                    except KeyError:
                                        licenseStatus = ''
                                        pass
                                    try:
                                        licenseLevel = str(item['licenseLevel'])
                                    except KeyError:
                                        licenseLevel = ''
                                        pass
                                    try:
                                        profileName = str(item['profileName']).replace(',', ' ')
                                    except KeyError:
                                        profileName = ''
                                        pass
                                    try:
                                        hclStatus = str(item['hclStatus'])
                                    except KeyError:
                                        hclStatus = ''
                                        pass
                                    try:
                                        ucsDomain = str(item['ucsDomain'])
                                    except KeyError:
                                        ucsDomain = ''
                                        pass
                                    try:
                                        hxCluster = str(item['hxCluster'])
                                    except KeyError:
                                        hxCluster = ''
                                        pass
                                    try:
                                        subscriptionId = str(item['subscriptionId'])
                                    except KeyError:
                                        subscriptionId = ''
                                        pass
                                    try:
                                        subscriptionStartDate = str(item['subscriptionStartDate'])
                                        if subscriptionStartDate == 'None':
                                            subscriptionStartDate = ''
                                    except KeyError:
                                        subscriptionStartDate = ''
                                        pass
                                    try:
                                        subscriptionEndDate = str(item['subscriptionEndDate'])
                                        if subscriptionEndDate == 'None':
                                            subscriptionEndDate = ''
                                    except KeyError:
                                        subscriptionEndDate = ''
                                        pass
                                    for track in successTrack:
                                        successTrackId = track['id']
                                        useCases = track['useCases']
                                        for useCase in useCases:
                                            if outputFormat == 1 or outputFormat == 3:
                                                with open(assets, 'a', encoding='utf-8', newline='') \
                                                        as report_target:
                                                    writer = csv.writer(report_target,
                                                                        delimiter=' ',
                                                                        quotechar=' ',
                                                                        quoting=csv.QUOTE_MINIMAL)
                                                    CSV_Data = f'{customerId},{successTrackId},{useCase},' \
                                                               f'{assetId},{assetName},{productFamily},' \
                                                               f'{productType},{serialNumber},{productId},' \
                                                               f'{ipAddress},{productDescription},{softwareType},' \
                                                               f'{softwareRelease},{role},{assetLocation},' \
                                                               f'{coverageStatus},{lastScan},' \
                                                               f'{endOfLifeAnnounced},{endOfSale},{lastShip},' \
                                                               f'{endOfRoutineFailureAnalysis},' \
                                                               f'{endOfNewServiceAttach},' \
                                                               f'{endOfServiceContractRenewal},{ldosDate},' \
                                                               f'{connectionStatus},{managedBy},{contractNumber},' \
                                                               f'{coverageEndDate},{coverageStartDate},' \
                                                               f'{supportType},{advisories},{assetType},' \
                                                               f'{criticalSecurityAdvisories},{addressLine1},' \
                                                               f'{addressLine2},{addressLine3},{licenseStatus},' \
                                                               f'{licenseLevel},{profileName},{hclStatus},' \
                                                               f'{ucsDomain},{hxCluster},{subscriptionId},' \
                                                               f'{subscriptionStartDate},{subscriptionEndDate}'
                                                    writer.writerow(CSV_Data.split())
                                                    tRecords += 1
                                logging.debug('Successfully written record')
                            else:
                                logging.debug(f'No Asset Data Found for Success Track {successTrackId}')
                        else:
                            logging.debug(f'No Asset Data Found for Success Track {successTrackId} '
                                          f'Skipping this record and continuing....')
                        if outputFormat == 1 or outputFormat == 2:
                            logging.debug(f'Saving {json_output_dir}{customerId}_Assets_{json_filename}')
                            shutil.copy(input_json_file, json_output_dir)
            else:
                logging.debug(f'No Asset Data Found for Success Track {successTrackId} '
                              f'Skipping this record and continuing....')
    logging.debug(f'Total Asset records {tRecords}')
    print(f'Total Asset records {tRecords}')


def pxc_hardware_reports():
    # Function to request, download and process Hardware report data from PX Cloud
    # The Hardware report will provide hardware inventory details for a specific device and customer.
    # CSV Naming Convention: Hardware.csv
    # JSON Naming Convention: {Customer ID}_Hardware_{UniqueReportID}.json
    logging.debug('********************** Running Hardware Report ***********************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Hardware Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(hardware, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,successTrackId,useCaseId,assetId,hwInstanceId,assetName,productFamily,productId,' \
                         'serialNumber,endOfLifeAnnounced,endOfSale,lastShip,endOfRoutineFailureAnalysis,' \
                         'endOfNewServiceAttach,endOfServiceContractRenewal,ldosDate,coverageEndDate,' \
                         'coverageStartDate,contractNumber,coverageStatus'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        count = 0
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            successTrackId = row['successTrackId']
            customerName = row['customerName']
            if successTrackId in STList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}/reports'
                hardware_payload = json.dumps({"reportName": "Hardware", "successTrackId": successTrackId})
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                max_hardware_attempts = 10
                hardware_attempts = 0
                logging.debug(f'Requesting Hardware data for {customerName} on Success Track ID {successTrackId}')
                while hardware_attempts < max_hardware_attempts:
                    # Make a request to API
                    response = requests.request('POST', url, headers=headers, data=hardware_payload,
                                                verify=True, timeout=apiTimeout)
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        logging.debug(f'Querying with {hardware_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if hardware_attempts == 0:
                            logging.debug(f'Successfuly posted POST request for {hardware_payload}')
                        break
                    # If rate limited, wait and try again
                    logging.debug(f'Rate Limit Exceeded for Hardware Report! Retrying...')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    hardware_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if hardware_attempts > 0:
                    logging.debug(f'Retry was successful for Hardware Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                if response.text.__contains__('Customer admin has not provided access.'):
                    logging.debug(f'{customerName} admin has not provided access for {successTrackId}')
                if not (response.text.__contains__('Customer admin has not provided access.')):
                    try:
                        if bool(response.headers['location']) is True:
                            pass
                        else:
                            raise KeyError
                    except KeyError:
                        logging.debug(f'\nException Thrown for Customer:{customerId} on Success Track {successTrackId} '
                                      f'\n{response} Headers:{headers} in function pxc_hardware_reports')
                        while response.status_code >= 400:
                            logging.debug(f'Pausing for {wait_time} seconds before re-request...')
                            time.sleep(wait_time)
                            logging.debug('Resuming...')
                            logging.debug('Making API Call again with the following...')
                            logging.debug(f'URL: {url}')
                            logging.debug(f'Data Payload:{hardware_payload}')
                            response = requests.request('POST', url, headers=headers, data=hardware_payload,
                                                        verify=True, timeout=apiTimeout)
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(f'Review for  {customerName} on Success Track {successTrackId}'
                                          f' Report Failed to Download')
                    location = response.headers['location']
                    count += 1
                    logging.debug(f'Hardware {count}:{location}')
                    location_ready_status(location, headers, 'Hardware')
                    json_filename = (location.split('/')[-1] + '.json')
                    tries = 1
                    while True:
                        # time.sleep(tries)
                        logging.debug('Scanning for data...')
                        filename = (temp_dir + customerId + '_Hardware_' + location.split('/')[-1] + '.zip')
                        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                        max_hardware_attempts = 10
                        hardware_attempts = 0
                        logging.debug(f'Requesting Hardware data for {customerName} on '
                                      f'Success Track ID {successTrackId}')
                        while hardware_attempts < max_hardware_attempts:
                            # Make a request to API
                            response = requests.request('GET', location, headers=headers,
                                                        verify=True, timeout=apiTimeout)
                            # If not rate limited, break out of while loop and continue
                            if response.status_code != 429:
                                logging.debug(f'Querying:')
                                logging.debug(f'URL Request: {url}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if hardware_attempts == 0:
                                    logging.debug(f'Successfuly posted GET request for {hardware_payload}')
                                break
                            # If rate limited, wait and try again
                            logging.debug(f'Rate Limit Exceeded for Hardware Report! Retrying...')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            hardware_attempts += 1
                            pause = random.random()
                            logging.debug(f'sleeping for {pause} seconds')
                            time.sleep(pause)
                        if hardware_attempts > 0:
                            logging.debug(f'Retry was successful for Hardware Report')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Location Request: {location}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Wait Time:{tries}')
                        try:
                            with open(filename, 'wb') as file:
                                file.write(response.content)
                            with open(filename, 'rb') as file:
                                if file.read() and response.status_code == 200:
                                    with zipfile.ZipFile(filename, 'r') as zip_file:
                                        zip_file.extractall(temp_dir)
                                        if tries >= 2:
                                            if debug_level == 2:
                                                logging.debug(f'Success on retry!')
                                else:
                                    logging.debug('Download Failed')
                                    raise Exception('File Corrupted')
                        except Exception as FileCorruptError:
                            logging.debug(f'Deleting file...{filename} - {FileCorruptError} - Retrying....')
                            logging.debug(f'Pausing for {wait_time} seconds before retrying...')
                            time.sleep(wait_time * tries)  # increase the wait with the number of retries
                            if tries >= 3:
                                break
                        else:
                            break
                        finally:
                            tries += 1
                    if useProductionURL == 0:
                        proccess_sandbox_files(filename, customerId, '_Hardware_', json_filename)
                    input_json_file = str(temp_dir + customerId + '_Hardware_' + json_filename)
                    logging.debug(f'Reading file:{input_json_file}')
                    if not os.path.isfile(input_json_file):
                        logging.debug('File Not Found, Skipping.....')
                    if os.path.isfile(input_json_file):
                        with open(input_json_file, 'r', encoding='utf-8') as inputTarget:
                            data = json.load(inputTarget)
                            customerId = data['metadata']['customerId']
                            totalCount = data['metadata']['totalCount']
                            items = data['items']
                        if outputFormat == 1 or outputFormat == 3:
                            if totalCount:
                                logging.debug(f'Found Hardware Data on Success Track {successTrackId}')
                                for item in items:
                                    assetId = str(item['assetId']).replace(',', '_')
                                    hwInstanceId = str(item['hwInstanceId']).replace(',', ' ')
                                    assetName = str(item['assetName'])
                                    productFamily = str(item['productFamily']).replace(',', ' ')
                                    productId = str(item['productId'])
                                    serialNumber = str(item['serialNumber'])
                                    successTrack = item['successTrack']
                                    endOfLifeAnnounced = str(item['endOfLifeAnnounced'])
                                    if endOfLifeAnnounced == 'None':
                                        endOfLifeAnnounced = ''
                                    endOfSale = str(item['endOfSale'])
                                    if endOfSale == 'None':
                                        endOfSale = ''
                                    lastShip = str(item['lastShip'])
                                    if lastShip == 'None':
                                        lastShip = ''
                                    endOfRoutineFailureAnalysis = str(item['endOfRoutineFailureAnalysis'])
                                    if endOfRoutineFailureAnalysis == 'None':
                                        endOfRoutineFailureAnalysis = ''
                                    endOfNewServiceAttach = str(item['endOfNewServiceAttach'])
                                    if endOfNewServiceAttach == 'None':
                                        endOfNewServiceAttach = ''
                                    endOfServiceContractRenewal = str(item['endOfServiceContractRenewal'])
                                    if endOfServiceContractRenewal == 'None':
                                        endOfServiceContractRenewal = ''
                                    ldosDate = str(item['ldosDate'])
                                    if ldosDate == 'None':
                                        ldosDate = ''
                                    coverageEndDate = str(item['coverageEndDate'])
                                    if coverageEndDate == 'None':
                                        coverageEndDate = ''
                                    coverageStartDate = str(item['coverageStartDate'])
                                    if coverageStartDate == 'None':
                                        coverageStartDate = ''
                                    contractNumber = str(item['contractNumber'])
                                    if contractNumber == 'None':
                                        contractNumber = ''
                                    coverageStatus = str(item['coverageStatus'])
                                    for track in successTrack:
                                        successTracksId = track['id']
                                        useCases = track['useCases']
                                        for useCase in useCases:
                                            if outputFormat == 1 or outputFormat == 3:
                                                with open(hardware, 'a', encoding='utf-8', newline='') as report_target:
                                                    writer = csv.writer(report_target,
                                                                        delimiter=' ',
                                                                        quotechar=' ',
                                                                        quoting=csv.QUOTE_MINIMAL)
                                                    CSV_Data = f'{customerId},{successTracksId},{useCase},{assetId},' \
                                                               f'{hwInstanceId},{assetName},{productFamily},' \
                                                               f'{productId},{serialNumber},{endOfLifeAnnounced},' \
                                                               f'{endOfSale},{lastShip},' \
                                                               f'{endOfRoutineFailureAnalysis},' \
                                                               f'{endOfNewServiceAttach},' \
                                                               f'{endOfServiceContractRenewal},' \
                                                               f'{ldosDate},{coverageEndDate},{coverageStartDate},' \
                                                               f'{contractNumber},{coverageStatus}'
                                                    writer.writerow(CSV_Data.split())
                                                    tRecords += 1
                                logging.debug('Successfully written record')
                            else:
                                logging.debug(f'No Hardware Data Found for Success Track {successTrackId}')
                        else:
                            logging.debug(f'No Hardware Data Found for Success Track {successTrackId} '
                                          f'Skipping this record and continuing....')
                        if outputFormat == 1 or outputFormat == 2:
                            if totalCount > 0:
                                logging.debug(f'Saving {json_output_dir}{customerId}_Hardware_{json_filename}')
                                shutil.copy(input_json_file, json_output_dir)
            else:
                logging.debug(f'No Hardware Data Found for Success Track {successTrackId} '
                              f'Skipping this record and continuing....')
    logging.debug(f'Total Hardware records {tRecords}')
    print(f'Total Hardware records {tRecords}')


def pxc_software_reports():
    # Function to request, download and process Software report data from PX Cloud
    # Software report will provide details about the software installed on a device.
    # CSV Naming Convention: Software.csv
    # JSON Naming Convention: {Customer ID}_Software_{UniqueReportID}.json
    logging.debug('*********************** Running Software Report **********************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Software Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(software, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,successTrackId,useCaseId,assetName,assetId,productId,softwareType,' \
                         'softwareRelease,endOfLifeAnnounced,endOfSoftwareMaintenance,endOfSale,lastShip,' \
                         'endOfVulnerabilitySecuritySupport,ldosDate'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        count = 0
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            successTrackId = row['successTrackId']
            customerName = row['customerName']
            if successTrackId in STList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}/reports'
                software_payload = json.dumps({"reportName": "Software", "successTrackId": successTrackId})
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                max_software_attempts = 10
                software_attempts = 0
                logging.debug(f'Requesting Software data for {customerName} on Success Track ID {successTrackId}')
                while software_attempts < max_software_attempts:
                    # Make a request to API
                    response = requests.request('POST', url, headers=headers, data=software_payload,
                                                verify=True, timeout=apiTimeout)
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        logging.debug(f'Querying with {software_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if software_attempts == 0:
                            logging.debug(f'Successfuly posted POST request for {software_payload}')
                        break
                    # If rate limited, wait and try again
                    logging.debug(f'Rate Limit Exceeded for Software Report! Retrying...')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    software_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if software_attempts > 0:
                    logging.debug(f'Retry was successful for Software Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                if response.text.__contains__('Customer admin has not provided access.'):
                    logging.debug(f'{customerName} admin has not provided access on Success Track {successTrackId}')
                if not (response.text.__contains__('Customer admin has not provided access.')):
                    try:
                        if bool(response.headers['location']) is True:
                            pass
                        else:
                            raise KeyError
                    except KeyError:
                        logging.debug(f'\nException Thrown for Customer:{customerId} on Success Track {successTrackId} '
                                      f'\n{response} Headers:{headers} in function pxc_software_reports')
                        while response.status_code >= 400:
                            logging.debug(f'Pausing for {wait_time} seconds before re-request...')
                            time.sleep(wait_time)
                            logging.debug('Resuming...')
                            logging.debug('Making API Call again with the following...')
                            logging.debug(f'URL: {url}')
                            logging.debug(f'Data Payload:{software_payload}')
                            response = requests.request('POST', url, headers=headers, data=software_payload,
                                                        verify=True, timeout=apiTimeout)
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(f'Review for  {customerName} on Success Track {successTrackId}'
                                          f' Report Failed to Download')
                    location = response.headers['location']
                    count += 1
                    logging.debug(f'Software {count}:{location}')
                    location_ready_status(location, headers, 'Software')
                    json_filename = (location.split('/')[-1] + '.json')
                    tries = 1
                    while True:
                        # time.sleep(tries)
                        logging.debug('Scanning for data...')
                        filename = (temp_dir + customerId + '_Software_' + location.split('/')[-1] + '.zip')
                        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                        max_software_attempts = 10
                        software_attempts = 0
                        logging.debug(f'Requesting Software data for {customerName} on '
                                      f'Success Track ID {successTrackId}')
                        while software_attempts < max_software_attempts:
                            # Make a request to API
                            response = requests.request('GET', location, headers=headers,
                                                        verify=True, timeout=apiTimeout)
                            # If not rate limited, break out of while loop and continue
                            if response.status_code != 429:
                                logging.debug(f'Querying:')
                                logging.debug(f'URL Request: {url}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if software_attempts == 0:
                                    logging.debug(f'Successfuly posted GET request for {software_payload}')
                                break
                            # If rate limited, wait and try again
                            logging.debug(f'Rate Limit Exceeded for Software Report! Retrying...')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            software_attempts += 1
                            pause = random.random()
                            logging.debug(f'sleeping for {pause} seconds')
                            time.sleep(pause)
                        if software_attempts > 0:
                            logging.debug(f'Retry was successful for Software Report')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Location Request: {location}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Wait Time:{tries}')
                        try:
                            with open(filename, 'wb') as file:
                                file.write(response.content)
                            with open(filename, 'rb') as file:
                                if file.read() and response.status_code == 200:
                                    with zipfile.ZipFile(filename, 'r') as zip_file:
                                        zip_file.extractall(temp_dir)
                                        if tries >= 2:
                                            if debug_level == 2:
                                                logging.debug(f'Success on retry!')
                                else:
                                    logging.debug('Download Failed')
                                    raise Exception('File Corrupted')
                        except Exception as FileCorruptError:
                            logging.debug(f'Deleting file...{filename} - {FileCorruptError} - Retrying....')
                            logging.debug(f'Pausing for {wait_time} seconds before retrying...')
                            time.sleep(wait_time * tries)  # increase the wait with the number of retries
                            if tries >= 3:
                                break
                        else:
                            break
                        finally:
                            tries += 1
                    if useProductionURL == 0:
                        proccess_sandbox_files(filename, customerId, '_Software_', json_filename)
                    input_json_file = str(temp_dir + customerId + '_Software_' + json_filename)
                    logging.debug(f'Reading file:{input_json_file}')
                    if not os.path.isfile(input_json_file):
                        logging.debug('File Not Found, Skipping.....')
                    if os.path.isfile(input_json_file):
                        with open(input_json_file, 'r', encoding='utf-8') as inputTarget:
                            data = json.load(inputTarget)
                            customerId = data['metadata']['customerId']
                            totalCount = data['metadata']['totalCount']
                            items = data['items']
                        if outputFormat == 1 or outputFormat == 3:
                            if totalCount:
                                logging.debug(f'Found Software Data on Success Track {successTrackId}')
                                for item in items:
                                    assetName = str(item['assetName'])
                                    assetId = str(item['assetId']).replace(',', '_')
                                    productId = str(item['productId'])
                                    softwareType = str(item['softwareType']).replace(',', ' ')
                                    softwareRelease = str(item['softwareRelease'])
                                    successTrack = item['successTrack']
                                    endOfLifeAnnounced = str(item['endOfLifeAnnounced'])
                                    if endOfLifeAnnounced == 'None':
                                        endOfLifeAnnounced = ''
                                    endOfSoftwareMaintenance = str(item['endOfSoftwareMaintenance'])
                                    if endOfSoftwareMaintenance == 'None':
                                        endOfSoftwareMaintenance = ''
                                    endOfSale = str(item['endOfSale'])
                                    if endOfSale == 'None':
                                        endOfSale = ''
                                    lastShip = str(item['lastShip'])
                                    if lastShip == 'None':
                                        lastShip = ''
                                    endOfVulnerabilitySecuritySupport = str(item['endOfVulnerabilitySecuritySupport'])
                                    if endOfVulnerabilitySecuritySupport == 'None':
                                        endOfVulnerabilitySecuritySupport = ''
                                    ldosDate = str(item['ldosDate'])
                                    if ldosDate == 'None':
                                        ldosDate = ''
                                    for track in successTrack:
                                        successTrackId = track['id']
                                        useCases = track['useCases']
                                        for useCase in useCases:
                                            if outputFormat == 1 or outputFormat == 3:
                                                with open(software, 'a', encoding='utf-8', newline='') as report_target:
                                                    writer = csv.writer(report_target,
                                                                        delimiter=' ',
                                                                        quotechar=' ',
                                                                        quoting=csv.QUOTE_MINIMAL)
                                                    CSV_Data = f'{customerId},{successTrackId},{useCase},{assetName},' \
                                                               f'{assetId},{productId},{softwareType},' \
                                                               f'{softwareRelease},{endOfLifeAnnounced},' \
                                                               f'{endOfSoftwareMaintenance},{endOfSale},{lastShip},' \
                                                               f'{endOfVulnerabilitySecuritySupport},{ldosDate}'
                                                    writer.writerow(CSV_Data.split())
                                                    tRecords += 1
                                logging.debug('Successfully written record')
                            else:
                                logging.debug(f'No Software Data Found for Success Track {successTrackId}')
                        else:
                            logging.debug(f'No Software Data Found for Success Track {successTrackId} '
                                          f'Skipping this record and continuing....')
                        if outputFormat == 1 or outputFormat == 2:
                            logging.debug(f'Saving {json_output_dir}{customerId}_Software_{json_filename}')
                            shutil.copy(input_json_file, json_output_dir)
            else:
                logging.debug(f'No Software Data Found for Success Track {successTrackId} '
                              f'Skipping this record and continuing....')
    logging.debug(f'Total Software records {tRecords}')
    print(f'Total Software records {tRecords}')


def pxc_licenses_reports():
    # Function to request, download and process Licenses report data from PX Cloud
    # It provides License details along with related asset information.
    # CSV Naming Convention: Licenses.csv
    # JSON Naming Convention: {Customer ID}_Licenses_{UniqueReportID}.json
    logging.debug('************************ Running License Report **********************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running License Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(licenses, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,successTrackId,useCaseId,assetName,assetId,productFamily,productType,' \
                         'connectionStatus,productDescription,licenseId,licenseStartDate,licenseEndDate,' \
                         'contractNumber,subscriptionId,supportType'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        count = 0
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            successTrackId = row['successTrackId']
            customerName = row['customerName']
            if successTrackId in STList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}/reports'
                licenses_payload = json.dumps({"reportName": "Licenses", "successTrackId": successTrackId})
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                max_license_attempts = 10
                license_attempts = 0
                logging.debug(f'Requesting License data for {customerName} on Success Track ID {successTrackId}')
                while license_attempts < max_license_attempts:
                    # Make a request to API
                    response = requests.request('POST', url, headers=headers, data=licenses_payload,
                                                verify=True, timeout=apiTimeout)
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        logging.debug(f'Querying with {licenses_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if license_attempts == 0:
                            logging.debug(f'Successfuly posted POST request for {licenses_payload}')
                        break
                    # If rate limited, wait and try again
                    logging.debug(f'Rate Limit Exceeded for License Report! Retrying...')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    license_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if license_attempts > 0:
                    logging.debug(f'Retry was successful for License Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                if response.text.__contains__('Customer admin has not provided access.'):
                    logging.debug(f'{customerName} admin has not provided access for {successTrackId}')
                if not (response.text.__contains__('Customer admin has not provided access.')):
                    try:
                        if bool(response.headers['location']) is True:
                            pass
                        else:
                            raise KeyError
                    except KeyError:
                        logging.debug(f'\nException Thrown for Customer:{customerId} on Success Track {successTrackId} '
                                      f'\n{response} Headers:{headers} in function pxc_licenses_reports')
                        while response.status_code >= 400:
                            logging.debug(f'Pausing for {wait_time} seconds before re-request...')
                            time.sleep(wait_time)
                            logging.debug('Resuming...')
                            logging.debug('Making API Call again with the following...')
                            logging.debug(f'URL: {url}')
                            logging.debug(f'Data Payload:{licenses_payload}')
                            response = requests.request('POST', url, headers=headers, data=licenses_payload,
                                                        verify=True, timeout=apiTimeout)
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(f'Review for  {customerName} on Success Track {successTrackId}'
                                          f' Report Failed to Download')
                    location = response.headers['location']
                    count += 1
                    logging.debug(f'Licenses {count}:{location}')
                    location_ready_status(location, headers, 'Licenses')
                    json_filename = (location.split('/')[-1] + '.json')
                    tries = 1
                    while True:
                        # time.sleep(tries)
                        filename = (temp_dir + customerId + '_Licenses_' + location.split('/')[-1] + '.zip')
                        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                        max_license_attempts = 10
                        license_attempts = 0
                        logging.debug(f'Requesting Licenses data for {customerName} on '
                                      f'Success Track ID {successTrackId}')
                        while license_attempts < max_license_attempts:
                            # Make a request to API
                            response = requests.request('GET', location, headers=headers,
                                                        verify=True, timeout=apiTimeout)
                            # If not rate limited, break out of while loop and continue
                            if response.status_code != 429:
                                if debug_level > 0:
                                    logging.debug(f'Querying:')
                                    logging.debug(f'URL Request: {url}')
                                    logging.debug(f'HTTP Code:{response.status_code}')
                                    logging.debug(f'Review API Headers:{response.headers}')
                                    logging.debug(f'Response Body:{response.content}')
                                    if license_attempts == 0:
                                        logging.debug(f'Successfuly posted GET request for {licenses_payload}')
                                break
                            # If rate limited, wait and try again
                            logging.debug(f'Rate Limit Exceeded for License Report! Retrying...')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            license_attempts += 1
                            pause = random.random()
                            logging.debug(f'sleeping for {pause} seconds')
                            time.sleep(pause)
                        if license_attempts > 0:
                            logging.debug(f'Retry was successful for License Report')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Location Request: {location}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Wait Time:{tries}')
                        try:
                            with open(filename, 'wb') as file:
                                file.write(response.content)
                            with open(filename, 'rb') as file:
                                if file.read() and response.status_code == 200:
                                    with zipfile.ZipFile(filename, 'r') as zip_file:
                                        zip_file.extractall(temp_dir)
                                        if tries >= 2:
                                            if debug_level == 2:
                                                logging.debug(f'Success on retry!')
                                        else:
                                            logging.debug("File is good")
                                else:
                                    logging.debug('Download Failed')
                                    raise Exception('File Corrupted')
                        except Exception as FileCorruptError:
                            logging.debug(f'Deleting file...{filename} - {FileCorruptError} - Retrying....')
                            logging.debug(f'Pausing for {wait_time} seconds before retrying...')
                            time.sleep(wait_time * tries)  # increase the wait with the number of retries
                            if tries >= 3:
                                break
                        else:
                            break
                        finally:
                            tries += 1
                    if useProductionURL == 0:
                        proccess_sandbox_files(filename, customerId, '_Licenses_', json_filename)
                    input_json_file = str(temp_dir + customerId + '_Licenses_' + json_filename)
                    logging.debug(f'Reading file:{input_json_file}')
                    if not os.path.isfile(input_json_file):
                        logging.debug('File Not Found, Skipping.....')
                    if os.path.isfile(input_json_file):
                        with open(input_json_file, 'r', encoding='utf-8') as inputTarget:
                            data = json.load(inputTarget)
                            customerId = data['metadata']['customerId']
                            totalCount = data['metadata']['totalCount']
                            items = data['items']
                        if outputFormat == 1 or outputFormat == 3:
                            if totalCount:
                                logging.debug(f'Found License Data on Success Track {successTrackId}')
                                for item in items:
                                    assetId = str(item['assetId']).replace(',', '_')
                                    assetName = str(item['assetName']).replace(',', ' ')
                                    productFamily = str(item['productFamily']).replace(',', ' ')
                                    productType = str(item['productType']).replace(',', ' ')
                                    connectionStatus = str(item['connectionStatus'])
                                    productDescription = str(item['productDescription']).replace(',', ' ')
                                    licenseId = str(item['license'])
                                    try:
                                        licenseStartDate = str(item['licenseStartDate'])
                                        if licenseStartDate == 'None':
                                            licenseStartDate = ''
                                    except KeyError:
                                        licenseStartDate = ''
                                        pass
                                    try:
                                        licenseEndDate = str(item['licenseEndDate'])
                                        if licenseEndDate == 'None':
                                            licenseEndDate = ''
                                    except KeyError:
                                        licenseEndDate = ''
                                        pass
                                    try:
                                        contractNumber = str(item['contractNumber'])
                                        if contractNumber == 'None':
                                            contractNumber = ''
                                    except KeyError:
                                        contractNumber = ''
                                        pass
                                    subscriptionId = str(item['subscriptionId'])
                                    supportType = str(item['supportType']).replace(',', ' ')
                                    successTrack = item['successTrack']
                                    for track in successTrack:
                                        successTracksId = track['id']
                                        useCases = track['useCases']
                                        for useCase in useCases:
                                            useCaseId = useCase
                                            if outputFormat == 1 or outputFormat == 3:
                                                with open(licenses, 'a', encoding='utf-8', newline='') as report_target:
                                                    writer = csv.writer(report_target,
                                                                        delimiter=' ',
                                                                        quotechar=' ',
                                                                        quoting=csv.QUOTE_MINIMAL)
                                                    CSV_Data = f'{customerId},{successTracksId},{useCaseId},' \
                                                               f'{assetName},{assetId},{productFamily},{productType},' \
                                                               f'{connectionStatus},{productDescription},{licenseId},' \
                                                               f'{licenseStartDate},{licenseEndDate},' \
                                                               f'{contractNumber},{subscriptionId},{supportType}'
                                                    writer.writerow(CSV_Data.split())
                                                    tRecords += 1
                                logging.debug('Successfully written record')
                            else:
                                logging.debug(f'No License Data Found For Success Track {successTrackId}')
                        else:
                            logging.debug(f'No License Data Found For Success Track {successTrackId}'
                                          f'Skipping this record and continuing....')
                        if outputFormat == 1 or outputFormat == 2:
                            if totalCount > 0:
                                logging.debug(f'Saving {json_output_dir}{customerId}_Licenses_{json_filename}')
                                shutil.copy(input_json_file, json_output_dir)
            else:
                logging.debug(f'No License Data Found For Success Track {successTrackId}'
                              f'Skipping this record and continuing....')
    logging.debug(f'Total Licenses records {tRecords}')
    print(f'Total Licenses records {tRecords}')


def pxc_purchased_licenses_reports():
    # Function to request, download and process Purchased Licenses report data from PX Cloud
    # This report provide details of all the licenses.
    # CSV Naming Convention: Purchased_Licenses.csv
    # JSON Naming Convention: {Customer ID}_Purchased_Licenses_{UniqueReportID}.json
    logging.debug('********************** Running Purchased Licenses ********************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Purchased Licenses Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(purchasedLicenses, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,successTrackId,useCaseId,licenseId,licenseLevel,purchasedQuantity,' \
                         'productFamily,licenseStartDate,licenseEndDate,contractNumber'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        count = 0
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            successTrackId = row['successTrackId']
            customerName = row['customerName']
            if successTrackId in STList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}/reports'
                purchased_licenses_payload = json.dumps(
                    {"reportName": "PurchasedLicenses", "successTrackId": successTrackId})
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                max_purchased_license_attempts = 10
                purchased_license_attempts = 0
                logging.debug(f'Requesting Purchased Licenses data for {customerName} '
                              f'on Success Track ID {successTrackId}')
                while purchased_license_attempts < max_purchased_license_attempts:
                    # Make a request to API
                    response = requests.request('POST', url, headers=headers, data=purchased_licenses_payload,
                                                verify=True, timeout=apiTimeout)
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        logging.debug(f'Querying with {purchased_licenses_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if purchased_license_attempts == 0:
                            logging.debug(f'Successfuly posted POST request for {purchased_licenses_payload}')
                        break
                    # If rate limited, wait and try again
                    logging.debug(f'Rate Limit Exceeded for Purchased Licenses Report! Retrying...')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    purchased_license_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if purchased_license_attempts > 0:
                    logging.debug(f'Retry was successful for Purchased Licenses Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                if response.text.__contains__('Customer admin has not provided access.'):
                    logging.debug(f'{customerName} admin has not provided access for {successTrackId}')
                if not (response.text.__contains__('Customer admin has not provided access.')):
                    try:
                        if bool(response.headers['location']) is True:
                            pass
                        else:
                            raise KeyError
                    except KeyError:
                        logging.debug(f'\nException Thrown for Customer:{customerId} on Success Track {successTrackId} '
                                      f'\n{response} Headers:{headers} in function pxc_purchased_licenses_reports')
                        while response.status_code >= 400:
                            logging.debug(f'Pausing for {wait_time} seconds before re-request...')
                            time.sleep(wait_time)
                            logging.debug('Resuming...')
                            logging.debug('Making API Call again with the following...')
                            logging.debug(f'URL: {url}')
                            logging.debug(f'Data Payload:{data_payload}')
                            response = requests.request('POST', url, headers=headers, data=purchased_licenses_payload,
                                                        verify=True, timeout=apiTimeout)
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(f'Review for  {customerName} on Success Track {successTrackId}'
                                          f' Report Failed to Download')
                    location = response.headers['location']
                    count += 1
                    logging.debug(f'Purchased Licenses {count}:{location}')
                    location_ready_status(location, headers, 'Purchased Licenses')
                    json_filename = (location.split('/')[-1] + '.json')
                    tries = 1
                    while True:
                        # time.sleep(tries)
                        filename = (temp_dir + customerId + '_Purchased_Licenses_' + location.split('/')[-1] + '.zip')
                        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                        max_purchased_license_attempts = 10
                        purchased_license_attempts = 0
                        logging.debug(f'Requesting Asset data for {customerName} on Success Track ID {successTrackId}')
                        while purchased_license_attempts < max_purchased_license_attempts:
                            # Make a request to API
                            response = requests.request('GET', location, headers=headers,
                                                        verify=True, timeout=apiTimeout)
                            # If not rate limited, break out of while loop and continue
                            if response.status_code != 429:
                                logging.debug(f'Querying:')
                                logging.debug(f'URL Request: {url}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if purchased_license_attempts == 0:
                                    logging.debug(f'Successfuly posted GET request for {purchased_licenses_payload}')
                                break
                                # If rate limited, wait and try again
                            # If rate limited, wait and try again
                            logging.debug(f'Rate Limit Exceeded for Purchased Licenses Report! Retrying...')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            purchased_license_attempts += 1
                            pause = random.random()
                            logging.debug(f'sleeping for {pause} seconds')
                            time.sleep(pause)
                        if purchased_license_attempts > 0:
                            logging.debug(f'Retry was successful for Purchased Licenses Report')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Location Request: {location}')
                        logging.debug(f'Report details:{data_payload}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Wait Time:{tries}')
                        try:
                            with open(filename, 'wb') as file:
                                file.write(response.content)
                            with open(filename, 'rb') as file:
                                if file.read() and response.status_code == 200:
                                    with zipfile.ZipFile(filename, 'r') as zip_file:
                                        zip_file.extractall(temp_dir)
                                        if tries >= 2:
                                            if debug_level == 2:
                                                logging.debug('Success on retry!')
                                else:
                                    logging.debug('Download Failed')
                                    raise Exception('File Corrupted')
                        except Exception as FileCorruptError:
                            logging.debug(f'Deleting file...{filename} - {FileCorruptError} - Retrying....')
                            logging.debug(f'Pausing for {wait_time} seconds before retrying...')
                            time.sleep(wait_time * tries)  # increase the wait with the number of retries
                            if tries >= 3:
                                break
                        else:
                            break
                        finally:
                            tries += 1
                    if useProductionURL == 0:
                        proccess_sandbox_files(filename, customerId, '_PurchasedLicenses_', json_filename)
                    input_json_file = str(temp_dir + customerId + '_PurchasedLicenses_' + json_filename)
                    logging.debug(f'Reading file:{input_json_file}')
                    if not os.path.isfile(input_json_file):
                        logging.debug('File Not Found, Skipping.....')
                    if os.path.isfile(input_json_file):
                        with open(input_json_file, 'r', encoding='utf-8') as inputTarget:
                            data = json.load(inputTarget)
                            customerId = data['metadata']['customerId']
                            totalCount = data['metadata']['totalCount']
                            items = data['items']
                        if outputFormat == 1 or outputFormat == 3:
                            if totalCount:
                                logging.debug(f'Found Purchased Licenses Data on Success Track {successTrackId}')
                                for item in items:
                                    licenseId = str(item['license'])
                                    licenseLevel = str(item['licenseLevel'])
                                    purchasedQuantity = str(item['purchasedQuantity'])
                                    productFamily = str(item['productFamily'])
                                    licenseStartDate = str(item['licenseStartDate'])
                                    if licenseStartDate == 'None':
                                        licenseStartDate = ''
                                    licenseEndDate = str(item['licenseEndDate'])
                                    if licenseEndDate == 'None':
                                        licenseEndDate = ''
                                    contractNumber = str(item['contractNumber'])
                                    if contractNumber == 'None':
                                        contractNumber = ''
                                    successTrack = item['successTrack']
                                    for track in successTrack:
                                        successTracksId = track['id']
                                        useCases = track['useCases']
                                        for useCase in useCases:
                                            useCaseId = useCase
                                            if outputFormat == 1 or outputFormat == 3:
                                                with open(purchasedLicenses, 'a', encoding='utf-8', newline='') \
                                                        as report_target:
                                                    writer = csv.writer(report_target,
                                                                        delimiter=' ',
                                                                        quotechar=' ',
                                                                        quoting=csv.QUOTE_MINIMAL)
                                                    CSV_Data = f'{customerId},{successTracksId},{useCaseId},' \
                                                               f'{licenseId},{licenseLevel},{purchasedQuantity},' \
                                                               f'{productFamily},{licenseStartDate},{licenseEndDate},' \
                                                               f'{contractNumber}'
                                                    writer.writerow(CSV_Data.split())
                                                    tRecords += 1
                                logging.debug('Successfully written record')
                            else:
                                logging.debug(f'No Purchased Licenses Data Found For Success Track {successTrackId}')
                        else:
                            logging.debug(f'No Purchased Licenses Data Found For Success Track {successTrackId}'
                                          f'Skipping this record and continuing....')
                        if outputFormat == 1 or outputFormat == 2:
                            logging.debug(f'Saving {json_output_dir}{customerId}'
                                          f'_Purchased Licenses_{json_filename}')
                            shutil.copy(input_json_file, json_output_dir)
            else:
                logging.debug(f'No Purchased Licenses Data Found For Success Track {successTrackId}'
                              f'Skipping this record and continuing....')
    logging.debug(f'Total Purchased License records {tRecords}')
    print(f'Total Purchased License records {tRecords}')


def pxc_security_advisories_reports():
    # Function to request, download and process Security Advisories report data from PX Cloud
    # This API provides security vulnerability information including CVE and CVSS for devices associated with customer.
    # CSV Naming Convention: Security_Advisories.csv
    # JSON Naming Convention: {Customer ID}_SecurityAdvisories_{UniqueReportID}.json
    logging.debug('********************** Running Security Advisories *******************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Security Advisories Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(securityAdvisories, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,successTrackId,useCaseId,assetName,assetId,ipAddress,serialNumber,advisoryId,' \
                         'impact,cvss,version,cve,published,updated,advisory,summary,url,additionalNotes,' \
                         'affectedStatus,affectedReason,additionalVerificationNeeded,softwareRelease,productId'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        count = 0
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            successTrackId = row['successTrackId']
            customerName = row['customerName']
            if successTrackId in STList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}/reports'
                security_advisories_payload = json.dumps({"reportName": "SecurityAdvisories",
                                                          "successTrackId": successTrackId})
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                max_security_advisory_attempts = 10
                security_advisory_attempts = 0
                logging.debug(f'Requesting Security Advisory data for {customerName} '
                              f'on Success Track ID {successTrackId}')
                while security_advisory_attempts < max_security_advisory_attempts:
                    # Make a request to API
                    response = requests.request('POST', url, headers=headers, data=security_advisories_payload,
                                                verify=True, timeout=apiTimeout)
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        logging.debug(f'Querying with {security_advisories_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if security_advisory_attempts == 0:
                            logging.debug(f'Successfuly posted POST request for {security_advisories_payload}')
                        break
                    # If rate limited, wait and try again
                    logging.debug(f'Rate Limit Exceeded for Security Advisories Report! Retrying...')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    security_advisory_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if security_advisory_attempts > 0:
                    logging.debug(f'Retry was successful for Security Advisories Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                if response.text.__contains__('Customer admin has not provided access.'):
                    logging.debug(f'{customerName} admin has not provided access for {successTrackId}')
                if not (response.text.__contains__('Customer admin has not provided access.')):
                    try:
                        if bool(response.headers['location']) is True:
                            pass
                        else:
                            raise KeyError
                    except KeyError:
                        logging.debug(f'\nException Thrown for Customer:{customerId} on Success Track {successTrackId} '
                                      f'\n{response} Headers:{headers} in function pxc_security_advisories_reports')
                        while response.status_code >= 400:
                            logging.debug(f'Pausing for {wait_time} seconds before re-request...')
                            time.sleep(wait_time)
                            logging.debug('Resuming...')
                            logging.debug('Making API Call again with the following...')
                            response = requests.request('POST', url, headers=headers, data=security_advisories_payload,
                                                        verify=True, timeout=apiTimeout)
                            logging.debug(f'URL Request: {url}')
                            logging.debug(f'Report details:{security_advisories_payload}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(f'Review for  {customerName} on Success Track {successTrackId}'
                                          f' Report Failed to Download')
                    location = response.headers['location']
                    count += 1
                    logging.debug(f'Security Advisories {count}:{location}')
                    location_ready_status(location, headers, 'Security Advisories')
                    json_filename = (location.split('/')[-1] + '.json')
                    tries = 1
                    while True:
                        # time.sleep(tries)
                        filename = (temp_dir + customerId + '_Security_Advisories_' + location.split('/')[-1] + '.zip')
                        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                        max_security_advisory_attempts = 10
                        security_advisory_attempts = 0
                        while security_advisory_attempts < max_security_advisory_attempts:
                            # Make a request to API
                            response = requests.request('GET', location, headers=headers,
                                                        verify=True, timeout=apiTimeout)
                            # If not rate limited, break out of while loop and continue
                            if response.status_code != 429:
                                logging.debug(f'Querying:')
                                logging.debug(f'URL Request: {url}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if security_advisory_attempts == 0:
                                    logging.debug(f'Successfuly posted GET request for {security_advisories_payload}')
                                break
                            # If rate limited, wait and try again
                            logging.debug(f'Rate Limit Exceeded for Purchased Licenses Report! Retrying...')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            security_advisory_attempts += 1
                            pause = random.random()
                            logging.debug(f'sleeping for {pause} seconds')
                            time.sleep(pause)
                        if security_advisory_attempts > 0:
                            logging.debug(f'Retry was successful for Purchased Licenses Report')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Location Request: {location}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Wait Time:{tries}')
                        try:
                            with open(filename, 'wb') as file:
                                file.write(response.content)
                            with open(filename, 'rb') as file:
                                if file.read() and response.status_code == 200:
                                    with zipfile.ZipFile(filename, 'r') as zip_file:
                                        zip_file.extractall(temp_dir)
                                        if tries >= 2:
                                            if debug_level == 2:
                                                logging.debug('Success on retry!')
                                else:
                                    logging.debug('Download Failed :', filename)
                                    raise Exception('File Corrupted')
                        except Exception as FileCorruptError:
                            if debug_level == 2:
                                logging.debug(f'Deleting file...{filename} - {FileCorruptError} - Retrying....')
                                logging.debug(f'Pausing for {wait_time} seconds before retrying...')
                            time.sleep(wait_time * tries)  # increase the wait with the number of retries
                            if tries >= 3:
                                break
                        else:
                            break
                        finally:
                            tries += 1
                    if useProductionURL == 0:
                        proccess_sandbox_files(filename, customerId, '_SecurityAdvisories_', json_filename)
                    input_json_file = str(temp_dir + customerId + '_SecurityAdvisories_' + json_filename)
                    logging.debug(f'Reading file:{input_json_file}')
                    if not os.path.isfile(input_json_file):
                        logging.debug('File Not Found, Skipping.....')
                    if os.path.isfile(input_json_file):
                        with open(input_json_file, 'r', encoding='utf-8') as inputTarget:
                            data = json.load(inputTarget)
                            customerId = data['metadata']['customerId']
                            totalCount = data['metadata']['totalCount']
                            items = data['items']
                        if outputFormat == 1 or outputFormat == 3:
                            if totalCount:
                                logging.debug(f'Found Security Advisories on Success Track {successTrackId}')
                                for item in items:
                                    assetName = str(item['assetName'])
                                    assetId = str(item['assetId']).replace(',', '_')
                                    ipAddress = str(item['ipAddress'])
                                    serialNumber = str(item['serialNumber'])
                                    successTrack = item['successTrack']
                                    advisoryId = str(item['advisoryId']).replace(',', ' ')
                                    impact = str(item['impact']).replace(',', ' ')
                                    cvss = str(item['cvss']).replace(',', ' ')
                                    version = str(item['version'])
                                    cve = str(item['cve']).replace(',', ' ')
                                    published = str(item['published']).replace(',', ' ')
                                    updated = str(item['updated'])
                                    advisory = str(item['advisory']).replace(',', ' ')
                                    summary = str(item['summary']).replace(',', ' ')
                                    url = str(item['url']).replace(',', ' ')
                                    additionalNotes = str(item['additionalNotes']).replace(',', ' ')
                                    affectedStatus = str(item['affectedStatus'])
                                    affectedReasons = item['affectedReason']
                                    try:
                                        additionalVerificationNeeded = item['additionalVerificationNeeded']
                                    except KeyError:
                                        additionalVerificationNeeded = 'Not Available'
                                        pass
                                    softwareRelease = str(item['softwareRelease'])
                                    productId = str(item['productId'])
                                    for track in successTrack:
                                        successTrackId = track['id']
                                        useCases = track['useCases']
                                        for useCase in useCases:
                                            useCaseId = useCase
                                            for reason in affectedReasons:
                                                affectedReason = reason
                                                with open(securityAdvisories, 'a', encoding='utf-8', newline='') \
                                                        as report_target:
                                                    writer = csv.writer(report_target,
                                                                        delimiter=' ',
                                                                        quotechar=' ',
                                                                        quoting=csv.QUOTE_MINIMAL)
                                                    CSV_Data = f'{customerId},{successTrackId},{useCaseId},' \
                                                               f'{assetName},{assetId},{ipAddress},{serialNumber},' \
                                                               f'{advisoryId},{impact},{cvss},{version},{cve},' \
                                                               f'{published},{updated},{advisory},{summary},{url},' \
                                                               f'{additionalNotes},{affectedStatus},{affectedReason},' \
                                                               f'{additionalVerificationNeeded},{softwareRelease},' \
                                                               f'{productId}'
                                                    writer.writerow(CSV_Data.split())
                                                    tRecords += 1
                                logging.debug('Successfully written record')
                            else:
                                logging.debug(f'No Security Advisories Found on Success Track {successTrackId}')
                        else:
                            logging.debug(f'No Security Advisories Data Found For Success Track {successTrackId}'
                                          f'Skipping this record and continuing....')
                        if outputFormat == 1 or outputFormat == 2:
                            if totalCount > 0:
                                logging.debug(
                                    f'Saving {json_output_dir}{customerId}_SecurityAdvisories_{json_filename}')
                                shutil.copy(input_json_file, json_output_dir)
            else:
                logging.debug(f'No Security Advisories Data Found For Success Track {successTrackId}'
                              f'Skipping this record and continuing....')
    logging.debug(f'Total Security Advisory records {tRecords}')
    print(f'Total Security Advisory records {tRecords}')


def pxc_field_notices_reports():
    # Function to request, download and process Field Notices report data from PX Cloud
    # This API gives details of all the notifications published and their associated details.
    # CSV Naming Convention: Field_Notices.csv
    # JSON Naming Convention: {Customer ID}_Field_Notices_{UniqueReportID}.json

    logging.debug('********************* Running Field Notices Report *******************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Field Notices Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(fieldNotices, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,successTrackId,useCaseId,assetName,assetId,hwInstanceId,productId,serialNumber,' \
                         'fieldNoticeId,updated,title,created,url,additionalNotes,affectedStatus,affectedReason,' \
                         'fieldNoticeDescription,softwareRelease,ipAddress'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        count = 0
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            successTrackId = row['successTrackId']
            customerName = row['customerName']
            if successTrackId in STList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}/reports'
                field_notices_payload = json.dumps({'reportName': 'FieldNotices', 'successTrackId': successTrackId})
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                max_fn_attempts = 10
                fn_attempts = 0
                logging.debug(f'Requesting Field Notices data for {customerName} on Success Track ID {successTrackId}')
                while fn_attempts < max_fn_attempts:
                    # Make a request to API
                    response = requests.request('POST', url, headers=headers, data=field_notices_payload,
                                                verify=True, timeout=apiTimeout)
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        logging.debug(f'Querying with {field_notices_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if fn_attempts == 0:
                            logging.debug(f'Successfuly posted POST request for {field_notices_payload}')
                        break
                    # If rate limited, wait and try again
                    logging.debug(f'Rate Limit Exceeded for Field Notices Report! Retrying...')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    fn_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if fn_attempts > 0:
                    logging.debug(f'Retry was successful for Field Notices Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                if response.text.__contains__('Customer admin has not provided access.'):
                    logging.debug(f'{customerName} admin has not provided access on Success Track {successTrackId}')
                if not (response.text.__contains__('Customer admin has not provided access.')):
                    try:
                        if bool(response.headers['location']) is True:
                            pass
                        else:
                            raise KeyError
                    except KeyError:
                        logging.debug(f'\nException Thrown for Customer:{customerId} on Success Track {successTrackId} '
                                      f'\n{response} Headers:{headers} in function pxc_field_notices_reports')
                        while response.status_code >= 400:
                            logging.debug(f'Pausing for {wait_time} seconds before re-request...')
                            time.sleep(wait_time)
                            logging.debug('Resuming...')
                            logging.debug('Making API Call again with the following...')
                            response = requests.request('POST', url, headers=headers, data=field_notices_payload,
                                                        verify=True, timeout=apiTimeout)
                            logging.debug(f'URL Request: {url}')
                            logging.debug(f'Report details:{field_notices_payload}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(f'Review for  {customerName} on Success Track {successTrackId}'
                                          f' Report Failed to Download')
                    location = response.headers['location']
                    count += 1
                    logging.debug(f'Field Notices {count}:{location}')
                    location_ready_status(location, headers, 'Field Notices')
                    json_filename = (location.split('/')[-1] + '.json')
                    tries = 1
                    while True:
                        # time.sleep(tries)
                        filename = (temp_dir + customerId + '_Field_Notices_' + location.split('/')[-1] + '.zip')
                        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                        max_fn_attempts = 10
                        fn_attempts = 0
                        logging.debug(f'Requesting Field Notices data for {customerName} '
                                      f'on Success Track ID {successTrackId}')
                        while fn_attempts < max_fn_attempts:
                            # Make a request to API
                            response = requests.request('GET', location, headers=headers,
                                                        verify=True, timeout=apiTimeout)
                            # If not rate limited, break out of while loop and continue
                            if response.status_code != 429:
                                logging.debug(f'Querying:')
                                logging.debug(f'URL Request: {url}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if fn_attempts == 0:
                                    logging.debug(f'Successfuly posted GET request for {field_notices_payload}')
                                break
                            # If rate limited, wait and try again
                            logging.debug(f'Rate Limit Exceeded for Field Notices Report! Retrying...')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            fn_attempts += 1
                            pause = random.random()
                            logging.debug(f'sleeping for {pause} seconds')
                            time.sleep(pause)
                        if fn_attempts > 0:
                            logging.debug(f'Retry was successful for Purchased Licenses Report')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        if debug_level == 2:
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(f'Wait Time:{tries}')
                        try:
                            with open(filename, 'wb') as file:
                                file.write(response.content)
                            with open(filename, 'rb') as file:
                                if file.read() and response.status_code == 200:
                                    with zipfile.ZipFile(filename, 'r') as zip_file:
                                        zip_file.extractall(temp_dir)
                                        if tries >= 2:
                                            if debug_level == 2:
                                                logging.debug('Success on retry!')
                                else:
                                    logging.debug('Download Failed')
                                    raise Exception('File Corrupted')
                        except Exception as FileCorruptError:
                            logging.debug(f'Deleting file...{filename} - {FileCorruptError} - Retrying....')
                            logging.debug(f'Pausing for {wait_time} seconds before retrying...')
                            time.sleep(wait_time * tries)  # increase the wait with the number of retries
                            if tries >= 3:
                                break
                        else:
                            break
                        finally:
                            tries += 1
                    if useProductionURL == 0:
                        proccess_sandbox_files(filename, customerId, '_FieldNotices_', json_filename)
                    input_json_file = str(temp_dir + customerId + '_FieldNotices_' + json_filename)
                    logging.debug(f'Reading file:{input_json_file}')
                    if not os.path.isfile(input_json_file):
                        logging.debug('File Not Found, Skipping.....')
                    if os.path.isfile(input_json_file):
                        with open(input_json_file, 'r', encoding='utf-8') as inputTarget:
                            data = json.load(inputTarget)
                            customerId = data['metadata']['customerId']
                            totalCount = data['metadata']['totalCount']
                            items = data['items']
                        if outputFormat == 1 or outputFormat == 3:
                            if totalCount:
                                logging.debug(f'Found Field Notices on Success Track {successTrackId}')
                                for item in items:
                                    assetId = str(item['assetId']).replace(',', '_')
                                    hwInstanceId = str(item['hwInstanceId']).replace(',', ' ')
                                    assetName = str(item['assetName'])
                                    productId = str(item['productId'])
                                    serialNumber = str(item['serialNumber'])
                                    successTrack = item['successTrack']
                                    fieldNoticeId = str(item['fieldNoticeId'])
                                    updated = str(item['updated']).replace(',', ' ')
                                    title = str(item['title']).replace(',', ' ')
                                    created = str(item['created'])
                                    url = str(item['url']).replace(',', ' ')
                                    additionalNotes = str(item['additionalNotes']).replace(',', ' ')
                                    affectedStatus = str(item['affectedStatus'])
                                    affectedReasons = item['affectedReason']
                                    fieldNoticeDescription = str(item['fieldNoticeDescription']).replace(',', ' ')
                                    softwareRelease = str(item['softwareRelease'])
                                    if not softwareRelease:
                                        softwareRelease = 'N/A'
                                    ipAddress = str(item['ipAddress'])
                                    if not ipAddress:
                                        ipAddress = 'N/A'
                                    for track in successTrack:
                                        successTrackId = track['id']
                                        useCases = track['useCases']
                                        for useCase in useCases:
                                            useCaseId = useCase
                                            for reason in affectedReasons:
                                                affectedReason = reason
                                                if outputFormat == 1 or outputFormat == 3:
                                                    with open(fieldNotices, 'a', encoding='utf-8', newline='') \
                                                            as report_target:
                                                        writer = csv.writer(report_target,
                                                                            delimiter=' ',
                                                                            quotechar=' ',
                                                                            quoting=csv.QUOTE_MINIMAL)
                                                        CSV_Data = f'{customerId},{successTrackId},{useCaseId},' \
                                                                   f'{assetName},{assetId},{hwInstanceId},' \
                                                                   f'{productId},{serialNumber},{fieldNoticeId},' \
                                                                   f'{updated},{title},{created},{url},' \
                                                                   f'{additionalNotes},{affectedStatus},' \
                                                                   f'{affectedReason},{fieldNoticeDescription},' \
                                                                   f'{softwareRelease},{ipAddress}'
                                                        writer.writerow(CSV_Data.split())
                                                        tRecords += 1
                                logging.debug('Successfully written record')
                            else:
                                logging.debug(f'No Field Notice Data Found For Success Track {successTrackId}')
                        else:
                            logging.debug(f'No Field Notice Data Found For Success Track {successTrackId}')
                    else:
                        logging.debug(f'No Field Notice Data Found For Success Track {successTrackId}'
                                      f'Skipping this record and continuing....')
                    if outputFormat == 1 or outputFormat == 2:
                        if totalCount > 0:
                            logging.debug(f'Saving {json_output_dir}{customerId}_FieldNotices_{json_filename}')
                            shutil.copy(input_json_file, json_output_dir)
            else:
                logging.debug(f'No Field Notice Data Found For Success Track {successTrackId}'
                              f'Skipping this record and continuing....')
    logging.debug(f'Total Field Notice records {tRecords}')
    print(f'Total Field Notice records {tRecords}')


def pxc_priority_bugs_reports():
    # Function to request, download and process Priority Bugs report data from PX Cloud
    # This API provides many bug details including asset name and ID, serial number, IP address and other fields
    # CSV Naming Convention: Priority_Bugs.csv
    # JSON Naming Convention: {Customer ID}_Priority_Bugs_{UniqueReportID}.json
    logging.debug('********************* Running Priority Bugs Report *******************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Priority Bug Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(priorityBugs, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,successTrackId,useCaseId,assetName,assetId,serialNumber,ipAddress,' \
                         'softwareRelease,productId,bugId,bugTitle,description,url,bugSeverity,impact'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        count = 0
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            successTrackId = row['successTrackId']
            customerName = row['customerName']
            if successTrackId in STList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}/reports'
                priority_bugs_payload = json.dumps({"reportName": "PriorityBugs", "successTrackId": successTrackId})
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                max_priorityBugs_attempts = 10
                priorityBugs_attempts = 0
                logging.debug(f'Requesting Priority Bugs data for {customerName} on Success Track ID {successTrackId}')
                while priorityBugs_attempts < max_priorityBugs_attempts:
                    # Make a request to API
                    response = requests.request('POST', url, headers=headers, data=priority_bugs_payload,
                                                verify=True, timeout=apiTimeout)
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        logging.debug(f'Querying with {priority_bugs_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if priorityBugs_attempts == 0:
                            logging.debug(f'Successfuly posted POST request for {priority_bugs_payload}')
                        break
                    # If rate limited, wait and try again
                    logging.debug(f'Rate Limit Exceeded for Priority Bug Report! Retrying...')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                    priorityBugs_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if priorityBugs_attempts > 1:
                    logging.debug(f'Retry was successful for Priority Bug Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                if response.text.__contains__('Customer admin has not provided access.'):
                    print(f'{customerName} admin has not provided access for {successTrackId}')
                if not (response.text.__contains__('Customer admin has not provided access.')):
                    try:
                        if bool(response.headers['location']) is True:
                            pass
                        else:
                            raise KeyError
                    except KeyError:
                        logging.debug(f'\nException Thrown for Customer:{customerId} on Success Track {successTrackId} '
                                      f'\n{response} Headers:{headers} in function pxc_priority_bugs_reports')
                        while response.status_code >= 400:
                            logging.debug(f'Pausing for {wait_time} seconds before re-request...')
                            time.sleep(wait_time)
                            logging.debug('Resuming...')
                            logging.debug('Making API Call again with the following...')
                            response = requests.request('POST', url, headers=headers, data=priority_bugs_payload,
                                                        verify=True, timeout=apiTimeout)
                            logging.debug(f'URL Request: {url}')
                            logging.debug(f'Report details:{priority_bugs_payload}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(f'Review for  {customerName} on Success Track {successTrackId}'
                                          f' Report Failed to Download')
                    location = response.headers['location']
                    count += 1
                    logging.debug(f'Priority Bugs {count}:{location}')
                    location_ready_status(location, headers, 'Priority Bugs')
                    json_filename = (location.split('/')[-1] + '.json')
                    tries = 1
                    while True:
                        # time.sleep(tries)
                        filename = (temp_dir + customerId + '_Priority_Bugs_' + location.split('/')[-1] + '.zip')
                        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                        max_priorityBugs_attempts = 10
                        priorityBugs_attempts = 0
                        logging.debug(f'Requesting Asset data for {customerName} on Success Track ID {successTrackId}')
                        while priorityBugs_attempts < max_priorityBugs_attempts:
                            # Make a request to API
                            response = requests.request('GET', location, headers=headers,
                                                        verify=True, timeout=apiTimeout)
                            # If not rate limited, break out of while loop and continue
                            if response.status_code != 429:
                                logging.debug(f'Querying:')
                                logging.debug(f'URL Request: {url}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if priorityBugs_attempts == 0:
                                    logging.debug(f'Successfuly posted GET request for {priority_bugs_payload}')
                                break
                            # If rate limited, wait and try again
                            logging.debug(f'Rate Limit Exceeded for Priority Bug Report! Retrying...')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            priorityBugs_attempts += 1
                            pause = random.random()
                            logging.debug(f'sleeping for {pause} seconds')
                            time.sleep(pause)
                        if priorityBugs_attempts > 0:
                            logging.debug(f'Retry was successful for Priority Bug Report')
                            logging.debug(f'Location Request: {location}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Location Request: {location}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        logging.debug(f'Wait Time:{tries}')
                        try:
                            with open(filename, 'wb') as file:
                                file.write(response.content)
                            with open(filename, 'rb') as file:
                                if file.read() and response.status_code == 200:
                                    with zipfile.ZipFile(filename, 'r') as zip_file:
                                        zip_file.extractall(temp_dir)
                                        if tries >= 2:
                                            if debug_level == 2:
                                                logging.debug('Success on retry! Continuing')
                                else:
                                    logging.debug('Download Failed')
                                    raise Exception('File Corrupted')
                        except Exception as FileCorruptError:
                            logging.debug(f'Deleting file...{filename} - {FileCorruptError} - Retrying....')
                            logging.debug(f'Pausing for {wait_time} seconds before retrying...')
                            time.sleep(wait_time * tries)  # increase the wait with the number of retries
                            if tries >= 3:
                                break
                        else:
                            break
                        finally:
                            tries += 1
                    if useProductionURL == 0:
                        proccess_sandbox_files(filename, customerId, '_PriorityBugs_', json_filename)
                    input_json_file = str(temp_dir + customerId + '_PriorityBugs_' + json_filename)
                    logging.debug(f'Reading file:{input_json_file}')
                    if not os.path.isfile(input_json_file):
                        logging.debug('File Not Found, Skipping.....')
                    if os.path.isfile(input_json_file):
                        with open(input_json_file, 'r', encoding='utf-8') as inputTarget:
                            data = json.load(inputTarget)
                            customerId = data['metadata']['customerId']
                            totalCount = data['metadata']['totalCount']
                            items = data['items']
                        if outputFormat == 1 or outputFormat == 3:
                            if totalCount:
                                logging.debug(f'Found Priority Bugs on Success Track {successTrackId}')
                                for item in items:
                                    assetName = str(item['assetName'])
                                    assetId = str(item['assetId']).replace(',', '_')
                                    serialNumber = str(item['serialNumber'])
                                    ipAddress = str(item['ipAddress'])
                                    softwareRelease = str(item['softwareRelease'])
                                    productId = str(item['productId'])
                                    bugId = str(item['bugId']).replace(',', ' ')
                                    bugTitle = str(item['bugTitle']).replace(',', ' ')
                                    description = str(item['description']).replace(',', ' ')
                                    url = str(item['url']).replace(',', ' ')
                                    bugSeverity = str(item['bugSeverity']).replace(',', ' ')
                                    successTrack = item['successTrack']
                                    impact = str(item['impact']).replace(',', ' ')
                                    for track in successTrack:
                                        successTrackId = track['id']
                                        useCases = track['useCases']
                                        for useCase in useCases:
                                            useCaseId = useCase
                                            if outputFormat == 1 or outputFormat == 3:
                                                with open(priorityBugs, 'a', encoding='utf-8', newline='') \
                                                        as report_target:
                                                    writer = csv.writer(report_target,
                                                                        delimiter=' ',
                                                                        quotechar=' ',
                                                                        quoting=csv.QUOTE_MINIMAL)
                                                    CSV_Data = f'{customerId},{successTrackId},{useCaseId},' \
                                                               f'{assetName},{assetId},{serialNumber},{ipAddress},' \
                                                               f'{softwareRelease},{productId},{bugId},{bugTitle},' \
                                                               f'{description},{url},{bugSeverity},{impact}'
                                                    writer.writerow(CSV_Data.split())
                                                    tRecords += 1
                                logging.debug('Successfully written record')
                            else:
                                logging.debug(f'No Priority Bug Data Found For Success Track {successTrackId}')
                        else:
                            logging.debug(f'No Priority Bug Data Found For Success Track {successTrackId}'
                                          f'{successTrackId}Skipping this record and continuing....')
                        if outputFormat == 1 or outputFormat == 2:
                            if totalCount > 0:
                                logging.debug(f'Saving {json_output_dir}{customerId}_PriorityBugs_{json_filename}')
                                shutil.copy(input_json_file, json_output_dir)
                else:
                    logging.debug(f'No Priority Bug Data Found For Success Track {successTrackId}'
                                  f'{successTrackId} Skipping this record and continuing....')
    logging.debug(f'Total Priority Bug records {tRecords}')
    print(f'Total Priority Bug records {tRecords}')


def get_pxc_lifecycle():
    # Function to get the Lifecycle data from PX Cloud
    # This API will return customer lifecycle which will provide the following:CX solution, Use case and Pitstop info.
    # CSV Naming Convention: Lifecycle.csv
    # JSON Naming Convention: {Customer ID}_Lifecycle.json
    logging.debug('********************** Running Lifecycle Report **********************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Lifecycle Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(lifecycle, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerName,successTracksId,successTrackName,useCaseName,useCaseId,currentPitstopName,' \
                         'pitStopName,pitstopActionName,pitstopActionId,pitstopActionCompleted'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            logging.debug(f'Scanning lifecycle data for {customerName} on Success Track {successTrackId}')
            if successTrackId:
                items = (get_json_reply
                         (url=f'{pxc_url_customers}{customerId}{pxc_url_lifecycle}?successTrackId={successTrackId}',
                          tag='items', report='Lifecycle Report'))
                if outputFormat == 1 or outputFormat == 3:
                    if items is not None:
                        if len(items) > 0:
                            logging.debug(f'Found Lifecycle Data on Success Track {successTrackId} '
                                          f'for customer {customerName}')
                            for item in items:
                                successTrackName = item['successTrack'].replace(',', ' ')
                                successTracksId = item['id'].replace(',', ' ')
                                useCases = item['usecases']
                                for useCase in useCases:
                                    useCaseName = useCase['name'].replace(',', ' ')
                                    useCaseId = useCase['id'].replace(',', ' ')
                                    currentPitstopName = useCase['currentPitstop'].replace(',', ' ')
                                    pitstops = useCase['pitstops']
                                    for pitstop in pitstops:
                                        pitstopName = pitstop['name'].replace(',', ' ')
                                        pitstopActions = pitstop['pitstopActions']
                                        for pitstopAction in pitstopActions:
                                            pitstopActionName = pitstopAction['name'].replace(',', ' ')
                                            pitstopActionId = pitstopAction['id'].replace(',', ' ')
                                            pitstopActionCompleted = pitstopAction['completed']
                                            if outputFormat == 1 or outputFormat == 3:
                                                with open(lifecycle, 'a', encoding='utf-8', newline='') \
                                                        as lifecycle_target:
                                                    writer = csv.writer(lifecycle_target,
                                                                        delimiter=' ',
                                                                        quotechar=' ',
                                                                        quoting=csv.QUOTE_MINIMAL)
                                                    CSV_Data = f'{customerName},{successTracksId},{successTrackName},' \
                                                               f'{useCaseName},{useCaseId},{currentPitstopName},' \
                                                               f'{pitstopName},{pitstopActionName},{pitstopActionId},' \
                                                               f'{pitstopActionCompleted}'
                                                    writer.writerow(CSV_Data.split())
                                                    tRecords += 1
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + customerId + '_Lifecycle_' + successTrackId + '.json',
                                      'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
            else:
                logging.debug(
                    f'No Lifecycle Data Found For {customerName} on Success Track {successTrackId}....')
    logging.debug(f'Total Lifecycle records {tRecords}')
    print(f'Total Lifecycle records {tRecords}')


def pxc_software_groups():
    # Functions to get the Optimal Software Version data from PX Cloud.
    # This API returns the Software Group information for the given customerID.
    # CSV Naming Convention: SoftwareGroup.csv
    # JSON Naming Convention: {customerName}_SoftwareGroups.json
    logging.debug('******************** Running Software Groups Insights Report ******************\n')
    tRecords = 0
    global software_groupsFlag
    software_groupsFlag = 0
    while contractswithcustomernamesFlag == 0:
        logging.debug(f'Waiting on Contract with Customer Names List to finish')
        time.sleep(1)
    print('Started Running Software Groups Insights Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(SWGroups, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,suggestionId,riskLevel,softwareGroupName,sourceId,' \
                         'productFamily,softwareType,currentReleases,selectedRelease,assetCount,suggestions,' \
                         'sourceSystemId,softwareGroupId,managedBy'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(OSV_AFM_List, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            time.sleep(1)
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            if successTrackId in insightList:
                logging.debug(f'Scanning {customerName}')
                url = f'{pxc_url_customers}{customerId}{pxc_url_software_groups}' \
                      f'?offset=0&max={max_items}&successTrackId={successTrackId}'
                items = (get_json_reply(url, tag='items', report='Software Groups Report'))
                if items is not None:
                    if outputFormat == 1 or outputFormat == 2:
                        if len(items) > 0:
                            with open(json_output_dir + customerName + '_SoftwareGroups.json',
                                      'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                    if outputFormat == 1 or outputFormat == 3:
                        for item in items:
                            riskLevel = item['riskLevel'].replace(',', ' ')
                            softwareGroupName = item['softwareGroupName'].replace(',', ' ')
                            sourceId = item['sourceId'].replace(',', ' ')
                            productFamily = item['productFamily'].replace(',', ' ')
                            softwareType = item['softwareType']
                            currentReleases = item['currentReleases'].replace(',', ' ')
                            selectedRelease = item['selectedRelease'].replace(',', ' ')
                            assetCount = item['assetCount']
                            suggestionId = item['suggestionId']
                            suggestions = item['suggestions']
                            sourceSystemId = item['sourceSystemId'].replace(',', ' ')
                            softwareGroupId = item['softwareGroupId'].replace(',', ' ')
                            managedBy = item['managedBy'].replace(',', ' ')
                            with open(SWGroups, 'a', encoding='utf-8', newline='') as temp_target:
                                writer = csv.writer(temp_target,
                                                    delimiter=' ',
                                                    quotechar=' ',
                                                    quoting=csv.QUOTE_MINIMAL)
                                CSV_Data = f'{customerId},{customerName},{successTrackId},{suggestionId},{riskLevel},' \
                                           f'{softwareGroupName},{sourceId},{productFamily},{softwareType},' \
                                           f'{currentReleases},{selectedRelease},{assetCount},{suggestions},' \
                                           f'{sourceSystemId},{softwareGroupId},{managedBy}'
                                writer.writerow(CSV_Data.split())
                                tRecords += 1

                else:
                    logging.debug('No Software Group Data Found .... Skipping')
    software_groupsFlag += 1
    logging.debug(f'Total Software Group records {tRecords}')
    print(f'Total Software Group records {tRecords}')


def pxc_software_group_suggestions():
    # This API returns Software Group suggestions, including detailed information about Cisco software release
    # recommendations and current Cisco software releases running on assets in the Software Group.
    # CSV Naming Convention: SoftwareGroup_Suggestions_Trends.csv
    #                        SoftwareGroup_Suggestions_Summaries.csv
    #                        SoftwareGroup_Suggestions_Releases.csv
    # JSON Naming Convention: {Customer ID}_SoftwareGroup_Suggestions_{Success Track ID}_{Suggestion ID}.json
    logging.debug('************** Running Software Groups Suggestions Report ************\n')
    tRecordsTrend = 0
    tRecordsReleases = 0
    tRecordsSummary = 0
    global software_group_suggestionsFlag
    software_group_suggestionsFlag = 0
    while software_groupsFlag == 0:
        logging.debug(f'Waiting on Software Groups Report to finish')
        time.sleep(1)
    print('Started Running Software Groups Suggestions  Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(SWGroupSuggestionsTrend, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,suggestionId,suggestionsInterval,' \
                         'suggestionUpdatedDate,suggestionSelectedDate,changeFromPrev,riskCategory,riskDate,' \
                         'riskScore'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
        with open(SWGroupSuggestionsReleases, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,suggestionId,suggestionsInterval,' \
                         'suggestionUpdatedDate,suggestionSelectedDate,releaseSummaryName,releaseSummaryReleaseDate,' \
                         'releaseSummaryRelease'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
        with open(SWGroupSuggestionSummaries, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,suggestionId,suggestionsInterval,' \
                         'suggestionUpdatedDate,suggestionSelectedDate,machineSuggestionId,expectedSoftwareGroupRisk,' \
                         'expectedSoftwareGroupRiskCategory,name,releaseDate,release,releaseNotesUrl,' \
                         'fieldNoticeSeverityFixedHigh,fieldNoticeSeverityFixedMedium,fieldNoticeSeverityFixedLow,' \
                         'fieldNoticeSeverityNewExposedHigh,fieldNoticeSeverityNewExposedMedium,' \
                         'fieldNoticeSeverityNewExposedLow,fieldNoticeSeverityExposedHigh,' \
                         'fieldNoticeSeverityExposedMedium,fieldNoticeSeverityExposedLow,advisoriesSeverityFixedHigh,' \
                         'advisoriesSeverityFixedMedium,advisoriesSeverityFixedLow,advisoriesSeverityNewExposedHigh,' \
                         'advisoriesSeverityNewExposedMedium,advisoriesSeverityNewExposedLow,' \
                         'advisoriesSeverityExposedHigh,advisoriesSeverityExposedMedium,advisoriesSeverityExposedLow,' \
                         'bugSeverityFixedHigh,bugSeverityFixedMedium,bugSeverityFixedLow,bugSeverityNewExposedHigh,' \
                         'bugSeverityNewExposedMedium,bugSeverityNewExposedLow,bugSeverityExposedHigh,' \
                         'bugSeverityExposedMedium,bugSeverityExposedLow,featuresCountActiveFeaturesCount,' \
                         'featuresCountAffectedFeaturesCount,featuresCountFixedFeaturesCount'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(SWGroups, 'r') as target:
        csvreader = csv.DictReader(target)
        for row in csvreader:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            suggestionId = row['suggestionId']
            if row['successTrackId'] in insightList:
                token_time_check()
                logging.debug(f'Found Software Group Suggestion for {customerName}')
                url = f'{pxc_url_customers}{customerId}{pxc_url_software_group_suggestions}' \
                      f'?successTrackId={successTrackId}&suggestionId={suggestionId}'
                try:
                    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                    # rate_limit_check(url, 'Software Group Suggestion Report')
                    max_SWGroups_attempts = 10
                    SWGroups_attempts = 0
                    while SWGroups_attempts < max_SWGroups_attempts:
                        # Make a request to API
                        for i in range(10):
                            i += 1
                            logging.debug(f'Request attempt number {i}')
                            try:
                                response = requests.request('GET', url, headers=headers, data=data_payload,
                                                            verify=True, timeout=apiTimeout)
                                if i > 1:
                                    logging.debug(f'API request retry # {i} was successful')
                                break
                            except Timeout:
                                logging.debug(f'Time out error getting resonse')
                        # If not rate limited, break out of while loop and continue
                        if response.status_code != 429:
                            if debug_level > 0:
                                logging.debug(f'Querying with {data_payload}')
                                logging.debug(f'URL Request: {url}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if SWGroups_attempts == 0:
                                    logging.debug('Success')
                            break
                        # If rate limited, wait and try again
                        logging.debug('Rate Limit Exceeded for Software Group Suggestion Report! Retrying.')
                        time.sleep((1 ** SWGroups_attempts) + random.random())
                        logging.debug(f'Sleeping for {(2 ** SWGroups_attempts) + random.random()} seconds')
                        SWGroups_attempts = SWGroups_attempts + 1
                        logging.debug(f'Number of Attempts {SWGroups_attempts}')
                    reply = json.loads(response.text)
                    if response.status_code == 200:
                        jsonDump = {'items': [reply]}
                        suggestionsInterval = str(reply['suggestionsInterval'])
                        suggestionUpdatedDate = str(reply['suggestionUpdatedDate'])
                        suggestionSelectedDate = str(reply['suggestionSelectedDate'])
                    else:
                        jsonDump = []
                    if debug_level == 2:
                        logging.debug(f'URL Request:{url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                except Exception as Error:
                    logging.debug(Error)
                    jsonDump = []
                finally:
                    if outputFormat == 1 or outputFormat == 2:
                        if not jsonDump == []:
                            with open(json_output_dir + customerName + '_SoftwareGroup_Suggestions_' + successTrackId
                                      + '_' + suggestionId + '.json', 'w') as json_file:
                                json.dump(jsonDump, json_file)
                            logging.debug(f'Saving {json_file.name}')
                try:
                    softwareGroupRiskTrend = (get_json_reply(url,
                                                             tag='softwareGroupRiskTrend',
                                                             report='Software Group Suggestions Report'))
                except Exception as Error:
                    logging.debug(Error)
                    softwareGroupRiskTrend = []
                logging.debug(f'Saving Software Group Risk Trend to CSV')
                if softwareGroupRiskTrend is not None:
                    for item in softwareGroupRiskTrend:
                        changeFromPrev = str(item['changeFromPrev'])
                        riskCategory = str(item['riskCategory'])
                        riskDate = str(item['date'])
                        riskScore = str(item['riskScore'])
                        with open(SWGroupSuggestionsTrend, 'a', encoding='utf-8', newline='') \
                                as temp_target:
                            writer = csv.writer(temp_target,
                                                delimiter=' ',
                                                quotechar=' ',
                                                quoting=csv.QUOTE_MINIMAL)
                            CSV_Data = f'{customerId},{customerName},{successTrackId},{suggestionId},' \
                                       f'{suggestionsInterval},{suggestionUpdatedDate},{suggestionSelectedDate},' \
                                       f'{changeFromPrev},{riskCategory},{riskDate},{riskScore}'
                            writer.writerow(CSV_Data.split())
                            tRecordsTrend += 1
                try:
                    releaseSummary = (get_json_reply(url,
                                                     tag='releaseSummary',
                                                     report='Software Group Suggestions Report'))
                except Exception as Error:
                    logging.debug(Error)
                    releaseSummary = []
                logging.debug(f'Saving Software Group Release Summaries to CSV')
                if releaseSummary is not None:
                    for item in releaseSummary:
                        releaseSummaryName = str(item['name'])
                        releaseSummaryReleaseDate = str(item['releaseDate'])
                        releaseSummaryRelease = str(item['release']).replace(',', '-')
                        with open(SWGroupSuggestionsReleases, 'a', encoding='utf-8', newline='') \
                                as temp_target:
                            writer = csv.writer(temp_target,
                                                delimiter=' ',
                                                quotechar=' ',
                                                quoting=csv.QUOTE_MINIMAL)
                            CSV_Data = f'{customerId},{customerName},{successTrackId},{suggestionId},' \
                                       f'{suggestionsInterval},{suggestionUpdatedDate},{suggestionSelectedDate},' \
                                       f'{releaseSummaryName},{releaseSummaryReleaseDate},{releaseSummaryRelease}'
                            writer.writerow(CSV_Data.split())
                            tRecordsReleases += 1
                try:
                    suggestionSummaries = (get_json_reply(url,
                                                          tag='suggestionSummaries',
                                                          report='Software Group Suggestions Report'))
                except Exception as Error:
                    logging.debug(Error)
                    suggestionSummaries = []
                logging.debug(f'Saving Software Group Summaries to CSV')
                if suggestionSummaries is not None:
                    for item in suggestionSummaries:
                        machineSuggestionId = str(item['machineSuggestionId'])
                        expectedSoftwareGroupRisk = str(item['expectedSoftwareGroupRisk'])
                        expectedSoftwareGroupRiskCategory = str(item['expectedSoftwareGroupRiskCategory'])
                        name = str(item['name'])
                        releaseDate = str(item['releaseDate'])
                        release = str(item['release']).replace(',', '-')
                        releaseNotesUrl = str(item['releaseNotesUrl'])
                        fieldNoticeSeverity = (item['fieldNoticeSeverity'])
                        try:
                            exposed = fieldNoticeSeverity['Exposed']
                            fieldNoticeSeverityExposedHigh = str(exposed['High'])
                        except KeyError:
                            fieldNoticeSeverityExposedHigh = '0'
                        try:
                            exposed = fieldNoticeSeverity['Exposed']
                            fieldNoticeSeverityExposedMedium = str(exposed['Medium'])
                        except KeyError:
                            fieldNoticeSeverityExposedMedium = '0'
                        try:
                            exposed = fieldNoticeSeverity['Exposed']
                            fieldNoticeSeverityExposedLow = str(exposed['Low'])
                        except KeyError:
                            fieldNoticeSeverityExposedLow = '0'
                        try:
                            fixed = fieldNoticeSeverity['Fixed']
                            fieldNoticeSeverityFixedHigh = str(fixed['High'])
                        except KeyError:
                            fieldNoticeSeverityFixedHigh = '0'
                        try:
                            fixed = fieldNoticeSeverity['Fixed']
                            fieldNoticeSeverityFixedMedium = str(fixed['Medium'])
                        except KeyError:
                            fieldNoticeSeverityFixedMedium = '0'
                        try:
                            fixed = fieldNoticeSeverity['Fixed']
                            fieldNoticeSeverityFixedLow = str(fixed['Low'])
                        except KeyError:
                            fieldNoticeSeverityFixedLow = '0'
                        try:
                            newExposed = fieldNoticeSeverity['New_Exposed']
                            fieldNoticeSeverityNewExposedHigh = str(newExposed['High'])
                        except KeyError:
                            fieldNoticeSeverityNewExposedHigh = '0'
                        try:
                            newExposed = fieldNoticeSeverity['New_Exposed']
                            fieldNoticeSeverityNewExposedMedium = str(newExposed['Medium'])
                        except KeyError:
                            fieldNoticeSeverityNewExposedMedium = '0'
                        try:
                            newExposed = fieldNoticeSeverity['New_Exposed']
                            fieldNoticeSeverityNewExposedLow = str(newExposed['Low'])
                        except KeyError:
                            fieldNoticeSeverityNewExposedLow = '0'
                        advisoriesSeverity = (item['advisoriesSeverity'])
                        try:
                            exposed = advisoriesSeverity['Exposed']
                            advisoriesSeverityExposedHigh = str(exposed['High'])
                        except KeyError:
                            advisoriesSeverityExposedHigh = '0'
                        try:
                            exposed = advisoriesSeverity['Exposed']
                            advisoriesSeverityExposedMedium = str(exposed['Medium'])
                        except KeyError:
                            advisoriesSeverityExposedMedium = '0'
                        try:
                            exposed = advisoriesSeverity['Exposed']
                            advisoriesSeverityExposedLow = str(exposed['Low'])
                        except KeyError:
                            advisoriesSeverityExposedLow = '0'
                        try:
                            fixed = advisoriesSeverity['Fixed']
                            advisoriesSeverityFixedHigh = str(fixed['High'])
                        except KeyError:
                            advisoriesSeverityFixedHigh = '0'
                        try:
                            fixed = advisoriesSeverity['Fixed']
                            advisoriesSeverityFixedMedium = str(fixed['Medium'])
                        except KeyError:
                            advisoriesSeverityFixedMedium = '0'
                        try:
                            fixed = advisoriesSeverity['Fixed']
                            advisoriesSeverityFixedLow = str(fixed['Low'])
                        except KeyError:
                            advisoriesSeverityFixedLow = '0'
                        try:
                            newExposed = advisoriesSeverity['New_Exposed']
                            advisoriesSeverityNewExposedHigh = str(newExposed['High'])
                        except KeyError:
                            advisoriesSeverityNewExposedHigh = '0'
                        try:
                            newExposed = advisoriesSeverity['New_Exposed']
                            advisoriesSeverityNewExposedMedium = str(newExposed['Medium'])
                        except KeyError:
                            advisoriesSeverityNewExposedMedium = '0'
                        try:
                            newExposed = advisoriesSeverity['New_Exposed']
                            advisoriesSeverityNewExposedLow = str(newExposed['Low'])
                        except KeyError:
                            advisoriesSeverityNewExposedLow = '0'
                        bugSeverity = (item['bugSeverity'])
                        try:
                            exposed = bugSeverity['Exposed']
                            bugSeverityExposedHigh = str(exposed['High'])
                        except KeyError:
                            bugSeverityExposedHigh = '0'
                        try:
                            exposed = bugSeverity['Exposed']
                            bugSeverityExposedMedium = str(exposed['Medium'])
                        except KeyError:
                            bugSeverityExposedMedium = '0'
                        try:
                            exposed = bugSeverity['Exposed']
                            bugSeverityExposedLow = str(exposed['Low'])
                        except KeyError:
                            bugSeverityExposedLow = '0'
                        try:
                            fixed = bugSeverity['Fixed']
                            bugSeverityFixedHigh = str(fixed['High'])
                        except KeyError:
                            bugSeverityFixedHigh = '0'
                        try:
                            fixed = bugSeverity['Fixed']
                            bugSeverityFixedMedium = str(fixed['Medium'])
                        except KeyError:
                            bugSeverityFixedMedium = '0'
                        try:
                            fixed = bugSeverity['Fixed']
                            bugSeverityFixedLow = str(fixed['Low'])
                        except KeyError:
                            bugSeverityFixedLow = '0'
                        try:
                            newExposed = bugSeverity['New_Exposed']
                            bugSeverityNewExposedHigh = str(newExposed['High'])
                        except KeyError:
                            bugSeverityNewExposedHigh = '0'
                        try:
                            newExposed = bugSeverity['New_Exposed']
                            bugSeverityNewExposedMedium = str(newExposed['Medium'])
                        except KeyError:
                            bugSeverityNewExposedMedium = '0'
                        try:
                            newExposed = bugSeverity['New_Exposed']
                            bugSeverityNewExposedLow = str(newExposed['Low'])
                        except KeyError:
                            bugSeverityNewExposedLow = '0'
                        featuresCount = (item['featuresCount'])
                        try:
                            featuresCountActiveFeaturesCount = str(featuresCount['activeFeaturesCount'])
                        except KeyError:
                            featuresCountActiveFeaturesCount = '0'
                        try:
                            featuresCountAffectedFeaturesCount = str(featuresCount['affectedFeaturesCount'])
                        except KeyError:
                            featuresCountAffectedFeaturesCount = '0'
                        try:
                            featuresCountFixedFeaturesCount = str(featuresCount['fixedFeaturesCount'])
                        except KeyError:
                            featuresCountFixedFeaturesCount = '0'
                        with open(SWGroupSuggestionSummaries, 'a', encoding='utf-8', newline='') \
                                as temp_target:
                            writer = csv.writer(temp_target,
                                                delimiter=' ',
                                                quotechar=' ',
                                                quoting=csv.QUOTE_MINIMAL)
                            CSV_Data = f'{customerId},{customerName},{successTrackId},{suggestionId},' \
                                       f'{suggestionsInterval},{suggestionUpdatedDate},{suggestionSelectedDate},' \
                                       f'{machineSuggestionId},{expectedSoftwareGroupRisk},' \
                                       f'{expectedSoftwareGroupRiskCategory},{name},{releaseDate},{release},' \
                                       f'{releaseNotesUrl},{fieldNoticeSeverityFixedHigh},' \
                                       f'{fieldNoticeSeverityFixedMedium},{fieldNoticeSeverityFixedLow},' \
                                       f'{fieldNoticeSeverityNewExposedHigh},{fieldNoticeSeverityNewExposedMedium},' \
                                       f'{fieldNoticeSeverityNewExposedLow},{fieldNoticeSeverityExposedHigh},' \
                                       f'{fieldNoticeSeverityExposedMedium},{fieldNoticeSeverityExposedLow},' \
                                       f'{advisoriesSeverityFixedHigh},{advisoriesSeverityFixedMedium},' \
                                       f'{advisoriesSeverityFixedLow},{advisoriesSeverityNewExposedHigh},' \
                                       f'{advisoriesSeverityNewExposedMedium},{advisoriesSeverityNewExposedLow},' \
                                       f'{advisoriesSeverityExposedHigh},{advisoriesSeverityExposedMedium},' \
                                       f'{advisoriesSeverityExposedLow},{bugSeverityFixedHigh},' \
                                       f'{bugSeverityFixedMedium},{bugSeverityFixedLow},{bugSeverityNewExposedHigh},' \
                                       f'{bugSeverityNewExposedMedium},{bugSeverityNewExposedLow},' \
                                       f'{bugSeverityExposedHigh},{bugSeverityExposedMedium},' \
                                       f'{bugSeverityExposedLow},{featuresCountActiveFeaturesCount},' \
                                       f'{featuresCountAffectedFeaturesCount},{featuresCountFixedFeaturesCount}'
                            writer.writerow(CSV_Data.split())
                            tRecordsSummary += 1
    software_group_suggestionsFlag = 1
    logging.debug(f'Total Software Groups Suggestion Trend records {tRecordsTrend}')
    logging.debug(f'Total Software Groups Suggestion Release records {tRecordsReleases}')
    logging.debug(f'Total Software Groups Suggestion Summary records {tRecordsSummary}')
    print(f'Total Software Groups Suggestion Trend records {tRecordsTrend}')
    print(f'Total Software Groups Suggestion Release records {tRecordsReleases}')
    print(f'Total Software Groups Suggestion Summary records {tRecordsSummary}')


def pxc_software_group_suggestions_assets():
    # This API returns information about assets in the Software Group based on the customerID and softwareGroupId.
    # CSV Naming Convention: SoftwareGroup_Suggestions_Assets.csv
    # JSON Naming Convention: {Customer ID}_SoftwareGroup_Suggestion_Assets_{Software Group ID}.json
    logging.debug('*********** Running Software Groups Suggestions Assets Report ********\n')
    tRecords = 0
    while software_groupsFlag == 0:
        logging.debug(f'Waiting on Software Groups Report to finish')
        time.sleep(1)
    print('Started Running Software Groups Suggestions Assets Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(SWGroupSuggestionAssets, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,softwareGroupId,deploymentStatus,selectedRelease,' \
                         'assetName,ipAddress,softwareType,currentRelease'
            writeheader = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writeheader.writerow(CSV_Header.split())
    with open(SWGroups, 'r') as target:
        csvreader = csv.DictReader(target)
        for row in csvreader:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            softwareGroupId = row['softwareGroupId']
            if row['successTrackId'] in insightList:
                logging.debug(f'Found Software Group ID {softwareGroupId} Suggestion Asset for {customerName}')
                url = f'{pxc_url_customers}{customerId}{pxc_url_software_group_suggestions_assets}' \
                      f'?softwareGroupId={softwareGroupId}&successTrackId={successTrackId}'
                items = (get_json_reply(url, tag='items', report='SoftwareGroup Suggestion Assets Report'))
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(
                                    json_output_dir + customerId + '_SoftwareGroup_Suggestion_Assets_' +
                                    softwareGroupId + '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                if outputFormat == 1 or outputFormat == 3:
                    if items:
                        for item in items:
                            deploymentStatus = str(item['deploymentStatus'])
                            selectedRelease = str(item['selectedRelease'])
                            try:
                                assetName = str(item['assetName'].replace(',', ' '))
                            except KeyError:
                                assetName = str(item['assetHostName'].replace(',', ' '))
                                pass
                            ipAddress = str(item['ipAddress'])
                            softwareType = str(item['softwareType'].replace(',', '|'))
                            currentRelease = str(item['currentRelease'])
                            with open(SWGroupSuggestionAssets, 'a', encoding='utf-8', newline='') \
                                    as temp_target:
                                writer = csv.writer(temp_target,
                                                    delimiter=' ',
                                                    quotechar=' ',
                                                    quoting=csv.QUOTE_MINIMAL)
                                CSV_Data = f'{customerId},{customerName},{successTrackId},{softwareGroupId},' \
                                           f'{deploymentStatus},{selectedRelease},{assetName},{ipAddress},' \
                                           f'{softwareType},{currentRelease}'
                                writer.writerow(CSV_Data.split())
                                tRecords += 1
    logging.debug(f'Total Software Groups Suggestions Asset records {tRecords}')
    print(f'Total Software Groups Suggestions Asset records {tRecords}')


def pxc_software_group_suggestions_bug_list():
    # This API returns information on bugs, including ID, description, and affected software releases.
    # CSV Naming Convention: SoftwareGroup_Suggestions_Bug_Lists.csv
    # JSON Naming Convention:
    # {CustomerName}_SoftwareGroup_Suggestions_Bug_Lists_{Machine Suggestion ID}_Page_{page}_of_{total}.json
    logging.debug('********** Running Software Groups Suggestions Bug List Report *******\n')
    tRecords = 0
    while software_group_suggestionsFlag == 0:
        logging.debug(f'Waiting on Software Groups Suggestions Report to finish')
        time.sleep(1)
    print('Started Running Software Groups Suggestions Bug List Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(SWGroupSuggestionsBugList, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,machineSuggestionId,bugId,severity,title,state,' \
                         'affectedAssets,features'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(SWGroupSuggestionSummaries, 'r') as target:
        csvreader = csv.DictReader(target)
        for row in csvreader:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            machineSuggestionId = row['machineSuggestionId']
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            if row['successTrackId'] in insightList:
                token_time_check()
                logging.debug(f'Found machine suggestion ID of {machineSuggestionId} for {customerName}')
                url = (f'{pxc_url_customers}{customerId}{pxc_url_software_group_suggestions_bugs}?offset=1'
                       f'&max={max_items}&machineSuggestionId={machineSuggestionId}&successTrackId={successTrackId}')
            max_SWGroupSuggestionsBugList_attempts = 10
            SWGroupSuggestionsBugList_attempts = 0
            while SWGroupSuggestionsBugList_attempts < max_SWGroupSuggestionsBugList_attempts:
                # Make a request to API
                for i in range(10):
                    i += 1
                    logging.debug(f'Request attempt number {i}')
                    try:
                        response = requests.request('GET', url, headers=headers, data=data_payload,
                                                    verify=True, timeout=apiTimeout)
                        if i > 1:
                            logging.debug(f'API request retry # {i} was successful')
                        break
                    except Timeout:
                        logging.debug(f'Time out error getting resonse')
                # If not rate limited, break out of while loop and continue
                if response.status_code != 429:
                    if debug_level > 0:
                        logging.debug(f'Querying with {data_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if SWGroupSuggestionsBugList_attempts == 0:
                            logging.debug('Success')
                    break
                # If rate limited, wait and try again
                logging.debug(f'Rate Limit Exceeded for Software Groups Suggestions Bug List! Retrying.')
                time.sleep((1 ** SWGroupSuggestionsBugList_attempts) + random.random())
                logging.debug(f'Sleeping for {(2 ** SWGroupSuggestionsBugList_attempts) + random.random()} seconds')
                SWGroupSuggestionsBugList_attempts = SWGroupSuggestionsBugList_attempts + 1
            if SWGroupSuggestionsBugList_attempts > 0:
                logging.debug(f'Retry was successful for Software Groups Suggestions Bug List')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
            reply = json.loads(response.text)
            try:
                if response.status_code == 200:
                    totalCount = reply['totalCount']
                    pages = math.ceil(totalCount / int(max_items))
                    if debug_level > 0:
                        logging.debug(f'Total Pages:{pages}')
                        logging.debug(f'Total Records:{totalCount}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                    page = 0
                    while page < pages:
                        url = (f'{pxc_url_customers}{customerId}{pxc_url_software_group_suggestions_bugs}'
                               f'?offset={page + 1}&max={max_items}&machineSuggestionId={machineSuggestionId}'
                               f'&successTrackId={successTrackId}')
                        items = (get_json_reply(url,
                                                tag='items',
                                                report='Software Groups Suggestions Bug List Report'))
                        if outputFormat == 1 or outputFormat == 2:
                            with open(json_output_dir + customerName +
                                      '_SoftwareGroup_Suggestions_Bug_Lists_' + machineSuggestionId +
                                      '_Page_' + str(page + 1) +
                                      '_of_' + str(pages) +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                                logging.debug(f'Saving {json_file.name}')
                        if outputFormat == 1 or outputFormat == 3:
                            if items is not None:
                                for item in items:
                                    bugId = str(item['bugId'])
                                    severity = str(item['severity'])
                                    title = str(item['title']).replace(',', ' ')
                                    state = str(item['state'])
                                    affectedAssets = str(item['affectedAssets'])
                                    featureCount = str(item['features'])
                                    with open(SWGroupSuggestionsBugList, 'a', encoding='utf-8', newline='') \
                                            as temp_target:
                                        writer = csv.writer(temp_target,
                                                            delimiter=' ',
                                                            quotechar=' ',
                                                            quoting=csv.QUOTE_MINIMAL)
                                        CSV_Data = f'{customerId},{customerName},{successTrackId},' \
                                                   f'{machineSuggestionId},{bugId},{severity},{title},{state},' \
                                                   f'{affectedAssets},{featureCount}'
                                        writer.writerow(CSV_Data.split())
                                        tRecords += 1

                        else:
                            logging.debug('No Data Found .... Skipping')
                        page += 1
            except KeyError:
                logging.debug('No Data to process... Skipping.')
                pass
    logging.debug(f'Total Software Groups Suggestions Bug List records {tRecords}')
    print(f'Total Software Groups Suggestions Bug List records {tRecords}')


def pxc_software_group_suggestions_field_notices():
    # This API returns field notice information, including ID number, title, and publish date.
    # CSV Naming Convention: SoftwareGroup_Suggestions_Field_Notices.csv
    # JSON Naming Convention:
    # {Customer ID}_SoftwareGroup_Suggestions_Field_Notices_{Machine Suggestion ID}_Page_{page}_of_{total}.json
    logging.debug('****** Running Software Groups Suggestions Field Notices Report ******\n')
    tRecords = 0
    while software_group_suggestionsFlag == 0:
        logging.debug(f'Waiting on Software Groups Suggestions Report to finish')
        time.sleep(1)
    print('Started Running Software Groups Suggestions Field Notices Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(SWGroupSuggestionsFieldNotices, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,machineSuggestionId,fieldNoticeId,title,state,' \
                         'firstPublished,lastUpdated'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(SWGroupSuggestionSummaries, 'r') as target:
        csvreader = csv.DictReader(target)
        for row in csvreader:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            machineSuggestionId = row['machineSuggestionId']
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            logging.debug(f'Software Group Suggestions Field Notices for {customerName}'
                          f'on Success Track {successTrackId}')
            if row['successTrackId'] in insightList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}{pxc_url_software_group_suggestions_field_notices}?offset=0' \
                      f'&max={max_items}&machineSuggestionId={machineSuggestionId}&successTrackId={successTrackId}'
                max_SWGroupSuggestionsFieldNotices_attempts = 10
                SWGroupSuggestionsFieldNotices_attempts = 0
                while SWGroupSuggestionsFieldNotices_attempts < max_SWGroupSuggestionsFieldNotices_attempts:
                    # Make a request to API
                    for i in range(10):
                        i += 1
                        logging.debug(f'Request attempt number {i}')
                        try:
                            response = requests.request('GET', url, headers=headers, data=data_payload,
                                                        verify=True, timeout=apiTimeout)
                            if i > 1:
                                logging.debug(f'API request retry # {i} was successful')
                            break
                        except Timeout:
                            logging.debug(f'Time out error getting resonse')
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        if debug_level > 0:
                            logging.debug(f'Querying with {data_payload}')
                            logging.debug(f'URL Request: {url}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            if SWGroupSuggestionsFieldNotices_attempts == 0:
                                logging.debug('Success')
                        break
                    # If rate limited, wait and try again
                    logging.debug('Rate Limit Exceeded for Software Group Suggestions Field Notice Report! Retrying.')
                    if debug_level > 0:
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                    SWGroupSuggestionsFieldNotices_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if SWGroupSuggestionsFieldNotices_attempts > 0:
                    logging.debug(f'Retry was successful for Software Group Suggestions Field Notice Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                reply = json.loads(response.text)
            try:
                if response.status_code == 200:
                    totalCount = reply['totalCount']
                    pages = math.ceil(totalCount / int(max_items))
                    if debug_level > 0:
                        logging.debug(f'Total Pages:{pages}')
                        logging.debug(f'Total Records:{totalCount}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                    page = 0
                    while page < pages:
                        url = f'{pxc_url_customers}{customerId}{pxc_url_software_group_suggestions_field_notices}' \
                              f'?offset={page + 1}&max={max_items}&machineSuggestionId={machineSuggestionId}' \
                              f'&successTrackId={successTrackId}'
                        items = (get_json_reply(url,
                                                tag='items',
                                                report='Software Group Suggestions Field Notice Report'))
                        logging.debug(f'Found Software Group Suggestions Field Notice for {customerName}'
                                      f'with machine suggestion ID of {machineSuggestionId}')
                        if outputFormat == 1 or outputFormat == 2:
                            if items is not None:
                                if len(items) > 0:
                                    with open(json_output_dir + customerId +
                                              '_SoftwareGroup_Suggestions_Field_Notices_' + machineSuggestionId +
                                              '_Page_' + str(page + 1) +
                                              '_of_' + str(pages) +
                                              '.json', 'w') as json_file:
                                        json.dump(items, json_file)
                                    logging.debug(f'Saving {json_file.name}')
                        if outputFormat == 1 or outputFormat == 3:
                            if items is not None:
                                if len(items) > 0:
                                    for item in items:
                                        fieldNoticeId = str(item['fieldNoticeId']).replace(',', ' ')
                                        title = str(item['title']).replace(',', ' ')
                                        state = str(item['state']).replace(',', ' ')
                                        firstPublished = str(item['firstPublished']).replace(',', ' ')
                                        lastUpdated = str(item['lastUpdated']).replace(',', ' ')
                                        with open(SWGroupSuggestionsFieldNotices, 'a', encoding='utf-8', newline='') \
                                                as temp_target:
                                            writer = csv.writer(temp_target,
                                                                delimiter=' ',
                                                                quotechar=' ',
                                                                quoting=csv.QUOTE_MINIMAL)
                                            CSV_Data = f'{customerId},{customerName},{successTrackId},' \
                                                       f'{machineSuggestionId},{fieldNoticeId},{title},{state},' \
                                                       f'{firstPublished},{lastUpdated}'
                                            writer.writerow(CSV_Data.split())
                                            tRecords += 1

                        else:
                            logging.debug('No Data Found .... Skipping')
                        page += 1
            except KeyError:
                logging.debug('No Data to process... Skipping.')
                pass
    logging.debug(f'Total Software Groups Suggestions Field Notice records {tRecords}')
    print(f'Total Software Groups Suggestions Field Notice records {tRecords}')


def pxc_software_group_suggestions_advisories():
    # This API returns software advisory information, including ID number, version number, and severity level.
    # CSV Naming Convention: SoftwareGroup_Suggestions_Security_Advisories.csv
    # JSON Naming Convention:
    # {Customer ID}_SoftwareGroup_Suggestions_Security_Advisories_{Machine Suggestion ID}_Page_{page}_of_{total}.json
    logging.debug('*** Running Software Groups Suggestions Security Advisories Report ***\n')
    tRecords = 0
    while software_group_suggestionsFlag == 0:
        logging.debug(f'Waiting on Software Groups Suggestions Report to finish')
        time.sleep(1)
    print('Started Running Software Groups Suggestions Security Advisories Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(SWGroupSuggestionsAdvisories, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,machineSuggestionId,state,advisoryId,impact,title,' \
                         'updated,advisoryVersion'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(SWGroupSuggestionSummaries, 'r') as target:
        csvreader = csv.DictReader(target)
        for row in csvreader:
            token_time_check()
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            machineSuggestionId = row['machineSuggestionId']
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            logging.debug(f'Software Group Suggestions Security Advisories for {customerName}'
                          f' on Success Track {successTrackId}')
            url = (f'{pxc_url_customers}{customerId}{pxc_url_software_group_suggestions_security_advisories}'
                   f'?offset=1&max={max_items}&machineSuggestionId={machineSuggestionId}'
                   f'&successTrackId={successTrackId}')
            max_SWGroupSuggestionsAdvisories_attempts = 10
            SWGroupSuggestionsAdvisories_attempts = 0
            while SWGroupSuggestionsAdvisories_attempts < max_SWGroupSuggestionsAdvisories_attempts:
                # Make a request to API
                for i in range(10):
                    i += 1
                    logging.debug(f'Request attempt number {i}')
                    try:
                        response = requests.request('GET', url, headers=headers, data=data_payload,
                                                    verify=True, timeout=apiTimeout)
                        if i > 1:
                            logging.debug(f'API request retry # {i} was successful')
                        break
                    except Timeout:
                        logging.debug(f'Time out error getting resonse')
                # If not rate limited, break out of while loop and continue
                if response.status_code != 429:
                    if debug_level > 0:
                        logging.debug(f'Querying with {data_payload}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                        if SWGroupSuggestionsAdvisories_attempts == 0:
                            logging.debug('Success')
                    break
                # If rate limited, wait and try again
                logging.debug(
                    'Rate Limit Exceeded for Software Group Suggestions Security Advisories Report! Retrying.')
                if debug_level > 0:
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                SWGroupSuggestionsAdvisories_attempts += 1
                pause = random.random()
                logging.debug(f'sleeping for {pause} seconds')
                time.sleep(pause)
            if SWGroupSuggestionsAdvisories_attempts > 0:
                logging.debug(f'Retry was successful for Software Group Suggestions Security Advisories Report')
                logging.debug(f'URL Request: {url}')
                logging.debug(f'HTTP Code:{response.status_code}')
                logging.debug(f'Review API Headers:{response.headers}')
                logging.debug(f'Response Body:{response.content}')
            reply = json.loads(response.text)
            try:
                if response.status_code == 200:
                    totalCount = reply['totalCount']
                    pages = math.ceil(totalCount / int(max_items))
                    if debug_level > 0:
                        logging.debug(f'Total Pages:{pages}')
                        logging.debug(f'Total Records:{totalCount}')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                    page = 0
                    while page < pages:
                        url = (f'{pxc_url_customers}{customerId}'
                               f'{pxc_url_software_group_suggestions_security_advisories}?offset={page + 1}'
                               f'&max={max_items}&machineSuggestionId={machineSuggestionId}'
                               f'&successTrackId={successTrackId}')
                        items = (get_json_reply(url,
                                                tag='items',
                                                report='Software Group Suggestions Security Advisories Report'))
                        logging.debug(f'Found Software Group Suggestions Security Advisory for {customerName} '
                                      f'with machine suggestion ID of {machineSuggestionId}')
                        if outputFormat == 1 or outputFormat == 2:
                            if items is not None:
                                if len(items) > 0:
                                    with open(json_output_dir + customerId +
                                              '_Security_Advisories_' + machineSuggestionId +
                                              '_Page_' + str(page + 1) +
                                              '_of_' + str(pages) +
                                              '.json', 'w') as json_file:
                                        json.dump(items, json_file)
                                    logging.debug(f'Saving {json_file.name}')
                            else:
                                logging.debug('No Data Found .... Skipping')
                        if outputFormat == 1 or outputFormat == 3:
                            if items is not None:
                                if len(items) > 0:
                                    for item in items:
                                        state = str(item['state']).replace(',', ' ')
                                        advisoryId = str(item['advisoryId']).replace(',', ' ')
                                        impact = str(item['impact']).replace(',', ' ')
                                        title = str(item['title']).replace(',', ' ')
                                        updated = str(item['updated']).replace(',', ' ')
                                        advisoryVersion = str(item['advisoryVersion']).replace(',', ' ')
                                        with open(SWGroupSuggestionsAdvisories, 'a', encoding='utf-8', newline='') \
                                                as temp_target:
                                            writer = csv.writer(temp_target,
                                                                delimiter=' ',
                                                                quotechar=' ',
                                                                quoting=csv.QUOTE_MINIMAL)
                                            CSV_Data = f'{customerId},{customerName},{successTrackId},' \
                                                       f'{machineSuggestionId},{state},{advisoryId},{impact},{title},' \
                                                       f'{updated},{advisoryVersion}'
                                            writer.writerow(CSV_Data.split())
                                            tRecords += 1

                        else:
                            logging.debug('No Data Found .... Skipping')
                        page += 1
            except KeyError:
                logging.debug('No Data to process... Skipping.')
                pass
    logging.debug(f'Total Software Groups Suggestions Security Advisory records {tRecords}')
    print(f'Total Software Groups Suggestions Security Advisory records {tRecords}')


def pxc_afm_faults():
    # Functions to get the Automated Fault Management data from PX Cloud
    # This API returns fault information for the customerId provided.
    # CSV Naming Convention: Automated_Fault_Management_Faults.csv
    # JSON Naming Convention: {Customer ID}_Automated_Fault_Management_Faults_{Success Track ID}.json
    logging.debug('********** Running Automated Fault Management Faults Insights Report **********\n')
    tRecords = 0
    global afm_faultsFlag
    afm_faultsFlag = 0
    while contractswithcustomernamesFlag == 0:
        logging.debug(f'Waiting on Contract with Customer Names List to finish')
        time.sleep(1)
    print('Started Running Automated Fault Management Faults Insights Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(AFMFaults, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,severity,title,lastOccurence,condition,' \
                         'caseAutomation,faultId,category,openCases,affectedAssets,occurences,ignoredAssets,' \
                         'mgmtSystemType,mgmtSystemAddr,mgmtSystemHostName'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(OSV_AFM_List, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            if row['successTrackId'] in insightList:
                customerId = row['customerId']
                customerName = row['customerName']
                successTrackId = row['successTrackId']
                serviceLevel = row['serviceLevel']
                url = f'{pxc_url_customers}{customerId}{pxc_url_automated_fault_management_faults}' \
                      f'?successTrackId={successTrackId}&days={pxc_fault_days}&max={max_items}'
                logging.debug(f'Found Automated Fault Management Faults for {customerName}'
                              f' on Success Track {successTrackId} with sevice level {serviceLevel}')
                items = get_json_reply(url, tag='items', report='Automated Fault Management Faults Report')
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + customerId +
                                      '_Automated_Fault_Management_Faults_' + successTrackId +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                        else:
                            logging.debug('No Data Found ... Skipping')
                if items:
                    for item in items:
                        severity = item['severity'].replace(',', ' ')
                        title = item['title'].replace(',', ' ')
                        lastOccurence = str(item['lastOccurence'])
                        condition = item['condition'].replace(',', ' ')
                        caseAutomation = item['caseAutomation'].replace(',', ' ')
                        faultId = str(item['faultId'])
                        category = item['category'].replace(',', ' ')
                        openCases = str(item['openCases'])
                        affectedAssets = str(item['affectedAssets'])
                        occurences = str(item['occurences'])
                        ignoredAssets = str(item['ignoredAssets'])
                        mgmtSystemType = str(item['mgmtSystemType'])
                        mgmtSystemAddr = str(item['mgmtSystemAddr'])
                        try:
                            mgmtSystemHostName = str(item['mgmtSystemHostName'])
                        except KeyError:
                            mgmtSystemHostName = 'N/A'
                            pass
                        with open(AFMFaults, 'a', encoding='utf-8', newline='') as target:
                            writer = csv.writer(target,
                                                delimiter=' ',
                                                quotechar=' ',
                                                quoting=csv.QUOTE_MINIMAL)
                            CSV_Data = f'{customerId},{customerName},{successTrackId},{severity},{title},' \
                                       f'{lastOccurence},{condition},{caseAutomation},{faultId},{category},' \
                                       f'{openCases},{affectedAssets},{occurences},{ignoredAssets},{mgmtSystemType},' \
                                       f'{mgmtSystemAddr},{mgmtSystemHostName} '
                            writer.writerow(CSV_Data.split())
                            tRecords += 1

                else:
                    logging.debug('No Automated Fault Management Faults Data Found .... Skipping')
    afm_faultsFlag = 1
    logging.debug(f'Total Automated Fault Management Fault records {tRecords}')
    print(f'Total Automated Fault Management Fault records {tRecords}')


def pxc_afm_fault_summary():
    # This API returns detailed information for a fault based on the fault signatureId and customerId provided.
    # CSV Naming Convention: Automated_Fault_Management_Fault_Summary.csv
    # JSON Naming Convention:{Customer ID}_Automated_Fault_Management_Fault_Summary_{Fault ID}_{Success Track ID}.json
    logging.debug('****** Running Automated Fault Management Faults Summary Report ******\n')
    tRecords = 0
    while afm_faultsFlag == 0:
        logging.debug(f'Waiting on Automated Fault Management Faults Summary Report to finish')
        time.sleep(1)
    print('Started Running Automated Fault Management Faults Summary Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(AFMFaultSummary, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,faultId,suggestion,impact,description,severity,' \
                         'title,condition,category,supportedProductSeries'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(AFMFaults, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            if row['successTrackId'] in insightList:
                customerId = row['customerId']
                customerName = row['customerName']
                successTrackId = row['successTrackId']
                faultId = row['faultId']
                logging.debug(f'Automated Fault Management Fault Summary for {customerName}'
                              f' on Success Tracks {successTrackId} with Fault ID of {faultId}')
                url = (f'{pxc_url_customers}{customerId}{pxc_url_automated_fault_management_faults}/{faultId}'
                       f'{pxc_url_automated_fault_management_fault_summary}?successTrackId={successTrackId}')
                items = get_json_reply(url, tag='items', report='Automated Fault Management Fault Summary Report')
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + customerId +
                                      '_Automated_Fault_Management_Fault_Summary_' +
                                      faultId + '_' + successTrackId +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                if outputFormat == 1 or outputFormat == 3:
                    if items:
                        for item in items:
                            suggestion = item['suggestion'].replace(',', ' ')
                            impact = item['impact'].replace(',', ' ')
                            description = item['description'].replace(',', ' ')
                            severity = item['severity'].replace(',', ' ')
                            title = item['title'].replace(',', ' ')
                            condition = item['condition'].replace(',', ' ')
                            category = item['category'].replace(',', ' ')
                            supportedProductSeries = item['supportedProductSeries'].replace(',', ' ')
                            with open(AFMFaultSummary,
                                      'a', encoding='utf-8', newline='') as target:
                                writer = csv.writer(target,
                                                    delimiter=' ',
                                                    quotechar=' ',
                                                    quoting=csv.QUOTE_MINIMAL)
                                CSV_Data = f'{customerId},{customerName},{successTrackId},{faultId},{suggestion},' \
                                           f'{impact},{description},{severity},{title},{condition},{category},' \
                                           f'{supportedProductSeries}'
                                writer.writerow(CSV_Data.split())
                                tRecords += 1

            else:
                logging.debug('No Data Found .... Skipping')
    logging.debug(f'Total Automated Fault Management Faults Summary records {tRecords}')
    print(f'Total Automated Fault Management Faults Summary records {tRecords}')


def pxc_afm_affected_assets():
    # This API returns information about the customers assets affected by the fault.
    # CSV Naming Convention: Automated_Fault_Management_Fault_Summary.csv
    # JSON Naming Convention:
    # {Customer ID}_Automated_Fault_Management_Fault_Summary_Affected_Assets_{Fault ID}_{Success Track ID}.json
    logging.debug('***** Running Automated Fault Management Affected Assets Report ******\n')
    tRecords = 0
    while afm_faultsFlag == 0:
        logging.debug(f'Waiting on Automated Fault Management Faults Summary Report to finish')
        time.sleep(1)
    print('Started Running Automated Fault Management Affected Assets Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(AFMFaultAffectedAssets, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,faultId,assetName,productId,caseNumber,caseAction,' \
                         'occurrences,firstOccurrence,lastOccurrence,serialNumber,mgmtSystemType,mgmtSystemAddr,' \
                         'mgmtSystemHostName'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(AFMFaults, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            if row['successTrackId'] in insightList:
                customerId = row['customerId']
                customerName = row['customerName']
                successTrackId = row['successTrackId']
                faultId = row['faultId']
                logging.debug(f'Automated Fault Management Affected Assets for {customerName}'
                              f' on Success Tracks {successTrackId} with FaultId of {faultId}')
                url = (f'{pxc_url_customers}{customerId}{pxc_url_automated_fault_management_faults}/'
                       f'{faultId}{pxc_url_automated_fault_management_affected_assets}?successTrackId={successTrackId}'
                       f'&days={pxc_fault_days}&max={max_items}')
                items = get_json_reply(url, tag='items', report='Automated Fault Management Affected Assets Report')
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + customerId +
                                      '_Automated_Fault_Management_Fault_Summary_Affected_Assets_' +
                                      faultId + '_' + successTrackId +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                if outputFormat == 1 or outputFormat == 3:
                    if items:
                        for item in items:
                            assetName = str(item['assetName'].replace(',', ' '))
                            productId = str(item['productId'].replace(',', ' '))
                            caseNumber = str(item['caseNumber'])
                            if (item['caseAction']) is not None:
                                caseAction = str(item['caseAction'].replace(',', ' '))
                            else:
                                caseAction = 'None'
                            occurrences = str(item['occurrences'])
                            firstOccurrence = str(item['firstOccurrence'].replace(',', ' '))
                            lastOccurrence = str(item['lastOccurrence'].replace(',', ' '))
                            serialNumber = str(item['serialNumber'].replace(',', ' '))
                            mgmtSystemType = str(item['mgmtSystemType'])
                            mgmtSystemAddr = str(item['mgmtSystemAddr'])
                            try:
                                mgmtSystemHostName = str(item['mgmtSystemHostName'])
                            except KeyError:
                                mgmtSystemHostName = 'N/A'
                                pass
                            with open(AFMFaultAffectedAssets,
                                      'a', encoding='utf-8', newline='') as target:
                                writer = csv.writer(target,
                                                    delimiter=' ',
                                                    quotechar=' ',
                                                    quoting=csv.QUOTE_MINIMAL)
                                CSV_Data = f'{customerId},{customerName},{successTrackId},{faultId},{assetName},' \
                                           f'{productId},{caseNumber},{caseAction},{occurrences},{firstOccurrence},' \
                                           f'{lastOccurrence},{serialNumber},{mgmtSystemType},{mgmtSystemAddr},' \
                                           f'{mgmtSystemHostName}'
                                writer.writerow(CSV_Data.split())
                                tRecords += 1

            else:
                logging.debug('No Data Found .... Skipping')
    logging.debug(f'Total Automated Fault Management Affected Asset records {tRecords}')
    print(f'Total Automated Fault Management Affected Asset records {tRecords}')


def pxc_afm_fault_history():
    # This API returns fault history information for a customer's asset
    # CSV Naming Convention: Automated_Fault_Management_Fault_History.csv
    # JSON Naming Convention:
    # {Customer ID}_Automated_Fault_Management_Fault_History_{Fault ID}_{Success Track ID}.json
    logging.debug('******* Running Automated Fault Management Fault History Report ******\n')
    tRecords = 0
    while afm_faultsFlag == 0:
        logging.debug(f'Waiting on Automated Fault Management Faults Summary Report to finish')
        time.sleep(1)
    print('Started Running Automated Fault Management Fault History Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(AFMFaultHistory, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,faultId,assetName,status,failureMessage,lastOccurrence'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(AFMFaultAffectedAssets, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            if row['successTrackId'] in insightList:
                customerId = row['customerId']
                customerName = row['customerName']
                successTrackId = row['successTrackId']
                faultId = row['faultId']
                assetName = row['assetName']
                logging.debug(f'Automated Fault Management Fault History for {customerName}'
                              f' on Success Tracks {successTrackId} with FaultId of {faultId}')
                url = (f'{pxc_url_customers}{customerId}{pxc_url_automated_fault_management_faults}/{faultId}'
                       f'/affectedAssets/{assetName}?successTrackId={successTrackId}&days={pxc_fault_days}'
                       f'&max=2147483647')
                items = get_json_reply(url, tag='items', report='Automated Fault Management Fault History Report')
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + customerId +
                                      '_Automated_Fault_Management_Fault_History_' +
                                      faultId + '_' + successTrackId +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                if outputFormat == 1 or outputFormat == 3:
                    if items:
                        for item in items:
                            status = str(item['status'].replace(',', ' '))
                            failureMessage = str(item['failureMessage'].replace(',', ' '))
                            lastOccurrence = str(item['lastOccurrence'])
                            with open(AFMFaultHistory,
                                      'a', encoding='utf-8', newline='') as target:
                                writer = csv.writer(target,
                                                    delimiter=' ',
                                                    quotechar=' ',
                                                    quoting=csv.QUOTE_MINIMAL)
                                CSV_Data = f'{customerId},{customerName},{successTrackId},{faultId},{assetName},' \
                                           f'{status},{failureMessage},{lastOccurrence}'
                                writer.writerow(CSV_Data.split())
                                tRecords += 1

            else:
                logging.debug('No Data Found .... Skipping')
    logging.debug(f'Total Automated Fault Management Fault History records {tRecords}')
    print(f'Total Automated Fault Management Fault History records {tRecords}')


def pxc_compliance_violations():
    # Functions to get the Compliance Violations data from PX Cloud
    # This API returns information about the rules violated for the customerId provided
    # CSV Naming Convention: Regulatory_Compliance_Violations.csv
    # JSON Naming Convention:{Customer ID}_Regulatory_Compliance_Violations_{Success Track ID}.json
    logging.debug('****************** Running Compliance Violations Report **************\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    global compliance_violationsFlag
    compliance_violationsFlag = 0
    print('Started Running Compliance Violations Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(RCCComplianceViolations, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,severity,severityId,policyGroupId,policyGroupName,' \
                         'policyId,ruleId,policyName,ruleTitle,violationCount,affectedAssetsCount,swType,policyCategory'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            if successTrackId in insightList:
                token_time_check()
                url = f'{pxc_url_customers}{customerId}{pxc_url_compliance_violations}' \
                      f'?successTrackId={successTrackId}&days={pxc_fault_days}&max={max_items}'
                max_RCCComplianceViolations_attempts = 10
                RCCComplianceViolations_attempts = 0
                logging.debug(f'Querying data for Compliance Violations Report')
                while RCCComplianceViolations_attempts < max_RCCComplianceViolations_attempts:
                    # Make a request to API
                    for i in range(10):
                        i += 1
                        logging.debug(f'Request attempt number {i}')
                        try:
                            response = requests.request('GET', url, headers=headers, data=data_payload,
                                                        verify=True, timeout=apiTimeout)
                            if i > 1:
                                logging.debug(f'API request retry # {i} was successful')
                            break
                        except Timeout:
                            logging.debug(f'Time out error getting resonse')
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        if debug_level > 0:
                            logging.debug(f'Querying with {data_payload}')
                            logging.debug(f'URL Request: {url}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            if RCCComplianceViolations_attempts == 0:
                                logging.debug('Success')
                        break
                    # If rate limited, wait and try again
                    logging.debug('Rate Limit Exceeded for Compliance Violations Report! Retrying.')
                    if debug_level > 0:
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                    RCCComplianceViolations_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                if RCCComplianceViolations_attempts > 0:
                    logging.debug(f'Retry was successful for Compliance Violations Report')
                    logging.debug(f'URL Request: {url}')
                    logging.debug(f'HTTP Code:{response.status_code}')
                    logging.debug(f'Review API Headers:{response.headers}')
                    logging.debug(f'Response Body:{response.content}')
                reply = json.loads(response.text)
                page = 0
                try:
                    logging.debug(f'Found Compliance Violations for {customerName} on Success Track {successTrackId}')
                    if response.status_code == 403:
                        logging.debug('Customer admin has not provided access.')
                    if response.status_code == 200:
                        totalCount = reply['totalCount']
                        if totalCount == 0:
                            logging.debug('No Data Found .... Skipping')
                        if totalCount > 0:
                            logging.debug('Data Found ....Retreving Data.')
                        pages = math.ceil(totalCount / int(max_items))
                        if debug_level > 0:
                            logging.debug(f'Total Pages:{pages}')
                            logging.debug(f'Total Records:{totalCount}')
                            logging.debug(f'URL Request: {url}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        while page < pages:
                            url = f'{pxc_url_customers}{customerId}{pxc_url_compliance_violations}' \
                                  f'?successTrackId={successTrackId}&days={pxc_fault_days}' \
                                  f'offset={page + 1}&max={max_items}'
                            items = (get_json_reply(url, tag='items',
                                                    report='Regulatory Compliance Violations Report'))
                            if outputFormat == 1 or outputFormat == 2:
                                if items is not None:
                                    if len(items) > 0:
                                        with open(json_output_dir + customerId +
                                                  '_Regulatory_Compliance_Violations_' + successTrackId +
                                                  '_Page_' + str(page + 1) +
                                                  '_of_' + str(pages) +
                                                  '.json', 'w') as json_file:
                                            json.dump(items, json_file)
                                        logging.debug(f'Saving {json_file.name}')
                            if outputFormat == 1 or outputFormat == 3:
                                if items is not None:
                                    if len(items) > 0:
                                        for item in items:
                                            severity = item['severity'].replace(',', ' ')
                                            severityId = item['severityId'].replace(',', ' ')
                                            policyGroupId = str(item['policyGroupId']).replace(',', ' ')
                                            policyGroupName = item['policyGroupName'].replace(',', ' ')
                                            policyId = item['policyId'].replace(',', ' ')
                                            ruleId = str(item['ruleId']).replace(',', ' ')
                                            policyName = item['policyName'].replace(',', ' ')
                                            ruleTitle = str(item['ruleTitle']).replace(',', ' ')
                                            violationCount = str(item['violationCount'])
                                            affectedAssetsCount = str(item['affectedAssetsCount'])
                                            swType = str(item['swType'])
                                            policyCategory = str(item['policyCategory']).replace(',', ' ')
                                            with open(RCCComplianceViolations, 'a', encoding='utf-8',
                                                      newline='') as target:
                                                writer = csv.writer(target,
                                                                    delimiter=' ',
                                                                    quotechar=' ',
                                                                    quoting=csv.QUOTE_MINIMAL)
                                                CSV_Data = f'{customerId},{customerName},{successTrackId},{severity},' \
                                                           f'{severityId},{policyGroupId},{policyGroupName},' \
                                                           f'{policyId},{ruleId},{policyName},{ruleTitle},' \
                                                           f'{violationCount},{affectedAssetsCount},{swType},' \
                                                           f'{policyCategory}'
                                                writer.writerow(CSV_Data.split())
                                                tRecords += 1

                            page += 1
                except KeyError:
                    logging.debug('No Compliance Violations Data to process... Skipping.')
                    page -= 1
                    pass
    compliance_violationsFlag = 1
    logging.debug(f'Total Compliance Violation records {tRecords}')
    print(f'Total Compliance Violation records {tRecords}')


def pxc_assets_violating_compliance_rule():
    # Function to get Assets violating compliance rule data from PX Cloud
    # This API returns information about the customer assets in violation of the rule based customer, policy, and rules.
    # CSV Naming Convention: Regulatory_Compliance_Assets_violating_Compliance_Rule.csv
    # JSON Naming Convention:
    # {Customer ID}_Assets_Violating_Compliance_Rule_{Success Track ID}_{File #}.json
    logging.debug('*********** Running Assets Violating Compliance Rules Report *********\n')
    tRecords = 0
    while compliance_violationsFlag == 0:
        logging.debug(f'Waiting on Compliance Violations Report to finish')
        time.sleep(1)
    print('Started Running Assets Violating Compliance Rules Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(RCCAssetsViolatingComplianceRule, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,mgmtSystemHostname,mgmtSystemType,ipAddress,' \
                         'productFamily,violationCount,role,assetId,assetName,softwareType,softwareRelease,productId,' \
                         'severity,lastChecked,policyId,ruleId,scanStatus'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(RCCComplianceViolations, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        fileNum = 0
        for row in custList:
            if row['successTrackId'] in insightList:
                fileNum += 1
                customerId = row['customerId']
                customerName = row['customerName']
                successTrackId = row['successTrackId']
                policyCategory = row['policyCategory']
                policyGroupId = row['policyGroupId']
                policyId = row['policyId']
                ruleId = row['ruleId']
                severity = row['severity']
                logging.debug(f'Assets Violating Compliance Rule {ruleId} for {customerName} '
                              f'on Success Track {successTrackId}')
                url = f'{pxc_url_customers}{customerId}{pxc_url_compliance_violations_assets}' \
                      f'?policyCategory={policyCategory}&policyGroupId={policyGroupId}&policyId={policyId}' \
                      f'&ruleId={ruleId}&severity={severity}&successTrackId={successTrackId}&max={max_items}'
                items = (get_json_reply(url, tag='items', report='Assets Violating Compliance Rule Report'))
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + customerId +
                                      '_Assets_Violating_Compliance_Rule_' + successTrackId +
                                      '_Page_' + str(fileNum) +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                if outputFormat == 1 or outputFormat == 3:
                    try:
                        if items is not None:
                            if len(items) > 0:
                                assetCount = 0
                                for item in items:
                                    assetCount += 1
                                    mgmtSystemHostname = item['mgmtSystemHostname'].replace(',', ' ')
                                    mgmtSystemType = item['mgmtSystemType'].replace(',', ' ')
                                    ipAddress = str(item['ipAddress']).replace(',', ' ')
                                    productFamily = str(item['productFamily']).replace(',', ' ')
                                    violationCount = str(item['violationCount']).replace(',', ' ')
                                    role = str(item['role']).replace(',', ' ')
                                    assetId = str(item['assetId']).replace(',', '_')
                                    assetName = item['assetName'].replace(',', ' ')
                                    softwareType = item['softwareType'].replace(',', ' ')
                                    softwareRelease = item['softwareRelease'].replace(',', ' ')
                                    productId = item['productId'].replace(',', ' ')
                                    severity = str(item['severity']).replace(',', ' ')
                                    lastChecked = str(item['lastChecked']).replace(',', ' ')
                                    scanStatus = str(item['scanStatus']).replace(',', ' ')
                                    with open(RCCAssetsViolatingComplianceRule, 'a',
                                              encoding='utf-8',
                                              newline='') \
                                            as target:
                                        writer = csv.writer(target,
                                                            delimiter=' ',
                                                            quotechar=' ',
                                                            quoting=csv.QUOTE_MINIMAL)
                                        CSV_Data = f'{customerId},{customerName},{successTrackId},' \
                                                   f'{mgmtSystemHostname},{mgmtSystemType},{ipAddress},' \
                                                   f'{productFamily},{violationCount},{role},{assetId},{assetName}' \
                                                   f',{softwareType},{softwareRelease},{productId},{severity},' \
                                                   f'{lastChecked},{policyId},{ruleId},{scanStatus}'
                                        writer.writerow(CSV_Data.split())
                                        tRecords += 1

                                logging.debug(f'Number of assets found {assetCount}')
                            else:
                                logging.debug('No Data Found .... Skipping')
                    except KeyError:
                        logging.debug('No Data to process... Skipping.')
                        pass
    logging.debug(f'Total Assets Violating Compliance Rule records {tRecords}')
    print(f'Total Assets Violating Compliance Rule records {tRecords}')


def pxc_compliance_rule_details():
    # Function to get Assets violating compliance rule data from PX Cloud
    # This API returns information about the policy the rule belongs to.
    # CSV Naming Convention: Regulatory_Compliance_Policy_Rule_Details.csv
    # JSON Naming Convention:
    # {Customer ID}_Policy_Rule_Details_{Success Track ID}_{Page #}.json
    logging.debug('***************** Running Compliance Rule Detail Report **************\n')
    tRecords = 0
    while compliance_violationsFlag == 0:
        logging.debug(f'Waiting on Compliance Violations Report to finish')
        time.sleep(1)
    print('Started Running Compliance Rule Detail Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(RCCPolicyRuleDetails, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,policyName,policyDescription,ruleId,policyId'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(RCCComplianceViolations, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        fileNum = 0
        for row in custList:
            if row['successTrackId'] in insightList:
                token_time_check()
                fileNum += 1
                customerId = row['customerId']
                customerName = row['customerName']
                successTrackId = row['successTrackId']
                policyCategory = row['policyCategory']
                policyGroupId = row['policyGroupId']
                policyId = row['policyId']
                ruleId = row['ruleId']
                severity = row['severity']
                logging.debug(f'Compliance Rule Detail for {customerName} on Success Track {successTrackId}')
                url = f'{pxc_url_customers}{customerId}{pxc_url_compliance_policy_rule_details}' \
                      f'?policyCategory={policyCategory}&policyGroupId={policyGroupId}&policyId={policyId}' \
                      f'&ruleId={ruleId}&severity={severity}&successTrackId={successTrackId}&max={max_items}'
                try:
                    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                    max_RCCPolicyRuleDetails_attempts = 10
                    RCCPolicyRuleDetails_attempts = 0
                    while RCCPolicyRuleDetails_attempts < max_RCCPolicyRuleDetails_attempts:
                        # Make a request to API
                        for i in range(10):
                            i += 1
                            logging.debug(f'Request attempt number {i}')
                            try:
                                response = requests.request('GET', url, headers=headers, data=data_payload,
                                                            verify=True, timeout=apiTimeout)
                                if i > 1:
                                    logging.debug(f'API request retry # {i} was successful')
                                break
                            except Timeout:
                                logging.debug(f'Time out error getting resonse')
                        # If not rate limited, break out of while loop and continue
                        if response.status_code != 429:
                            if debug_level > 0:
                                logging.debug(f'Querying with {data_payload}')
                                logging.debug(f'URL Request: {url}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if RCCPolicyRuleDetails_attempts == 0:
                                    logging.debug('Success')
                            break
                        # If rate limited, wait and try again
                        logging.debug('Rate Limit Exceeded for Compliance Rule Detail Report! Retrying.')
                        time.sleep((1 ** RCCPolicyRuleDetails_attempts) + random.random())
                        logging.debug(f'Sleeping for {(2 ** RCCPolicyRuleDetails_attempts) + random.random()} seconds')
                        RCCPolicyRuleDetails_attempts = RCCPolicyRuleDetails_attempts + 1
                    if RCCPolicyRuleDetails_attempts > 0:
                        logging.debug(f'Retry was successful for Compliance Rule Detail Report')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                    reply = json.loads(response.text)
                    if response.status_code == 200:
                        items = {'items': [reply]}
                        policyName = str(reply['policyName'])
                        policyDescription = str(reply['policyDescription'])
                        ruleId = str(reply['ruleId'])
                        policyId = str(reply['policyId'])
                        logging.debug(f'Found Policy {policyName}')
                    if debug_level == 2:
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                except Exception as Error:
                    logging.debug(f'Failed to collect {Error} due to reply from API: ', reply['message'])
                    items = []
                finally:
                    if outputFormat == 1 or outputFormat == 2:
                        with open(json_output_dir + customerId +
                                  '_Policy_Rule_Details_' + successTrackId +
                                  '_Page_' + str(fileNum) +
                                  '.json', 'w') as json_file:
                            json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                    if outputFormat == 1 or outputFormat == 3:
                        with open(RCCPolicyRuleDetails, 'a', encoding='utf-8', newline='') as target:
                            writer = csv.writer(target,
                                                delimiter=' ',
                                                quotechar=' ',
                                                quoting=csv.QUOTE_MINIMAL)
                            CSV_Data = f'{customerId},{customerName},{successTrackId},{policyName},' \
                                       f'{policyDescription},{ruleId},{policyId}'
                            writer.writerow(CSV_Data.split())
                            tRecords += 1
    logging.debug(f'Total Compliance Rule Detail records {tRecords}')
    print(f'Total Compliance Rule Detail records {tRecords}')


def pxc_compliance_suggestions():
    # Functions to get the Compliance Suggestions data from PX Cloud
    # This API returns information about the violated rule conditions based on the policy, and rule information.
    # CSV Naming Convention: Compliance_Suggestions.csv
    # JSON Naming Convention:{Customer ID}_Compliance_Suggestions__{Success Track ID}_{Page #}.json
    logging.debug('***************** Running Compliance Suggestions Report **************\n')
    tRecords = 0
    while compliance_violationsFlag == 0:
        logging.debug(f'Waiting on Compliance Violations Report to finish')
        time.sleep(1)
    print('Started Running Compliance Suggestions Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(RCCComplianceSuggestions, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,policyId,policyCategory,policyGroupId,ruleId,' \
                         'severity,violationMessage,suggestion,affectedAssetsCount'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(RCCComplianceViolations, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        fileNum = 0
        for row in custList:
            if row['successTrackId'] in insightList:
                token_time_check()
                fileNum += 1
                customerId = row['customerId']
                customerName = row['customerName']
                successTrackId = row['successTrackId']
                policyId = row['policyId']
                policyCategory = row['policyCategory']
                policyGroupId = row['policyGroupId']
                ruleId = row['ruleId']
                logging.debug(f'Compliance Suggestions for Customer :{customerName} on Success Track {successTrackId}')
                headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                url = (f'{pxc_url_customers}{customerId}{pxc_url_compliance_suggestions}'
                       f'?successTrackId={successTrackId}&policyId={policyId}&policyCategory={policyCategory}'
                       f'&policyGroupId={policyGroupId}&ruleId={ruleId}&max={max_items}')
                if fileNum == 1:
                    logging.debug(f'Found Compliance Suggestions on Success Track {successTrackId} '
                                  f'for {customerName}')
                max_RCCComplianceViolations_attempts = 10
                RCCComplianceViolations_attempts = 0
                while RCCComplianceViolations_attempts < max_RCCComplianceViolations_attempts:
                    # Make a request to API
                    for i in range(10):
                        i += 1
                        logging.debug(f'Request attempt number {i}')
                        try:
                            response = requests.request('GET', url, headers=headers, data=data_payload,
                                                        verify=True, timeout=apiTimeout)
                            if i > 1:
                                logging.debug(f'API request retry # {i} was successful')
                            break
                        except Timeout:
                            logging.debug(f'Time out error getting resonse')
                    # If not rate limited, break out of while loop and continue
                    if response.status_code != 429:
                        if debug_level > 0:
                            logging.debug(f'Querying with {data_payload}')
                            logging.debug(f'URL Request: {url}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                            if RCCComplianceViolations_attempts == 0:
                                logging.debug('Success')
                        break
                    # If rate limited, wait and try again
                    logging.debug('Rate Limit Exceeded for Compliance Suggestions on Success Track Report! Retrying.')
                    if debug_level > 0:
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                    RCCComplianceViolations_attempts += 1
                    pause = random.random()
                    logging.debug(f'sleeping for {pause} seconds')
                    time.sleep(pause)
                reply = json.loads(response.text)
                if debug_level > 0:
                    logging.debug(reply)
                try:
                    if response.status_code == 200:
                        totalCount = reply['totalCount']
                        pages = math.ceil(totalCount / int(max_items))
                        if debug_level > 0:
                            logging.debug(f'Total Pages:{pages}')
                            logging.debug(f'Total Records:{totalCount}')
                            logging.debug(f'URL Request: {url}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                        page = 0
                        while page < pages:
                            url = (f'{pxc_url_customers}{customerId}{pxc_url_compliance_suggestions}'
                                   f'?successTrackId={successTrackId}&policyId={policyId}'
                                   f'&policyCategory={policyCategory}&policyGroupId={policyGroupId}&ruleId={ruleId}'
                                   f'&days={pxc_fault_days}&max={max_items}')
                            items = (get_json_reply(url, tag='items', report='Compliance Suggestions Report'))
                            if items is not None:
                                if len(items) > 0:
                                    if outputFormat == 1 or outputFormat == 2:
                                        with open(json_output_dir + customerId +
                                                  '_Compliance_Suggestions_' + successTrackId + '_Page_' +
                                                  str(fileNum) + '.json', 'w') as json_file:
                                            json.dump(items, json_file)
                                        logging.debug(f'Saving {json_file.name}')
                                    if outputFormat == 1 or outputFormat == 3:
                                        for item in items:
                                            severity = item['severity'].replace(',', ' ')
                                            violationMessage = item['violationMessage'].replace(',', ' ')
                                            suggestion = str(item['suggestion']).replace(',', ' ')
                                            affectedAssetsCount = str(item['affectedAssetsCount'])
                                            logging.debug(f'Violation: {violationMessage}')
                                            with open(RCCComplianceSuggestions, 'a', encoding='utf-8', newline='') \
                                                    as target:
                                                writer = csv.writer(target,
                                                                    delimiter=' ',
                                                                    quotechar=' ',
                                                                    quoting=csv.QUOTE_MINIMAL)
                                                CSV_Data = f'{customerId},{customerName},{successTrackId},{policyId},' \
                                                           f'{policyCategory},{policyGroupId},{ruleId},{severity},' \
                                                           f'{violationMessage},{suggestion},{affectedAssetsCount}'
                                                writer.writerow(CSV_Data.split())
                                                tRecords += 1
                                else:
                                    logging.debug('No Data Found .... Skipping')
                            page += 1
                except KeyError:
                    logging.debug('No Data to process... Skipping.')
                    pass
    logging.debug(f'Total Compliance Rule Suggestion records {tRecords}')
    print(f'Total Compliance Rule Suggestion records {tRecords}')


def pxc_assets_with_violations():
    # Function to get the Assets with Violations data from PX Cloud
    # This API returns information about assets that have at least one rule violation based on the customerId provided.
    # CSV Naming Convention: Regulatory_Compliance_Assets_With_Violations.csv
    # JSON Naming Convention: {Customer ID}_Assets_with_Violations_{successTrackId}.json
    logging.debug('***************** Running Assets with Violations Report **************\n')
    tRecords = 0
    global assets_with_violationsFlag
    assets_with_violationsFlag = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Assets with Violations Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(RCCAssetsWithViolations, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,ipAddress,serialNumber,violationCount,assetGroup,' \
                         'role,sourceSystemId,assetId,assetName,lastChecked,softwareType,softwareRelease,severity,' \
                         'severityId,policyId,ruleId,scanStatus'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(RCCComplianceViolations, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        fileNum = 0
        for row in custList:
            time.sleep(1)
            fileNum += 1
            if row['successTrackId'] in insightList:
                customerName = row['customerName']
                customerId = row['customerId']
                successTrackId = row['successTrackId']
                policyId = row['policyId']
                ruleId = row['ruleId']
                url = f'{pxc_url_customers}{customerId}{pxc_url_compliance_assets_with_violations}' \
                      f'?successTrackId={successTrackId}'
                if fileNum == 1:
                    logging.debug(f'Found Customer {customerName} on Success Track {successTrackId}')
                items = (get_json_reply(url, tag='items', report='Assets with Violations Report'))
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(
                                    json_output_dir + customerId + '_Assets_with_Violations_' + successTrackId +
                                    '_Page_' + str(fileNum) + '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                if outputFormat == 1 or outputFormat == 3:
                    if items is not None:
                        if len(items) > 0:
                            for item in items:
                                ipAddress = str(item['ipAddress'])
                                serialNumber = str(item['serialNumber'])
                                violationCount = str(item['violationCount'])
                                assetGroups = str(item['assetGroups'])
                                role = str(item['role'])
                                sourceSystemId = str(item['sourceSystemId'])
                                assetId = str(item['assetId']).replace(',', '_')
                                assetName = str(item['assetName'])
                                lastChecked = str(item['lastChecked'])
                                softwareType = str(item['softwareType'])
                                softwareRelease = str(item['softwareRelease'])
                                severity = str(item['severity'])
                                severityId = str(item['severityId'])
                                scanStatus = str(item['scanStatus'])
                                logging.debug(f'Violation of {ruleId} on asset: {assetId} for {customerName}'
                                              f' on Success Track {successTrackId}')
                                with open(RCCAssetsWithViolations, 'a', encoding='utf-8', newline='') as target:
                                    writer = csv.writer(target,
                                                        delimiter=' ',
                                                        quotechar=' ',
                                                        quoting=csv.QUOTE_MINIMAL)
                                    CSV_Data = f'{customerId},{customerName},{successTrackId},{ipAddress},' \
                                               f'{serialNumber},{violationCount},{assetGroups},{role},' \
                                               f'{sourceSystemId},{assetId},{assetName},{lastChecked},{softwareType},' \
                                               f'{softwareRelease},{severity},{severityId},{policyId},{ruleId},' \
                                               f'{scanStatus}'
                                    writer.writerow(CSV_Data.split())
                                    tRecords += 1
            else:
                logging.debug('No Assets with Violations Data Found .... Skipping')
    assets_with_violationsFlag = 1
    logging.debug(f'Total Assets with Violation records {tRecords}')
    print(f'Total Assets with Violation records {tRecords}')


def pxc_asset_violations():
    # Function to get the Asset Violations data from PX Cloud
    # This API returns information about the rules violated by an asset based on the information provided.
    # CSV Naming Convention: Regulatory_Compliance_Asset_Violations.csv
    # JSON Naming Convention: {Customer ID}_Asset_Violations_{sourceSystemId}_Page_#.json
    logging.debug('****************** Running Assets Violations Report ******************\n')
    tRecords = 0
    while assets_with_violationsFlag == 0:
        logging.debug(f'Waiting on Assets with Violations Report to finish')
        time.sleep(1)
    print('Started Running Assets Violations Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(RCCAssetViolations, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,sourceSystemId,assetId,successTrackId,severity,regulatoryType,' \
                         'violationMessage,suggestion,violationAge,policyDescription,ruleTitle,ruleDescription'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(RCCAssetsWithViolations, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        fileNum = 0
        for row in custList:
            if row['successTrackId'] in insightList:
                fileNum += 1
                customerId = row['customerId']
                customerName = row['customerName']
                sourceSystemId = row['sourceSystemId']
                assetId = row['assetId'].replace('_', ',')
                successTrackId = row['successTrackId']
                url = (f'{pxc_url_customers}{customerId}{pxc_url_compliance_asset_violations}'
                       f'?sourceSystemId={sourceSystemId}&assetId={assetId}&successTrackId={successTrackId}'
                       f'&max={max_items}')
                if fileNum == 1:
                    logging.debug(f'Found {customerName} on Success Track {successTrackId}')
                items = (get_json_reply(url, tag='items', report='Asset Violations Report'))
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + customerId +
                                      '_Asset_Violations_' + sourceSystemId +
                                      '_Page_' + str(fileNum) +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                try:
                    if outputFormat == 1 or outputFormat == 3:
                        if items is not None:
                            if len(items) > 0:
                                for item in items:
                                    assetId = str(row['assetId']).replace(',', '_')
                                    severity = str(item['severity']).replace(',', ' ')
                                    regulatoryType = str(item['regulatoryType']).replace(',', ' ')
                                    violationMessage = str(item['violationMessage']).replace(',', ' ')
                                    suggestion = str(item['suggestion']).replace(',', ' ')
                                    violationAge = str(item['violationAge']).replace(',', ' ')
                                    policyDescription = str(item['policyDescription']).replace(',', ' ')
                                    ruleTitle = str(item['ruleTitle']).replace(',', ' ')
                                    ruleDescription = str(item['ruleDescription']).replace(',', ' ')
                                    logging.debug(f'{severity} Violation of {regulatoryType} Regulations with '
                                                  f'asset:{assetId} on Success Track {successTrackId}')
                                    with open(RCCAssetViolations, 'a', encoding='utf-8', newline='') as target:
                                        writer = csv.writer(target,
                                                            delimiter=' ',
                                                            quotechar=' ',
                                                            quoting=csv.QUOTE_MINIMAL)
                                        CSV_Data = f'{customerId},{customerName},{sourceSystemId},{assetId},' \
                                                   f'{successTrackId},{severity},{regulatoryType},{violationMessage},' \
                                                   f'{suggestion},{violationAge},{policyDescription},{ruleTitle},' \
                                                   f'{ruleDescription}'
                                        writer.writerow(CSV_Data.split())
                                        tRecords += 1
                except KeyError:
                    logging.debug('No Data to process... Skipping.')
                    pass
    logging.debug(f'Total Assets Violation records {tRecords}')
    print(f'Total Assets Violation records {tRecords}')


def pxc_optin():
    # Function to get the OptIn data from PX Cloud
    # This API returns information about whether the customer has successfully configured the regulatory compliance
    #    feature and has violation data available
    # CSV Naming Convention: Regulatory_Compliance_Obtained.csv
    # JSON Naming Convention: {Customer ID}_Obtained_{successTrackId}.json
    logging.debug('************* Running Regulatory Compliance Obtained Report **********\n')
    tRecords = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Regulatory Compliance Obtained Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(RCCObtained, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTracksId,status,hasQualifiedAssets'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            if debug_level > 0:
                logging.debug(f'Obtained data for {customerName} on Success Track {successTrackId}')
            if row['successTrackId'] in insightList:
                token_time_check()
                logging.debug(f'Found Customer {customerName} on Success Track {successTrackId}')
                url = f'{pxc_url_customers}{customerId}{pxc_url_compliance_optin}?successTrackId={successTrackId}'
                try:
                    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
                    max_RCCObtained_attempts = 10
                    RCCObtained_attempts = 0
                    while RCCObtained_attempts < max_RCCObtained_attempts:
                        # Make a request to API
                        for i in range(10):
                            i += 1
                            logging.debug(f'Request attempt number {i}')
                            try:
                                response = requests.request('GET', url, headers=headers, data=data_payload,
                                                            verify=True, timeout=apiTimeout)
                                if i > 1:
                                    logging.debug(f'API request retry # {i} was successful')
                                break
                            except Timeout:
                                logging.debug(f'Time out error getting resonse')
                        # If not rate limited, break out of while loop and continuee
                        if response.status_code != 429:
                            if debug_level > 0:
                                logging.debug(f'Querying with {data_payload}')
                                logging.debug(f'URL Request: {url}')
                                logging.debug(f'HTTP Code:{response.status_code}')
                                logging.debug(f'Review API Headers:{response.headers}')
                                logging.debug(f'Response Body:{response.content}')
                                if RCCObtained_attempts == 0:
                                    logging.debug('Success')
                            break
                        # If rate limited, wait and try again
                        logging.debug('Rate Limit Exceeded for Regulatory Compliance Obtained Report! Retrying...')
                        time.sleep((1 ** RCCObtained_attempts) + random.random())
                        logging.debug(f'sleeping for {(2 ** RCCObtained_attempts) + random.random()} seconds')
                        RCCObtained_attempts = RCCObtained_attempts + 1
                        logging.debug(f'Number of Attempts {RCCObtained_attempts}')
                    if RCCObtained_attempts > 0:
                        logging.debug(f'Retry was successful for Regulatory Compliance Obtained Report')
                        logging.debug(f'URL Request: {url}')
                        logging.debug(f'HTTP Code:{response.status_code}')
                        logging.debug(f'Review API Headers:{response.headers}')
                        logging.debug(f'Response Body:{response.content}')
                    reply = json.loads(response.text)
                    if response.status_code == 200:
                        if len(reply) > 0:
                            items = {'items': [reply]}
                            status = str(reply['status'])
                            hasQualifiedAssets = str(reply['hasQualifiedAssets'])
                        if debug_level == 2:
                            logging.debug(f'URL Request: {url}')
                            logging.debug(f'HTTP Code:{response.status_code}')
                            logging.debug(f'Review API Headers:{response.headers}')
                            logging.debug(f'Response Body:{response.content}')
                    if outputFormat == 1 or outputFormat == 2:
                        if len(items) > 0:
                            if items is not None:
                                with open(json_output_dir + customerId + '_Obtained_' + successTrackId + '.json',
                                          'w') as json_file:
                                    json.dump(items, json_file)
                                logging.debug(f'Saving {json_file.name}')
                    if outputFormat == 1 or outputFormat == 3:
                        if len(items) > 0:
                            if items is not None:
                                with open(RCCObtained, 'a', encoding='utf-8', newline='') as target:
                                    writer = csv.writer(target,
                                                        delimiter=' ',
                                                        quotechar=' ',
                                                        quoting=csv.QUOTE_MINIMAL)
                                    CSV_Data = f'{customerId},{customerName},{successTrackId},{status},' \
                                               f'{hasQualifiedAssets}'
                                    writer.writerow(CSV_Data.split())
                                    tRecords += 1

                except Exception as Error:
                    if response.text.__contains__('Customer admin has not provided access.'):
                        if debug_level == 2:
                            logging.debug(f'Response Body:{response.content}')
                            logging.debug(Error)
                        logging.debug('Customer admin has not provided access....Skipping')
    logging.debug(f'Total Regulatory Compliance Obtained records {tRecords}')
    print(f'Total Regulatory Compliance Obtained records {tRecords}')


def pxc_crash_risk_assets():
    # Functions to get the Crash Risk Assets data from PX Cloud
    # This API returns information about assets with a crash risk value of Medium or High for the customerId provided.
    # CSV Naming Convention: Crash_Risk_Assets.csv
    # JSON Naming Convention:{Customer ID}_Crash_Risk_Assets_{Success Track ID}.json
    logging.debug('***** Running Risk Mitigation Checks on Crash Risk Assets Report *****\n')
    tRecords = 0
    global crash_risk_assetsFlag
    crash_risk_assetsFlag = 0
    while customersFlag == 0:
        logging.debug(f'Waiting on Customer List to finish')
        time.sleep(1)
    print('Started Running Risk Mitigation Checks on Crash Risk Assets Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(CrashRiskAssets, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,crashPredicted,assetId,assetUniqueId,assetName,' \
                         'ipAddress,productId,productFamily,softwareRelease,softwareType,serialNumber,risk,endDate'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(customers, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            if successTrackId in insightList:
                logging.debug(f'Found {customerName} on Success Track {successTrackId}')
                url = f'{pxc_url_customers}{customerId}{pxc_url_crash_risk_assets}' \
                      f'?successTrackId={successTrackId}'  # &max={max_items}'
                items = get_json_reply(url, tag='items', report='Crash Risk Assets Report')
                crashPredicted = get_json_reply(url, tag='crashPredicted', report='Crash Risk Assets Report')
                if items is not None:
                    if len(items) > 0:
                        if outputFormat == 1 or outputFormat == 2:
                            with open(json_output_dir + customerId +
                                      '_Crash_Risk_Assets_' + successTrackId +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                        if outputFormat == 1 or outputFormat == 3:
                            if items:
                                for item in items:
                                    assetId = (item['assetId'].lower()).replace(',', '_')
                                    assetId_bytes = assetId.lower().encode('ascii')
                                    assetIdBase64_bytes = base64.b64encode(assetId_bytes)
                                    assetUniqueId = assetIdBase64_bytes.decode('ascii')
                                    assetName = str(item['assetName']).replace(',', ' ')
                                    ipAddress = str(item['ipAddress'])
                                    productId = str(item['productId']).replace(',', ' ')
                                    productFamily = str(item['productFamily']).replace(',', ' ')
                                    softwareRelease = str(item['softwareRelease'])
                                    softwareType = str(item['softwareType']).replace(',', ' ')
                                    serialNumber = str(item['serialNumber'])
                                    risk = str(item['risk'])
                                    endDate = str(item['endDate'])
                                    if debug_level > 0:
                                        logging.debug(f'assetId From CSV: {assetId}')
                                        logging.debug(f'assetId converted: {assetId_bytes}')
                                        logging.debug(f'assetUniqueId: {assetUniqueId}')
                                    with open(CrashRiskAssets, 'a', encoding='utf-8', newline='') as target:
                                        writer = csv.writer(target,
                                                            delimiter=' ',
                                                            quotechar=' ',
                                                            quoting=csv.QUOTE_MINIMAL)
                                        CSV_Data = f'{customerId},{customerName},{successTrackId},{crashPredicted},' \
                                                   f'{assetId},{assetUniqueId},{assetName},{ipAddress},{productId},' \
                                                   f'{productFamily},{softwareRelease},{softwareType},{serialNumber},' \
                                                   f'{risk},{endDate}'
                                        writer.writerow(CSV_Data.split())
                                        tRecords += 1
    crash_risk_assetsFlag = 1
    logging.debug(f'Total Risk Mitigation Checks on Crash Risk Asset records {tRecords}')
    print(f'Total Risk Mitigation Checks on Crash Risk Asset records {tRecords}')


def pxc_crash_risk_factors():
    # Functions to get the Crash Risk Factors data from PX Cloud
    # This API returns the risk factors that contribute to the crash risk value of the asset.
    # CSV Naming Convention: Crash_Risk_Factors.csv
    # JSON Naming Convention:{Customer ID}_Crash_Risk_Factors_{Success Track ID}_{Asset ID}.json
    logging.debug('***** Running Risk Mitigation Checks on Crash Risk Factors Report ****\n')
    tRecords = 0
    while crash_risk_assetsFlag == 0:
        logging.debug(f'Waiting on Risk Mitigation Checks on Crash Risk Assets Report to finish')
        time.sleep(1)
    print('Started Running Risk Mitigation Checks on Crash Risk Factors Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(CrashRiskFactors, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,assetId,assetUniqueId,factor,factorType'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(CrashRiskAssets, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            assetId = row['assetId'].replace('/', '-')
            assetUniqueId = row['assetUniqueId']
            if row['successTrackId'] in insightList:
                logging.debug(f'Found {customerName} with Asset {assetId}')
                url = (f'{pxc_url_customers}{customerId}{pxc_url_crash_risk_assets}/{assetUniqueId}'
                       f'{pxc_url_crash_risk_factors}?successTrackId={successTrackId}&max={max_items}')
                items = get_json_reply(url, tag='items', report='Crash Risk Factors Report')
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + customerId +
                                      '_Crash_Risk_Factors_' + successTrackId + '_' + assetId.upper() +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                if outputFormat == 1 or outputFormat == 3:
                    if items is not None:
                        if len(items) > 0:
                            for item in items:
                                factor = str(item['factor']).replace(',', ' ')
                                factorType = str(item['factorType']).replace(',', ' ')
                                with open(CrashRiskFactors, 'a', encoding='utf-8', newline='') as target:
                                    writer = csv.writer(target,
                                                        delimiter=' ',
                                                        quotechar=' ',
                                                        quoting=csv.QUOTE_MINIMAL)
                                    CSV_Data = f'{customerId},{customerName},{successTrackId},{assetId},' \
                                               f'{assetUniqueId},{factor},{factorType}'
                                    writer.writerow(CSV_Data.split())
                                    tRecords += 1
            else:
                logging.debug('No Data Found .... Skipping')
    logging.debug(f'Total Risk Mitigation Checks on Crash Risk Factor records {tRecords}')
    print(f'Total Risk Mitigation Checks on Crash Risk Factor records {tRecords}')


def pxc_similar_assets():
    # Functions to get the Similar Asset's data from PX Cloud
    # This API returns information about similar assets with a lower crash risk rating than the specified assetId
    #    based on the similarityCriteria and customerId provided.
    # CSV Naming Convention: Similar_Assets.csv
    # JSON Naming Convention:{Customer ID}_Similar_Assets_{Success Track ID}_{Asset ID}_{Features}.json
    logging.debug('******* Running Risk Mitigation Checks on Similar Assets Report ******\n')
    tRecords = 0
    while crash_risk_assetsFlag == 0:
        logging.debug(f'Waiting on Risk Mitigation Checks on Crash Risk Assets Report to finish')
        time.sleep(1)
    print('Started Running Risk Mitigation Checks on Similar Assets Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(CrashRiskSimilarAssets, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,crashPredicted,assetId,assetUniqueId,assetName,' \
                         'productId,productFamily,softwareRelease,softwareType,serialNumber,risk,feature,' \
                         'similarityScore'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(CrashRiskAssets, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            assetId = row['assetId']
            assetUniqueId = row['assetUniqueId']
            if row['successTrackId'] in insightList:
                for feature in features:
                    url = (f'{pxc_url_customers}{customerId}{pxc_url_crash_risk_assets}/{assetUniqueId}'
                           f'{pxc_url_crash_risk_similar_assets}?similarityCriteria={feature}'
                           f'&successTrackId={successTrackId}&max={max_items}')
                    logging.debug(f'Scanning Similar Assets for asset ID {assetId} for {customerName} '
                                  f'with same {feature}')
                    items = get_json_reply(url,
                                           tag='items',
                                           report='Risk Mitigation Checks on Similar Assets Report')
                    crashPredicted = str(get_json_reply(url,
                                                        tag='crashPredicted',
                                                        report='Risk Mitigation Checks on Similar Assets Report'))
                    if outputFormat == 1 or outputFormat == 2:
                        if items is not None:
                            if len(items) > 0:
                                with open(json_output_dir + customerId +
                                          '_Similar_Assets_' + successTrackId + '_' + assetId.upper() + '_' + feature +
                                          '.json', 'w') as json_file:
                                    json.dump(items, json_file)
                                logging.debug(f'Saving {json_file.name}')
                    if outputFormat == 1 or outputFormat == 3:
                        if items is not None:
                            if len(items) > 0:
                                for item in items:
                                    assetId = str(item['assetId']).replace(',', '_')
                                    assetName = str(item['assetName']).replace(',', ' ')
                                    productId = str(item['productId']).replace(',', ' ')
                                    productFamily = str(item['productFamily']).replace(',', ' ')
                                    softwareRelease = str(item['softwareRelease']).replace(',', ' ')
                                    softwareType = str(item['softwareType']).replace(',', ' ')
                                    serialNumber = str(item['serialNumber']).replace(',', ' ')
                                    risk = str(item['risk']).replace(',', ' ')
                                    similarityScore = str(item['similarityScore']).replace(',', ' ')
                                    with open(CrashRiskSimilarAssets, 'a', encoding='utf-8', newline='') as target:
                                        writer = csv.writer(target,
                                                            delimiter=' ',
                                                            quotechar=' ',
                                                            quoting=csv.QUOTE_MINIMAL)
                                        CSV_Data = f'{customerId},{customerName},{successTrackId},{crashPredicted},' \
                                                   f'{assetId},{assetUniqueId},{assetName},{productId},' \
                                                   f'{productFamily},{softwareRelease},{softwareType},{serialNumber},' \
                                                   f'{risk},{feature},{similarityScore}'
                                        writer.writerow(CSV_Data.split())
                                        tRecords += 1
            else:
                logging.debug(f'No data found for {feature}')
    logging.debug(f'Total Risk Mitigation Checks on Similar Asset records {tRecords}')
    print(f'Total Risk Mitigation Checks on Similar Asset records {tRecords}')


def pxc_crash_in_last():
    # Functions to get the Assets Crashed in last 1d, 7d, 15d, 90d data from PX Cloud.
    # This API provides the list of devices with details (i.e. Asset, Product Id, Product Family, Software Version,
    #   Crash Count, First Occurrence and Last Occurrence) by customer ID that have crashed in the last 1d,7d,15d,90d
    #   based on the filter input. Default sort is by lastCrashDate.
    # CSV Naming Convention: Crash_Risk_Assets_Last_Crashed.csv
    # JSON Naming Convention:{Customer ID}_Crash_Risk_Assets_Last_Crashed_In_{timePeriod}_Days_{Success Track ID}.json
    logging.debug('* Running Risk Mitigation Checks on Assets Last Crashed Date Report **\n')
    tRecords = 0
    while crash_risk_assetsFlag == 0:
        logging.debug(f'Waiting on Risk Mitigation Checks on Crash Risk Assets Report to finish')
        time.sleep(1)
    print('Started Running Risk Mitigation Checks on Assets Last Crashed Date Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(CrashRiskAssetsLastCrashed, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,daysLastCrashed,assetId,assetUniqueId,assetName,' \
                         'productId,productFamily,softwareRelease,softwareType,serialNumber,firstCrashDate,' \
                         'lastCrashDate,crashCount,ipAddress'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(CrashRiskAssets, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            serialNumber = row['serialNumber']
            for daysLastCrashed in timePeriods:
                if row['successTrackId'] in insightList:
                    logging.debug(f'Scanning Serial Number {serialNumber} with a last crashed date within '
                                  f'{daysLastCrashed} days for {customerName} ')
                    url = (f'{pxc_url_customers}{customerId}{pxc_url_crash_risk_assets_last_crashed}'
                           f'?successTrackId={successTrackId}&timePeriod={daysLastCrashed}')
                    if debug_level > 0:
                        logging.debug(url)
                    items = get_json_reply(url,
                                           tag='items',
                                           report='Risk Mitigation Checks on Assets last Crashed date Report')
                    if outputFormat == 1 or outputFormat == 2:
                        if items is not None:
                            if len(items) > 0:
                                with open(json_output_dir + customerId +
                                          '_Crash_Risk_Assets_Last_Crashed_In_' + str(daysLastCrashed) + '_Days_' +
                                          successTrackId + '.json', 'w') as json_file:
                                    json.dump(items, json_file)
                                logging.debug(f'Saving {json_file.name}')
                    if outputFormat == 1 or outputFormat == 3:
                        if items:
                            for item in items:
                                assetId = str(item['assetId']).replace(',', '_')
                                assetName = str(item['assetName']).replace(',', ' ')
                                assetUniqueId = str(item['assetUniqueId']).replace(',', ' ')
                                productId = str(item['productId']).replace(',', ' ')
                                productFamily = str(item['productFamily']).replace(',', ' ')
                                softwareRelease = str(item['softwareRelease']).replace(',', ' ')
                                softwareType = str(item['softwareType']).replace(',', ' ')
                                serialNumber = str(item['serialNumber']).replace(',', ' ')
                                firstCrashDate = str(item['firstCrashDate']).replace(',', ' ')
                                lastCrashDate = str(item['lastCrashDate']).replace(',', ' ')
                                crashCount = str(item['crashCount']).replace(',', ' ')
                                ipAddress = str(item['ipAddress']).replace(',', ' ')
                                with open(CrashRiskAssetsLastCrashed, 'a', encoding='utf-8', newline='') as target:
                                    writer = csv.writer(target,
                                                        delimiter=' ',
                                                        quotechar=' ',
                                                        quoting=csv.QUOTE_MINIMAL)
                                    CSV_Data = f'{customerId},{customerName},{successTrackId},{daysLastCrashed},' \
                                               f'{assetId},{assetUniqueId},{assetName},{productId},{productFamily},' \
                                               f'{softwareRelease},{softwareType},{serialNumber},{firstCrashDate},' \
                                               f'{lastCrashDate},{crashCount},{ipAddress}'
                                    writer.writerow(CSV_Data.split())
                                    tRecords += 1
            else:
                logging.debug('No Data Found .... Skipping')
    logging.debug(f'Total Risk Mitigation Checks on Assets last Crashed date records {tRecords}')
    print(f'Total Risk Mitigation Checks on Assets last Crashed date records {tRecords}')


def pxc_asset_crash_history():
    # Functions to get the Asset Crash History from PX Cloud
    # This API returns information for each crash that occurred during the last 365 days for the assetId
    #   and customerId provided in the last 1 year. Default sort is by timeStamp.
    # CSV Naming Convention: Asset_Crash_History.csv
    # JSON Naming Convention:{Customer ID}_Asset_Crash_History_{Success Track ID}_{Base64 Asset ID}.json
    logging.debug('*** Running Risk Mitigation Checks on Assets Crash History Report ***\n')
    tRecords = 0
    while crash_risk_assetsFlag == 0:
        logging.debug(f'Waiting on Risk Mitigation Checks on Crash Risk Assets Report to finish')
        time.sleep(1)
    print('Started Running Risk Mitigation Checks on Assets Crash History Report')
    if outputFormat == 1 or outputFormat == 3:
        with open(CrashRiskAssetCrashHistory, 'w', encoding='utf-8', newline='') as target:
            CSV_Header = 'customerId,customerName,successTrackId,assetUniqueId,resetReason,timeStamp'
            writer = csv.writer(target, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_NONE)
            writer.writerow(CSV_Header.split())
    with open(CrashRiskAssets, 'r') as cust_target:
        custList = csv.DictReader(cust_target)
        for row in custList:
            customerId = row['customerId']
            customerName = row['customerName']
            successTrackId = row['successTrackId']
            assetUniqueId = row['assetUniqueId']
            assetId = row['assetId']
            if row['successTrackId'] in insightList:
                logging.debug(f'Scanning Asset ID:{assetId} for {customerName} on Success Track {successTrackId}')
                url = (pxc_url_customers +
                       customerId +
                       pxc_url_crash_risk + '/' +
                       assetUniqueId +
                       pxc_url_crash_risk_asset_crash_history +
                       '?successTrackId=' + successTrackId +
                       '&max=' + max_items)
                items = get_json_reply(url,
                                       tag='items',
                                       report='Risk Mitigation Checks on Assets Crash History Report')
                if outputFormat == 1 or outputFormat == 2:
                    if items is not None:
                        if len(items) > 0:
                            with open(json_output_dir + customerId +
                                      '_Asset_Crash_History_' + successTrackId + '_' + assetUniqueId +
                                      '.json', 'w') as json_file:
                                json.dump(items, json_file)
                            logging.debug(f'Saving {json_file.name}')
                if outputFormat == 1 or outputFormat == 3:
                    if items:
                        for item in items:
                            resetReason = str(item['resetReason']).replace(',', '_')
                            timeStamp = str(item['timeStamp']).replace(',', ' ')
                            with open(CrashRiskAssetCrashHistory, 'a', encoding='utf-8', newline='') as target:
                                writer = csv.writer(target,
                                                    delimiter=' ',
                                                    quotechar=' ',
                                                    quoting=csv.QUOTE_MINIMAL)
                                CSV_Data = f'{customerId},{customerName},{successTrackId},{assetUniqueId},' \
                                           f'{resetReason},{timeStamp}'
                                writer.writerow(CSV_Data.split())
                                tRecords += 1
            else:
                logging.debug('No Data Found .... Skipping')
    logging.debug(f'Total Risk Mitigation Checks on Crash Risk Assets Report records {tRecords}')
    print(f'Total Risk Mitigation Checks on Crash Risk Assets Report records {tRecords}')


'''
Begin main application control
=======================================================================
All endpoints require customer approval to return data.

All endpoints that are L1 contract supported are listed in phase 1:
These include:  Customers, Contracts, Contract Details, Partner Offers, Partner Offers Sessions
                Success Tracks Metadata, Lifecycle, Assets, Software, Hardware, FieldNotices, PriorityBugs,
                SecurityAdvisories, PurchasedLicenses, and Licenses Reports

All endpoints that are L2 contract supported are listed in phase 2.
These only apply to Campus Networks (38396885) and Cloud Network () Success Tracks:
These include:  Optimal Software Version endpoints:
                Software Groups, Software Group Suggestions, Software Group Suggestions-Assets, 
                Bug list, Field Notices, Advisories
                
                Automated Faults Management endpoints:
                Faults, Fault Summary, Affected Assets, Fault History
                
                Regulatory Compliance Check endpoints:
                Compliance Violations, Assets Violating Compliance Rule, Policy Rule Details
                Compliance Suggestions, Assets with Violations, Asset Violations, OptIn
                
                Risk Mitigation Checks endpoints:
                Crash Risk Assets, Crash Risk Factors, Similar Assets, Assets Crashed in last 1d, 7d, 15d, 90d,
                Asset Crash History
=======================================================================      
'''


def main():
    # Set number of itterations, time stamp, logging level and if the log should be written to file
    for x in range(0, testLoop):
        print('******************** Script Started Successfully ********************')
        logging.debug('******************** Script Started Successfully ********************')
        startTime = time.time()
        print(f'Start Time: {datetime.now()}')
        print(f'Execution: {x + 1} of {testLoop}\nVersion is: {codeVersion}\nEnvironment is: {environment}')
        if outputFormat == 1:
            print('Saving data in JSON and CSV formats')
        if outputFormat == 2:
            print('Saving data in JSON format')
        if outputFormat == 3:
            print('Saving data in CSV format')
        temp_storage()  # delete temp and output directories and recreate before every run

        get_pxc_token()  # call the function to get a valid PX Cloud API token
        get_pxc_customers()  # No requirements but all others require it
        get_pxc_contracts()  # requires get_pxc_customers
        csv_count_rows(csv_output_dir + 'unique_contracts.csv', 3)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(get_pxc_contracts_details_part1)
            executor.submit(get_pxc_contracts_details_part2)
            executor.submit(get_pxc_contracts_details_part3)
            executor.submit(get_pxc_contractswithcustomernames)  # No requirements
            executor.submit(get_pxc_lifecycle)  # requires get_pxc_customers
            executor.submit(get_pxc_successtracks)  # No requirements
            executor.submit(get_pxc_partner_offers)  # No requirements
            executor.submit(get_pxc_partner_offer_sessions)  # No requirements
            executor.submit(pxc_assets_reports)  # requires get_pxc_customers
            executor.submit(pxc_hardware_reports)  # requires get_pxc_customers
            executor.submit(pxc_software_reports)  # requires get_pxc_customers
            executor.submit(pxc_licenses_reports)  # requires get_pxc_customers
            executor.submit(pxc_purchased_licenses_reports)  # requires get_pxc_customers
            executor.submit(pxc_security_advisories_reports)  # requires get_pxc_customers
            executor.submit(pxc_field_notices_reports)  # requires get_pxc_customers
            executor.submit(pxc_priority_bugs_reports)  # requires get_pxc_customers
            executor.submit(pxc_compliance_violations)  # requires get_pxc_customers
            executor.submit(pxc_assets_with_violations)  # requires get_pxc_customers
        merge_files_with_os(file1, file2, file3, contractDetails)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(pxc_software_groups)  # requires get_pxc_contractswithcustomernames
            executor.submit(pxc_afm_faults)  # requires get_pxc_contractswithcustomernames
            executor.submit(pxc_optin)  # requires get_pxc_customers
            executor.submit(pxc_crash_risk_assets)  # requires get_pxc_customers
            executor.submit(pxc_assets_violating_compliance_rule)  # requires pxc_compliance_violations
            executor.submit(pxc_asset_violations)  # requires pxc_assets_with_violations
            executor.submit(pxc_compliance_rule_details)  # requires pxc_compliance_violations
            executor.submit(pxc_compliance_suggestions)  # requires pxc_compliance_violations
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(pxc_software_group_suggestions)  # requires pxc_software_groups
            executor.submit(pxc_software_group_suggestions_assets)  # requires pxc_software_groups
            executor.submit(pxc_afm_fault_summary)  # requires pxc_afm_faults
            executor.submit(pxc_afm_affected_assets)  # requires pxc_afm_faults
            executor.submit(pxc_afm_fault_history)  # requires pxc_afm_faults
            executor.submit(pxc_crash_risk_factors)  # requires pxc_crash_risk_assets
            executor.submit(pxc_crash_in_last)  # requires pxc_crash_risk_assets
            executor.submit(pxc_asset_crash_history)  # requires pxc_crash_risk_assets
            executor.submit(pxc_similar_assets)  # requires pxc_crash_risk_assets
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(pxc_software_group_suggestions_field_notices)  # req pxc_software_group_suggestions
            executor.submit(pxc_software_group_suggestions_advisories)  # requires pxc_software_group_suggestions
            executor.submit(pxc_software_group_suggestions_bug_list)  # requires pxc_software_group_suggestions
        # Send all CSV files in the output data folder to AWS S3 storage
        s3_storage()
        # Record time and cleanly exit or run next itteration
        print('******************** Script Executed Successfully ********************')
        logging.debug('******************** Script Executed Successfully ********************')
        stopTime = time.time()
        logging.debug(f'Stop Time:{datetime.now()}')
        print(f'Total Time:{math.ceil(int(stopTime - startTime) / 60)} minutes')
        logging.debug(f'Total Time:{math.ceil(int(stopTime - startTime) / 60)} minutes')

        # copy multiple tests in different folders under 'Results-#'
        if testLoop > 1:
            collectionResultsDir = 'Results-' + str(x + 1) + '/'
            if os.path.isdir(collectionResultsDir):
                shutil.rmtree(collectionResultsDir)

            # Save all output files
            shutil.move(log_output_dir, collectionResultsDir + log_output_dir)
            if outputFormat == 1 or outputFormat == 2:
                print(f'Total bytes collected for JSONs: {get_dir_size(json_output_dir)}')
                logging.info(f'Total bytes collected for JSONs: {get_dir_size(json_output_dir)}')
                shutil.move(json_output_dir, collectionResultsDir + json_output_dir)
            if outputFormat == 1 or outputFormat == 3:
                print(f'Total bytes collected for CSVs : {get_dir_size(csv_output_dir)}')
                logging.info(f'Total bytes collected for CSVs : {get_dir_size(csv_output_dir)}')
                shutil.move(csv_output_dir, collectionResultsDir + csv_output_dir)
        #end if 

        if x + 1 == testLoop:
            # Clean exit
            if outputFormat == 2:
                shutil.rmtree(csv_output_dir)
            exit()
        else:
            # pause 5 sec between each itteration
            print('pausing for 5 secs\n')
            logging.debug('pausing for 5 secs')
            time.sleep(5)


if __name__ == '__main__':
    # setup parser
    parser = argparse.ArgumentParser(description="Your script description.")
    parser.add_argument("customer", nargs='?', default='credentials', help="Customer name")
    parser.add_argument("-log", "--log-level", default="DEBUG", help="Set the logging level (default: ERROR)")

    # Parse command-line arguments
    args = parser.parse_args()

    # call function to load config.ini data into variables
    customer = args.customer
    if not customer:
        usage()

    # create a per-customer folder for saving data
    load_config(customer)
    if customer:
        # Create the customers directory
        os.makedirs(customer, exist_ok=True)
        # Change into the directory
        os.chdir(customer)

    # delete temp and output directories and recreate before every run
    temp_storage()  # delete temp and output directories and recreate before every run

    # setup the logging level
    init_logger(args.log_level.upper())

    # Set URL to Sandbox if useProductionURL is false
    if useProductionURL == 0:
        pxc_url_base = urlProtocol + urlHost + urlLinkSandbox
        pxc_scope = 'api.customer.assets.manage'
        environment = 'Sandbox'
    elif useProductionURL == 1:
        pxc_url_base = urlProtocol + urlHost + urlLink
        pxc_scope = 'api.authz.iam.manage'
        environment = 'Production'
    pxc_url_customers = pxc_url_base + 'customers/'
    pxc_url_contracts = pxc_url_base + 'contracts/'
    pxc_url_contractswithcustomers = pxc_url_base + 'contractsWithCustomers/'
    pxc_url_contracts_details = pxc_url_base + 'contract/details/'
    pxc_url_partner_offers = pxc_url_base + 'partnerOffers/'
    pxc_url_partner_offers_sessions = pxc_url_base + 'partnerOffersSessions/'
    pxc_url_successTracks = pxc_url_base + 'successTracks/'
    file1 = 'temp/CD_0.csv'
    file2 = 'temp/CD_1.csv'
    file3 = 'temp/CD_2.csv'
    main()
