# DataMiner
This is a Fork of [CiscoSteve/DataMiner](https://github.com/CiscoSteve/DataMiner)

Primary changes from Cisco Verson:
  1) Some edits/changes I wish to keep track of
  2) Additon of gitignore to avoid tracking temp files
  3) Deleted all the zips in favor of release branch

#
This is a sample python script that will extract all the PX Cloud data for all customers within a single specified partner.
It will gather the JSON data and convert it to CSV or JSON files, or both for injecting into any platform that can digest a CSV or JSON file.

#
# Usage

$ python3 .\DataMiner.py <Auth Section>
     <Auth Section> auth section in config.in - default is credentials  (optional)


To excute the script with the default auth secion [credentials] enter the command line
   python3 .\DataMiner.py

If you wish to have multiple auth sections, add named sections to the config.ini 
Example:
   config.ini:
	[credentials]
	pxc_client_id =     # PX Cloud API Client ID  Default is blank
	pxc_client_secret = # PX Cloud API Client Secret  Default is blank
	s3access_key = 
	s3access_secret = 
	s3bucket_folder = 
	s3bucket_name = 
	
	[DVS]
	pxc_client_id = 123456789012345
	pxc_client_secret = 1234567890abcdefghijklmnoprstuvwxyz
	s3access_key = 
	s3access_secret = 
	s3bucket_folder = 
	s3bucket_name = 


In this case you have two auth sections - the default and one named DVS.
To use the nameed auth section you enter:
   python3 .\DataMiner.py DVS


The script support multiple named auth sections.
