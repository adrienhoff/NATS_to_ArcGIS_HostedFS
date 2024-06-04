# NATS_to_ArcGIS_HostedFS
Py script to update, add, and delete values in hosted feature service based on received JSON messages in NATS server

The script listens for updates, adds, or deletes on NATS server and adjusts the hosted feature service in ArcGIS Portal as needed.

Connection to the NATS is stored in a config.json created before running this script. A config file is attached as an example. Replace your values in this file and save as config.json.

Authentication to the portal site can be accessed via the GIS(Profile= ' ') function which is created in the signin script. Replace your values within these scripts. Do not change the name of signin.py. This imports as a module so that the function can delete the old profile and recreate if a warning is encountered.

For more information on storing local credentials: https://developers.arcgis.com/python/guide/working-with-different-authentication-schemes/#storing-your-credentials-locally

Store both authentication files to a folder with secure access.

Replace 'your_table', 'your_item_id', 'your_profile', and any other placeholders with your actual database table, feature layer ID, ArcGIS profile, etc. Make sure to fill in the update and delete logic according to your specific requirements.

The bat file can be used to automate the process in NSSM.
