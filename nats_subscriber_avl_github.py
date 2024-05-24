import asyncio
import json
from nats.aio.client import Client as NATS
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import warnings


# Function to load configuration from a JSON file
def load_config(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config

async def run():
    # Load configuration
    config = load_config(r'path\\to\\your\\config.json')

    # Initialize the NATS client
    nc = NATS()

    # Connect to the NATS server with credentials
    await nc.connect(config['nats_server_url'], user=config['nats_user'], password=config['nats_password'])

    # Connect to ArcGIS Portal
    gis = authenticate_gis(profile=config['profile'])

    # Get the feature layer
    feature_layer = FeatureLayer(config['feature_layer_url'], gis)
    layer = feature_layer.query()

    message_queue = asyncio.Queue()

    async def message_handler(msg):
        # Add the incoming message to the queue
        await message_queue.put(msg.data.decode())

    async def process_messages():
        while True:
            # Get a message from the queue
            avl_message = await message_queue.get()
            try:
                # Parse AVL data
                avl_data = parse_avl_data(avl_message)
                # Print the parsed AVL data
                print("Parsed AVL data:", avl_data) 
                
                # Check if the status is not "available" and unit does not begin with "SM"
                #replace with your unique query
                if avl_data["StatusName"] != "Available" and not avl_data["UnitName"].startswith("SM"):
                    # Construct the GeoJSON object for the new feature
                    geojson_feature = construct_geojson(avl_data)
                    # Print the GeoJSON object for verification
                    print("GeoJSON feature to add:", json.dumps(geojson_feature, indent=2))
                    
                    # Push GeoJSON data to ArcGIS feature layer
                    push_to_arcgis(geojson_feature, feature_layer)
                else:  
                    # Delete the vehicle if its status is "Available"
                    delete_vehicle(avl_data["VehicleId"], feature_layer)
            except Exception as e:
                print(f"Error processing message: {e}")
            finally:
                # Notify the queue that the message has been processed
                message_queue.task_done()

    
    await nc.subscribe(config['subject'], cb=message_handler)

    asyncio.create_task(process_messages())

    # Keep the script running to listen for messages
    await asyncio.Event().wait()

def authenticate_gis(profile):
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always")
        try:
            gis = GIS(profile=profile)
            #print(f"Successfully logged into Enterprise")
            return gis
        except Exception as e:
            for warning in caught_warnings:
                if issubclass(warning.category, UserWarning) and "profiles in" in str(warning.message):
                    print("Caught profile warning, setting up profiles again.")
                    signin.setup_profiles()
                    # Retry authentication after setting up profiles
                    gis = GIS(profile=profile)
                    #print(f"Successfully logged into '{gis.properties.portalHostname}' via the '{gis.properties.user.username}' user after deleting old profile")
                    return gis

def parse_avl_data(avl_message):
    # Parse the JSON data
    avl_data = json.loads(avl_message)
    return avl_data
 
def construct_geojson(avl_data):
    # Construct the GeoJSON object for the new feature


    
    geojson_feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [avl_data.get("Longitude"), avl_data.get("Latitude")]
        },
        "properties": {
            "Agency": avl_data.get("Agency"),
            #...continue with your fields
            "Master_Incident_Number": avl_data.get("Incident", {}).get("Master_Incident_Number") #for nested value under "Incident"

        }
    }
    return geojson_feature



def push_to_arcgis(geojson_feature, feature_layer):
    # Mapping dictionary for attribute names to column names
    attribute_mapping = {
        "Agency": "agency",
        "Master_Incident_Number":"master_incident_number"
        #...continue with your fields
    }
    
    # Convert GeoJSON to the format expected by ArcGIS
    arcgis_feature = {
        "attributes": {},
        "geometry": {
            "x": geojson_feature["geometry"]["coordinates"][0],
            "y": geojson_feature["geometry"]["coordinates"][1],
            "spatialReference": {"wkid": 4326}  # Assuming WGS 1984 coordinate system
        }
    }

    # Map attributes from GeoJSON to ArcGIS feature using attribute_mapping
    for geojson_attribute, arcgis_column in attribute_mapping.items():
        # Check if the attribute exists in the GeoJSON properties
        if geojson_attribute in geojson_feature["properties"]:
            arcgis_feature["attributes"][arcgis_column] = geojson_feature["properties"][geojson_attribute]
        else:
            # Handle the case where the attribute is missing
            arcgis_feature["attributes"][arcgis_column] = None
    
    # Check if the vehicle is already in the feature layer
    query = f"vehicleid = '{geojson_feature['properties']['VehicleId']}'"
    existing_features = feature_layer.query(where=query)


    if existing_features.features:
        # Update existing feature
        existing_feature = existing_features.features[0]
        existing_attributes = existing_feature.attributes

        # Keep the ObjectID in the attributes
        arcgis_feature["attributes"]["objectid"] = existing_attributes["objectid"]
        
        # Update attributes with values from the geojson_feature
        for geojson_attribute, arcgis_column in attribute_mapping.items():
            if geojson_attribute in geojson_feature["properties"]:
                arcgis_feature["attributes"][arcgis_column] = geojson_feature["properties"][geojson_attribute]
            else:
                arcgis_feature["attributes"][arcgis_column] = existing_attributes[arcgis_column]
        
       # print("ArcGIS Feature to Update:", arcgis_feature)
        result = feature_layer.edit_features(updates=[arcgis_feature])
        print("Feature updated:", result)
    else:
        result = feature_layer.edit_features(adds=[arcgis_feature])
        print("Feature added:", result)

    if result.get('addResults', []) and not result['addResults'][0]['success']:
        print("Error adding/updating feature:", result['addResults'][0]['error'])
    if result.get('updateResults', []) and not result['updateResults'][0]['success']:
        print("Error updating feature:", result['updateResults'][0]['error'])

##
def delete_vehicle(vehicleId, feature_layer):
    where_clause = f"vehicleid = '{vehicleId}'"
    
    # Query the features to delete using the where clause
    features_to_delete = feature_layer.query(where=where_clause).features

    if not features_to_delete:
      #  print(f"No vehicle found with vehicleId {vehicleId} to delete.")
        return

    object_ids = [feature.attributes['objectid'] for feature in features_to_delete]
    #print(f"ObjectIDs to delete: {object_ids}")
    
    result = feature_layer.edit_features(deletes=object_ids)
    print("Vehicle deleted:", result)
    if not result['deleteResults'][0]['success']:
        print("Error deleting vehicle:", result['deleteResults'][0]['error'])


# Run the asyncio event loop
asyncio.run(run())
