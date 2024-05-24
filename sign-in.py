from arcgis.gis import GIS

def setup_profiles():
    profile_name = 'python_portal_prof2'

    # Attempt to delete the profile if it exists
    try:
        GIS.delete_profile(profile_name)
        print(f"Deleted existing profile '{profile_name}'")
    except Exception as e:
        print(f"Profile '{profile_name}' does not exist or could not be deleted: {e}")

    # Create a new profile
    portal_gis = GIS(url="your/url.com",
                     username='your user',
                     password='your pw',
                     profile=profile_name)
    agol_gis = GIS('home')
    # Print profile information
    print("Profile defined for {}".format(portal_gis))


if __name__ == "__main__":
    setup_profiles()
