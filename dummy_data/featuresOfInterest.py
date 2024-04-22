import csv

def generate_featuresOfInterest_data(id_num, feat_int):
    data = []

    for i in range(0, feat_int):
        id = id_num + i
        name = f"Room No_{str(id)}"
        description = f"Feature of interest_{str(id)}"
        encodingType ="application/vnd.geo+json"
        feature = "0101000020E6100000BA490C022B7F52C0355EBA490C624440"
        properties = f"{{}}"

        # Append the row to the data list
        data.append([str(id), name,description, encodingType, feature, properties])

    print("creating FeaturesOfInterest data...")
    # Write the data to a CSV file
    with open('data/FeaturesOfInterest.csv', 'w', newline = '') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name","description", "encodingType", "feature", "properties"])
        writer.writerows(data)
