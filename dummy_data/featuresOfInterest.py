import csv
import random
import string

def generate_featuresOfInterest_data(id_num,feat_int):
    data = []

    for i in range(0, feat_int):

        # Generate a unique ID
        id = id_num+i


        # Generate a unique name
        name = "Room No"+ "_" + str(id)
        description= "Feature of interest" + "_" + str(id)
        encodingType="application/vnd.geo+json"
        feature="0101000020E6100000BA490C022B7F52C0355EBA490C624440"
        properties=metadata=f'{{}}'




        # Append the row to the data list
        data.append([str(id), name,description, encodingType,feature,properties])
        

# Generate 200 combinations of data
    #print(data)
    print("creating features of interest data...")
    # Write the data to a CSV file
    with open('data/FeaturesOfInterest.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name","description", "encodingType", "feature","properties"])
        writer.writerows(data)
