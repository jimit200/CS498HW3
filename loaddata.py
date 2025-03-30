import csv
from google.cloud import bigtable
from google.cloud.bigtable import row

def load_data_to_bigtable():
    client = bigtable.Client(admin=True)
    instance = client.instance('ev-bigtable')
    table = instance.table('ev-population')
    
    with open('Electric_Vehicle_Population_Data.csv') as file:
        reader = csv.DictReader(file)
        for row_data in reader:
            row_key = row_data['DOL Vehicle ID'].encode()
            row = table.row(row_key)
            
            row.set_cell('ev_info', 'make', row_data['Make'].encode())
            row.set_cell('ev_info', 'model', row_data['Model'].encode())
            row.set_cell('ev_info', 'model year', row_data['Model Year'].encode())
            row.set_cell('ev_info', 'electric range', row_data['Electric Range'].encode())
            row.set_cell('ev_info', 'city', row_data['City'].encode())
            row.set_cell('ev_info', 'county', row_data['County'].encode())
            row.commit()

if __name__ == "__main__":
    load_data_to_bigtable()