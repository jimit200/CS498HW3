from flask import Flask
from google.cloud import bigtable

app = Flask(__name__)


@app.route('/rows', methods=['GET'])
def row_count():
    client = bigtable.Client(admin=True)
    instance = client.instance('ev-bigtable')
    table = instance.table('ev-population')
    rows = table.read_rows()
    count = sum(1 for _ in rows)
    return str(count)

@app.route('/Best-BMW', methods=['GET'])
def best_bmws():
    client = bigtable.Client(admin=True)
    instance = client.instance('ev-bigtable')
    table = instance.table('ev-population')
    rows = table.read_rows()
    rows.consume_all()
    count = sum(
    1 for row in rows.rows.values()
    if (
        int(row.cells['ev_info']['electric range'][0].value) > 100 and
        row.cells['ev_info']['make'][0].value.decode() == 'BMW'
    )
)
    return str(count)

@app.route('/tesla-owners', methods=['GET'])
def tesla_owner_count():
    client = bigtable.Client(admin=True)
    instance = client.instance('ev-bigtable')
    table = instance.table('ev-population')
    rows = table.read_rows()
    rows.consume_all()
    count = sum(1 for row in rows.rows.values() 
                if (row.cells['ev_info']['city'][0].value.decode() == 'Seattle' 
                    and row.cells['ev_info']['make'][0].value.decode() == 'Tesla'))
    return str(count)

@app.route('/update', methods=['POST'])
def update_range():
    client = bigtable.Client(admin=True)
    instance = client.instance('ev-bigtable')
    table = instance.table('ev-population')
    row_key = '257246118'.encode()
    row = table.row(row_key)
    row.set_cell('ev_info', 'electric range', '200'.encode())
    row.commit()
    return "Success"

@app.route('/delete', methods=['DELETE'])
def delete_records():
    client = bigtable.Client(admin=True)
    instance = client.instance('ev-bigtable')
    table = instance.table('ev-population')
    rows = table.read_rows()
    rows.consume_all()
    count = 0
    for row_key, row in rows.rows.items():
        if int(row.cells['ev_info']['model year'][0].value) < 2014:
            table.mutate_rows([row.delete()])
        else:
            count += 1
    return str(count)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
