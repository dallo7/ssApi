from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)

server = app.server

# Initialize SQLite Database
def init_db():
    conn = sqlite3.connect('airspace.db')
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS flights
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  "ENTRY POINT" TEXT,
                  "EXIT POINT"  TEXT,
                  "FLIGHT" VARCHAR,
                  "FLIGHT DATE" TEXT,
                  "FLIGHT TIME" TEXT,
                  "AIRCRAFT REGISTRATION" TEXT,
                  "AIRCRAFT TYPE" TEXT,
                  "FLIGHT CALL SIGN" TEXT,
                  "ORIGIN" TEXT,
                  "DESTINATION" TEXT,
                  "ROUTE" TEXT,
                  "TIMESTAMP" DATETIME DEFAULT CURRENT_TIMESTAMP)''')

    conn.commit()
    conn.close()


init_db()


# POST endpoint - Supports single & multiple entries
@app.route('/api/flights', methods=['POST'])
def add_flight():
    data = request.json  # Get JSON input

    if not data:
        return jsonify({'error': 'No data provided'}), 400

    # Ensure data is always a list
    if isinstance(data, dict):
        data = [data]

    required_fields = ["ENTRY POINT", "EXIT POINT", "FLIGHT", "FLIGHT DATE", "FLIGHT TIME",
                       "AIRCRAFT REGISTRATION", "AIRCRAFT TYPE", "FLIGHT CALL SIGN",
                       "ORIGIN", "DESTINATION", "ROUTE"]

    for record in data:
        if not all(field in record for field in required_fields):
            return jsonify({'error': 'Missing required fields in some records'}), 400

    try:
        conn = sqlite3.connect('airspace.db')
        c = conn.cursor()

        sql = '''INSERT INTO flights 
                 ("ENTRY POINT", "EXIT POINT", "FLIGHT", "FLIGHT DATE", "FLIGHT TIME", 
                  "AIRCRAFT REGISTRATION", "AIRCRAFT TYPE", "FLIGHT CALL SIGN", 
                  "ORIGIN", "DESTINATION", "ROUTE") 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)'''

        # Convert 'nan' values to None
        cleaned_data = [
            (
                record["ENTRY POINT"],
                None if record["EXIT POINT"] == "nan" else record["EXIT POINT"],
                record["FLIGHT"],
                record["FLIGHT DATE"],
                record["FLIGHT TIME"],
                record["AIRCRAFT REGISTRATION"],
                record["AIRCRAFT TYPE"],
                record["FLIGHT CALL SIGN"],
                record["ORIGIN"],
                record["DESTINATION"],
                record["ROUTE"]
            )
            for record in data
        ]

        # Execute batch insert
        c.executemany(sql, cleaned_data)
        conn.commit()
        conn.close()

        return jsonify({'message': f'{len(cleaned_data)} flight(s) added successfully'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# GET endpoint (latest entry)
@app.route('/api/flights/latest', methods=['GET'])
def get_latest_flight():
    try:
        conn = sqlite3.connect('airspace.db')
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute('''SELECT * FROM flights ORDER BY TIMESTAMP DESC LIMIT 1''')
        result = c.fetchone()

        if result:
            return jsonify(dict(result)), 200
        else:
            return jsonify({'message': 'No flights found'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


if __name__ == '__main__':
    app.run(debug=True)

