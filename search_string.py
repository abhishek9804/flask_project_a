
import sqlite3
import threading
from flask import Flask, request, jsonify
# from databasefile.db import initialize_database
from constants.status_code import HTTP_200_OK
app = Flask(__name__)

# Function to search for a string in a chunk of the log file
def search_in_chunk(chunk, search_list, start_line, result):
    for line_number, line in enumerate(chunk, start=start_line):
        for search_string in search_list:
            if search_string in line:
                result.append((search_string, line_number, line))


# Function to create the SQLite database and table
def initialize_database():
    conn = sqlite3.connect('log_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS log_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_string TEXT,
            line_number INTEGER,
            complete_line TEXT
        )
    ''')
    conn.commit()
    conn.close()


# REST API endpoint for Step 1
@app.route('/search_strings', methods=['POST'])
def search_strings():
    data = request.get_json()
    search_list = data.get('search_list')
    log_file_path = data.get('log_file_path')
    result = []

    chunk_size = 10000  
    with open(log_file_path, 'r') as log_file:
        chunk = []
        start_line = 1
        for line in log_file:
            chunk.append(line)
            if len(chunk) >= chunk_size:
                thread = threading.Thread(target=search_in_chunk, args=(chunk, search_list, start_line, result))
                thread.start()
                chunk = []
                start_line += chunk_size

        if chunk:
            search_in_chunk(chunk, search_list, start_line, result)


    conn = sqlite3.connect('log_data.db')
    cursor = conn.cursor()
    for search_string, line_number, line in result:
        cursor.execute("INSERT INTO log_data (search_string, line_number, complete_line) VALUES (?, ?, ?)", (search_string, line_number, line))
    conn.commit()
    conn.close()

    return jsonify({"message": "Strings searched and results stored successfully."}),HTTP_200_OK


@app.route('/get_line_numbers', methods=['POST'])
def get_line_numbers():
    data = request.get_json()
    search_strings = data.get('search_strings')
    
    if not search_strings:
        return jsonify({'message': 'No search strings provided'})

    conn = sqlite3.connect('log_data.db')
    cursor = conn.cursor()
    
    results = []

    for search_string in search_strings:
        cursor.execute("SELECT line_number, complete_line FROM log_data WHERE search_string = ?", (search_string,))
        result = cursor.fetchone()
        if result:
            result_dict = {
                'searchString': search_string,
                'line No': result[0],
                'complete line': result[1]
            }
            results.append(result_dict)

    conn.close()
    return jsonify(results),HTTP_200_OK



if __name__ == '__main__':
    initialize_database()
    app.run(debug=True)