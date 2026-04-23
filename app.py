import os
import sqlite3
import csv
from flask import Flask, render_template, request, redirect, url_for, flash
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'emr-archive-secret-key'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max

DB_PATH = 'emr_archive.db'
ALLOWED_EXTENSIONS = {'csv'}

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def init_db():
    """Initialize database tables"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Demographics table
    c.execute('''CREATE TABLE IF NOT EXISTS demographics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mrn TEXT,
        first_name TEXT,
        last_name TEXT,
        dob TEXT,
        gender TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        phone TEXT,
        email TEXT,
        ssn TEXT
    )''')
    
    # Encounters table
    c.execute('''CREATE TABLE IF NOT EXISTS encounters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mrn TEXT,
        encounter_date TEXT,
        encounter_type TEXT,
        provider TEXT,
        diagnosis TEXT,
        notes TEXT
    )''')
    
    # Medications table
    c.execute('''CREATE TABLE IF NOT EXISTS medications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mrn TEXT,
        medication_name TEXT,
        dosage TEXT,
        frequency TEXT,
        start_date TEXT,
        end_date TEXT,
        prescriber TEXT
    )''')
    
    # Allergies table
    c.execute('''CREATE TABLE IF NOT EXISTS allergies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mrn TEXT,
        allergen TEXT,
        reaction TEXT,
        severity TEXT,
        status TEXT
    )''')
    
    # Labs table
    c.execute('''CREATE TABLE IF NOT EXISTS labs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mrn TEXT,
        test_name TEXT,
        result TEXT,
        unit TEXT,
        reference_range TEXT,
        test_date TEXT,
        status TEXT
    )''')
    
    # Create indexes for search
    c.execute('CREATE INDEX IF NOT EXISTS idx_demographics_mrn ON demographics(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_demographics_name ON demographics(last_name, first_name)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_encounters_mrn ON encounters(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_medications_mrn ON medications(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_allergies_mrn ON allergies(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_labs_mrn ON labs(mrn)')
    
    conn.commit()
    conn.close()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def import_csv(table_name, filepath, field_mapping):
    """Import CSV into database table"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    rows_imported = 0
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            values = []
            for db_field, csv_field in field_mapping.items():
                if csv_field in row and row[csv_field]:
                    values.append(row[csv_field])
                else:
                    values.append('')
            
            fields = ', '.join(field_mapping.keys())
            placeholders = ', '.join(['?' for _ in field_mapping])
            query = f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders})"
            c.execute(query, values)
            rows_imported += 1
    
    conn.commit()
    conn.close()
    return rows_imported

@app.route('/')
def index():
    query = request.args.get('q', '').strip()
    results = []
    
    if query:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # Search across demographics
        c.execute('''SELECT * FROM demographics 
                     WHERE mrn LIKE ? OR first_name LIKE ? OR last_name LIKE ? OR dob LIKE ?
                     LIMIT 100''', 
                  (f'%{query}%', f'%{query}%', f'%{query}%', f'%{query}%'))
        results = c.fetchall()
        conn.close()
    
    return render_template('index.html', results=results, query=query)

@app.route('/patient/<mrn>')
def patient(mrn):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Get demographics
    c.execute('SELECT * FROM demographics WHERE mrn = ?', (mrn,))
    demographics = c.fetchone()
    
    # Get encounters
    c.execute('SELECT * FROM encounters WHERE mrn = ? ORDER BY encounter_date DESC', (mrn,))
    encounters = c.fetchall()
    
    # Get medications
    c.execute('SELECT * FROM medications WHERE mrn = ?', (mrn,))
    medications = c.fetchall()
    
    # Get allergies
    c.execute('SELECT * FROM allergies WHERE mrn = ?', (mrn,))
    allergies = c.fetchall()
    
    # Get labs
    c.execute('SELECT * FROM labs WHERE mrn = ? ORDER BY test_date DESC', (mrn,))
    labs = c.fetchall()
    
    conn.close()
    
    if not demographics:
        flash('Patient not found', 'error')
        return redirect(url_for('index'))
    
    return render_template('patient.html', 
                         demographics=demographics,
                         encounters=encounters,
                         medications=medications,
                         allergies=allergies,
                         labs=labs)

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        file = request.files.get('file')
        data_type = request.form.get('data_type')
        
        if not file or not data_type:
            flash('Please select a file and data type', 'error')
            return redirect(url_for('upload'))
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # Field mappings for each data type
            mappings = {
                'demographics': {
                    'mrn': 'MRN', 'first_name': 'First Name', 'last_name': 'Last Name',
                    'dob': 'DOB', 'gender': 'Gender', 'address': 'Address',
                    'city': 'City', 'state': 'State', 'zip': 'ZIP',
                    'phone': 'Phone', 'email': 'Email', 'ssn': 'SSN'
                },
                'encounters': {
                    'mrn': 'MRN', 'encounter_date': 'Date', 'encounter_type': 'Type',
                    'provider': 'Provider', 'diagnosis': 'Diagnosis', 'notes': 'Notes'
                },
                'medications': {
                    'mrn': 'MRN', 'medication_name': 'Medication', 'dosage': 'Dosage',
                    'frequency': 'Frequency', 'start_date': 'Start Date',
                    'end_date': 'End Date', 'prescriber': 'Prescriber'
                },
                'allergies': {
                    'mrn': 'MRN', 'allergen': 'Allergen', 'reaction': 'Reaction',
                    'severity': 'Severity', 'status': 'Status'
                },
                'labs': {
                    'mrn': 'MRN', 'test_name': 'Test Name', 'result': 'Result',
                    'unit': 'Unit', 'reference_range': 'Reference Range',
                    'test_date': 'Date', 'status': 'Status'
                }
            }
            
            try:
                rows = import_csv(data_type, filepath, mappings[data_type])
                flash(f'Successfully imported {rows} records', 'success')
            except Exception as e:
                flash(f'Error importing file: {str(e)}', 'error')
            
            return redirect(url_for('upload'))
        else:
            flash('Invalid file type. Please upload a CSV file.', 'error')
    
    return render_template('upload.html')

@app.route('/stats')
def stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    stats = {}
    tables = ['demographics', 'encounters', 'medications', 'allergies', 'labs']
    for table in tables:
        c.execute(f'SELECT COUNT(*) FROM {table}')
        stats[table] = c.fetchone()[0]
    
    conn.close()
    return render_template('stats.html', stats=stats)

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)