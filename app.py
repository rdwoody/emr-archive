import os
import sqlite3
import csv
import shutil
from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'emr-archive-secret-key'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['IMAGES_FOLDER'] = 'images'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max

DB_PATH = 'emr_archive.db'
ALLOWED_EXTENSIONS = {'csv', 'xml', 'ccd'}

# Ensure folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['IMAGES_FOLDER'], exist_ok=True)

def init_db():
    """Initialize database tables"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Demographics table
    c.execute('''CREATE TABLE IF NOT EXISTS demographics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mrn TEXT UNIQUE,
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
    
    # Imaging table
    c.execute('''CREATE TABLE IF NOT EXISTS imaging (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mrn TEXT,
        study_type TEXT,
        study_date TEXT,
        description TEXT,
        findings TEXT,
        file_path TEXT
    )''')
    
    # Billing table
    c.execute('''CREATE TABLE IF NOT EXISTS billing (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mrn TEXT,
        service_date TEXT,
        service_description TEXT,
        cpt_code TEXT,
        charge REAL,
        payment REAL,
        insurance TEXT,
        status TEXT
    )''')
    
    # Create indexes for search
    c.execute('CREATE INDEX IF NOT EXISTS idx_demographics_mrn ON demographics(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_demographics_name ON demographics(last_name, first_name)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_encounters_mrn ON encounters(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_medications_mrn ON medications(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_allergies_mrn ON allergies(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_labs_mrn ON labs(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_imaging_mrn ON imaging(mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_billing_mrn ON billing(mrn)')
    
    conn.commit()
    conn.close()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def import_csv(table_name, filepath, field_mapping):
    """Import CSV into database table"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    rows_imported = 0
    errors = 0
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                values = []
                for db_field, csv_field in field_mapping.items():
                    if csv_field in row and row[csv_field]:
                        values.append(row[csv_field])
                    else:
                        values.append('')
                
                fields = ', '.join(field_mapping.keys())
                placeholders = ', '.join(['?' for _ in field_mapping])
                query = f"INSERT OR REPLACE INTO {table_name} ({fields}) VALUES ({placeholders})"
                c.execute(query, values)
                rows_imported += 1
            except Exception as e:
                errors += 1
    
    conn.commit()
    conn.close()
    return rows_imported, errors

def import_imaging(mrn, file, study_type, study_date, description):
    """Import imaging file"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Save file
    filename = secure_filename(f"{mrn}_{study_type}_{file.filename}")
    filepath = os.path.join(app.config['IMAGES_FOLDER'], filename)
    file.save(filepath)
    
    c.execute('''INSERT INTO imaging (mrn, study_type, study_date, description, file_path)
                 VALUES (?, ?, ?, ?, ?)''',
              (mrn, study_type, study_date, description, filepath))
    
    conn.commit()
    conn.close()
    return True

@app.route('/')
def index():
    query = request.args.get('q', '').strip()
    data_type = request.args.get('type', 'all')
    results = []
    result_count = 0
    
    if query:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        if data_type == 'all' or data_type == 'demographics':
            c.execute('''SELECT 'demographics' as source, mrn, first_name, last_name, dob, gender 
                         FROM demographics 
                         WHERE mrn LIKE ? OR first_name LIKE ? OR last_name LIKE ? OR dob LIKE ?
                         LIMIT 50''', 
                      (f'%{query}%', f'%{query}%', f'%{query}%', f'%{query}%'))
            results.extend(c.fetchall())
        
        if data_type == 'all' or data_type == 'medications':
            c.execute('''SELECT 'medications' as source, mrn, medication_name as name, dosage, frequency
                         FROM medications 
                         WHERE mrn LIKE ? OR medication_name LIKE ?
                         LIMIT 30''', 
                      (f'%{query}%', f'%{query}%'))
            results.extend(c.fetchall())
        
        if data_type == 'all' or data_type == 'labs':
            c.execute('''SELECT 'labs' as source, mrn, test_name, result, test_date
                         FROM labs 
                         WHERE mrn LIKE ? OR test_name LIKE ? OR result LIKE ?
                         LIMIT 30''', 
                      (f'%{query}%', f'%{query}%', f'%{query}%'))
            results.extend(c.fetchall())
        
        if data_type == 'all' or data_type == 'encounters':
            c.execute('''SELECT 'encounters' as source, mrn, encounter_date, encounter_type, diagnosis
                         FROM encounters 
                         WHERE mrn LIKE ? OR diagnosis LIKE ?
                         LIMIT 30''', 
                      (f'%{query}%', f'%{query}%'))
            results.extend(c.fetchall())
        
        if data_type == 'all' or data_type == 'imaging':
            c.execute('''SELECT 'imaging' as source, mrn, study_type, study_date, description
                         FROM imaging 
                         WHERE mrn LIKE ? OR study_type LIKE ? OR description LIKE ?
                         LIMIT 30''', 
                      (f'%{query}%', f'%{query}%', f'%{query}%'))
            results.extend(c.fetchall())
        
        result_count = len(results)
        conn.close()
    
    return render_template('index.html', results=results, query=query, data_type=data_type, result_count=result_count)

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
    
    # Get imaging
    c.execute('SELECT * FROM imaging WHERE mrn = ? ORDER BY study_date DESC', (mrn,))
    imaging = c.fetchall()
    
    # Get billing
    c.execute('SELECT * FROM billing WHERE mrn = ? ORDER BY service_date DESC', (mrn,))
    billing = c.fetchall()
    
    conn.close()
    
    if not demographics:
        flash('Patient not found', 'error')
        return redirect(url_for('index'))
    
    return render_template('patient.html', 
                         demographics=demographics,
                         encounters=encounters,
                         medications=medications,
                         allergies=allergies,
                         labs=labs,
                         imaging=imaging,
                         billing=billing)

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
            
            # Check if CCDA/XML file
            ext = filename.rsplit('.', 1)[1].lower()
            
            if ext in ['xml', 'ccd']:
                # Try to parse as CCDA
                try:
                    import ccda_parser
                    data = ccda_parser.parse_ccda(filepath)
                    
                    # Export and import each type
                    temp_dir = app.config['UPLOAD_FOLDER']
                    files = ccda_parser.export_to_csv(data, temp_dir)
                    
                    for dtype, csv_path in files.items():
                        if os.path.getsize(csv_path) > 0:
                            rows, errors = import_csv(dtype, csv_path, FIELD_MAPPINGS.get(dtype, {}))
                            flash(f'Imported {rows} {dtype} records', 'success')
                    
                    flash('CCDA file parsed and imported successfully', 'success')
                except Exception as e:
                    flash(f'Error parsing CCDA: {str(e)}', 'error')
            else:
                # Regular CSV import
                mappings = FIELD_MAPPINGS.get(data_type, {})
                if mappings:
                    try:
                        rows, errors = import_csv(data_type, filepath, mappings)
                        flash(f'Successfully imported {rows} records' + (f', {errors} errors' if errors else ''), 'success')
                    except Exception as e:
                        flash(f'Error importing file: {str(e)}', 'error')
                else:
                    flash('Unknown data type', 'error')
            
            return redirect(url_for('upload'))
        else:
            flash('Invalid file type. Please upload a CSV or XML/CCDA file.', 'error')
    
    return render_template('upload.html')

@app.route('/upload_imaging', methods=['GET', 'POST'])
def upload_imaging():
    if request.method == 'POST':
        mrn = request.form.get('mrn')
        file = request.files.get('file')
        study_type = request.form.get('study_type')
        study_date = request.form.get('study_date')
        description = request.form.get('description')
        
        if not all([mrn, file, study_type, study_date]):
            flash('Please fill all required fields', 'error')
            return redirect(url_for('upload_imaging'))
        
        try:
            import_imaging(mrn, file, study_type, study_date, description)
            flash('Imaging uploaded successfully', 'success')
        except Exception as e:
            flash(f'Error uploading imaging: {str(e)}', 'error')
        
        return redirect(url_for('upload_imaging'))
    
    return render_template('upload_imaging.html')

@app.route('/image/<path:filepath>')
def serve_image(filepath):
    return send_from_directory('.', filepath)

@app.route('/stats')
def stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    stats = {}
    tables = ['demographics', 'encounters', 'medications', 'allergies', 'labs', 'imaging', 'billing']
    for table in tables:
        c.execute(f'SELECT COUNT(*) FROM {table}')
        stats[table] = c.fetchone()[0]
    
    conn.close()
    return render_template('stats.html', stats=stats)

# Field mappings for CSV import
FIELD_MAPPINGS = {
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
    },
    'imaging': {
        'mrn': 'MRN', 'study_type': 'Study Type', 'study_date': 'Date',
        'description': 'Description', 'findings': 'Findings'
    },
    'billing': {
        'mrn': 'MRN', 'service_date': 'Date', 'service_description': 'Description',
        'cpt_code': 'CPT Code', 'charge': 'Charge', 'payment': 'Payment',
        'insurance': 'Insurance', 'status': 'Status'
    }
}

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)
