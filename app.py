import os
import sqlite3
import csv
import shutil
from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, session
from werkzeug.utils import secure_filename
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

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

# Flask-Login setup
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    return User(int(user_id))

class User:
    def __init__(self, id):
        self.id = id
        user = get_user(id)
        if user:
            self.username = user[1]
            self.role = user[2]
        else:
            self.username = None
            self.role = None
    
    def is_authenticated(self:
        return self.username is not None
    
    def is_active(self:
        return True
    
    def is_anonymous(self:
        return False
    
    def get_id(self):
        return str(self.id)

# Authentication routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        
        user_data = verify_user(username, password)
        if user_data:
            user = User(user_data['id'])
            login_user(user)
            log_audit(user.id, user.username, 'LOGIN', 'User logged in')
            
            # Update last login
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?', (user.id,))
            conn.commit()
            conn.close()
            
            return redirect(url_for('index'))
        else:
            flash('Invalid username or password', 'error')
    
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    username = current_user.username
    user_id = current_user.id
    logout_user()
    log_audit(user_id, username, 'LOGOUT', 'User logged out')
    flash('You have been logged out', 'success')
    return redirect(url_for('login'))

# Admin routes
@app.route('/admin/users', methods=['GET', 'POST'])
@login_required
def admin_users():
    if current_user.role != 'admin':
        flash('Access denied', 'error')
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'create':
            username = request.form.get('username', '').strip()
            password = request.form.get('password', '')
            role = request.form.get('role', 'user')
            
            if username and password:
                if create_user(username, password, role):
                    flash(f'User {username} created successfully', 'success')
                    log_audit(current_user.id, current_user.username, 'USER_CREATE', f'Created user: {username}')
                else:
                    flash('Username already exists', 'error')
        elif action == 'delete':
            user_id = request.form.get('user_id')
            if user_id:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('SELECT username FROM users WHERE id = ?', (user_id,))
                row = c.fetchone()
                if row:
                    c.execute('DELETE FROM users WHERE id = ?', (user_id,))
                    conn.commit()
                    flash(f'User deleted', 'success')
                    log_audit(current_user.id, current_user.username, 'USER_DELETE', f'Deleted user: {row[0]}')
                conn.close()
    
    users = get_all_users()
    return render_template('admin_users.html', users=users)

@app.route('/admin/applications', methods=['GET', 'POST'])
@login_required
def admin_applications():
    if current_user.role != 'admin':
        flash('Access denied', 'error')
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'create':
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '')
            
            if name:
                create_application(name, description)
                flash(f'Application {name} created', 'success')
                log_audit(current_user.id, current_user.username, 'APP_CREATE', f'Created app: {name}')
        elif action == 'delete':
            app_id = request.form.get('app_id')
            if app_id:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('SELECT name FROM applications WHERE id = ?', (app_id,))
                row = c.fetchone()
                if row:
                    c.execute('DELETE FROM applications WHERE id = ?', (app_id,))
                    c.execute('DELETE FROM user_app_access WHERE app_id = ?', (app_id,))
                    conn.commit()
                    flash(f'Application deleted', 'success')
                    log_audit(current_user.id, current_user.username, 'APP_DELETE', f'Deleted app: {row[0]}')
                conn.close()
        elif action == 'grant':
            user_id = request.form.get('user_id')
            app_id = request.form.get('app_id')
            if user_id and app_id:
                grant_user_app_access(user_id, app_id)
                log_audit(current_user.id, current_user.username, 'ACCESS_GRANT', f'Granted user {user_id} access to app {app_id}')
                flash('Access granted', 'success')
        elif action == 'revoke':
            user_id = request.form.get('user_id')
            app_id = request.form.get('app_id')
            if user_id and app_id:
                revoke_user_app_access(user_id, app_id)
                log_audit(current_user.id, current_user.username, 'ACCESS_REVOKE', f'Revoked user {user_id} access to app {app_id}')
                flash('Access revoked', 'success')
    
    applications = get_all_applications()
    users = get_all_users()
    return render_template('admin_applications.html', applications=applications, users=users)

@app.route('/admin/audit')
@login_required
def admin_audit():
    if current_user.role != 'admin':
        flash('Access denied', 'error')
        return redirect(url_for('index'))
    
    logs = get_audit_log(200)
    return render_template('admin_audit.html', logs=logs)

def init_db():
    """Initialize database tables and ensure crosswalk table exists"""
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
    c.execute('''CREATE TABLE IF NOT EXISTS mrn_crosswalk (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        legacy_mrn TEXT UNIQUE,
        epic_mrn TEXT UNIQUE
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_crosswalk_epic ON mrn_crosswalk(epic_mrn)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_crosswalk_legacy ON mrn_crosswalk(legacy_mrn)')
    
    # Users table
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT DEFAULT 'user',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        last_login TEXT
    )''')
    
    # Applications table
    c.execute('''CREATE TABLE IF NOT EXISTS applications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # User-Application access table
    c.execute('''CREATE TABLE IF NOT EXISTS user_app_access (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        app_id INTEGER,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (app_id) REFERENCES applications(id),
        UNIQUE(user_id, app_id)
    )''')
    
    # Audit log table
    c.execute('''CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        username TEXT,
        action TEXT NOT NULL,
        details TEXT,
        ip_address TEXT,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    
    conn.commit()
    conn.close()

# Audit logging
def log_audit(user_id, username, action, details='', ip_address=''):
    """Log an audit event."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO audit_log (user_id, username, action, details, ip_address) VALUES (?, ?, ?, ?, ?)',
              (user_id, username, action, details, ip_address))
    conn.commit()
    conn.close()

# User management functions
def create_user(username, password, role='user'):
    """Create a new user with hashed password."""
    from werkzeug.security import generate_password_hash
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
                  (username, generate_password_hash(password), role))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def verify_user(username, password):
    """Verify username and password."""
    from werkzeug.security import check_password_hash
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, password_hash, role FROM users WHERE username = ?', (username,))
    row = c.fetchone()
    conn.close()
    if row and check_password_hash(row[1], password):
        return {'id': row[0], 'role': row[1]}
    return None

def get_user(user_id):
    """Get user by ID."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, username, role, created_at, last_login FROM users WHERE id = ?', (user_id,))
    row = c.fetchone()
    conn.close()
    return row

def get_all_users():
    """Get all users."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, username, role, created_at, last_login FROM users ORDER BY username')
    rows = c.fetchall()
    conn.close()
    return rows

# Application management functions
def create_application(name, description=''):
    """Create a new application."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('INSERT INTO applications (name, description) VALUES (?, ?)', (name, description))
        conn.commit()
        return True
    finally:
        conn.close()

def get_all_applications():
    """Get all applications."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, name, description, created_at FROM applications ORDER BY name')
    rows = c.fetchall()
    conn.close()
    return rows

def get_user_applications(user_id):
    """Get applications a user has access to."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT a.id, a.name, a.description 
                 FROM applications a 
                 JOIN user_app_access uaa ON a.id = uaa.app_id 
                 WHERE uaa.user_id = ?''', (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def grant_user_app_access(user_id, app_id):
    """Grant a user access to an application."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('INSERT OR IGNORE INTO user_app_access (user_id, app_id) VALUES (?, ?)', (user_id, app_id))
        conn.commit()
        return True
    finally:
        conn.close()

def revoke_user_app_access(user_id, app_id):
    """Revoke a user's access to an application."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM user_app_access WHERE user_id = ? AND app_id = ?', (user_id, app_id))
    conn.commit()
    conn.close()

def get_audit_log(limit=100):
    """Get recent audit log entries."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, username, action, details, ip_address, timestamp FROM audit_log ORDER BY timestamp DESC LIMIT ?', (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

# Crosswalk utility functions
def add_crosswalk(legacy_mrn, epic_mrn):
    """Insert or update an MRN crosswalk mapping."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('INSERT OR REPLACE INTO mrn_crosswalk (legacy_mrn, epic_mrn) VALUES (?, ?)', (legacy_mrn, epic_mrn))
        conn.commit()
        return True
    finally:
        conn.close()

def get_epic_mrn(legacy_mrn):
    """Retrieve the Epic MRN for a given legacy MRN, or None if not found."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT epic_mrn FROM mrn_crosswalk WHERE legacy_mrn = ?', (legacy_mrn,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def import_csv(table_name, filepath, field_mapping, resolve_epic=False):
    """Import CSV into database table
    
    Args:
        table_name: Target database table
        filepath: Path to CSV file
        field_mapping: Dict mapping DB fields to CSV column names
        resolve_epic: If True, resolve legacy MRNs to Epic MRNs using crosswalk table
    """
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
                    # Handle Epic MRN resolution for demographics
                    if resolve_epic and db_field == 'mrn':
                        legacy_mrn = row.get(csv_field, '').strip()
                        epic_mrn = get_epic_mrn(legacy_mrn)
                        values.append(epic_mrn if epic_mrn else legacy_mrn)
                    elif csv_field in row and row[csv_field]:
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
                        # Check if we should resolve to Epic MRN
                        resolve_epic = request.form.get('resolve_epic') == 'on'
                        rows, errors = import_csv(data_type, filepath, mappings, resolve_epic=resolve_epic)
                        resolve_msg = ' (resolved to Epic MRNs)' if resolve_epic else ''
                        flash(f'Successfully imported {rows} records{resolve_msg}' + (f', {errors} errors' if errors else ''), 'success')
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

@app.route('/crosswalk', methods=['GET', 'POST'])
def crosswalk():
    """Manage MRN crosswalk - add new mappings or search existing."""
    if request.method == 'POST':
        legacy_mrn = request.form.get('legacy_mrn', '').strip()
        epic_mrn = request.form.get('epic_mrn', '').strip()
        if legacy_mrn and epic_mrn:
            add_crosswalk(legacy_mrn, epic_mrn)
            flash(f'Mapped legacy MRN {legacy_mrn} to Epic MRN {epic_mrn}', 'success')
        else:
            flash('Please provide both legacy and Epic MRNs', 'error')
        return redirect(url_for('crosswalk'))
    
    # GET: show existing mappings
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM mrn_crosswalk ORDER BY legacy_mrn')
    mappings = c.fetchall()
    conn.close()
    return render_template('crosswalk.html', mappings=mappings)

@app.route('/crosswalk/<legacy_mrn>')
def crosswalk_lookup(legacy_mrn):
    """Look up Epic MRN for a given legacy MRN."""
    epic_mrn = get_epic_mrn(legacy_mrn)
    if epic_mrn:
        return {'legacy_mrn': legacy_mrn, 'epic_mrn': epic_mrn}
    return {'error': 'Mapping not found'}, 404

@app.route('/crosswalk/import', methods=['GET', 'POST'])
def crosswalk_import():
    """Bulk import MRN crosswalk mappings from CSV."""
    if request.method == 'POST':
        file = request.files.get('file')
        if not file:
            flash('Please select a file', 'error')
            return redirect(url_for('crosswalk_import'))
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            try:
                with open(filepath, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    count = 0
                    for row in reader:
                        legacy_mrn = row.get('legacy_mrn', '').strip()
                        epic_mrn = row.get('epic_mrn', '').strip()
                        if legacy_mrn and epic_mrn:
                            add_crosswalk(legacy_mrn, epic_mrn)
                            count += 1
                    flash(f'Imported {count} MRN mappings', 'success')
            except Exception as e:
                flash(f'Error importing CSV: {str(e)}', 'error')
            
            return redirect(url_for('crosswalk_import'))
        else:
            flash('Invalid file type. Please upload a CSV file.', 'error')
    
    return render_template('crosswalk_import.html')

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
    
    # Create default admin user if none exists
    users = get_all_users()
    if not users:
        create_user('admin', 'admin123', 'admin')
        print('Default admin user created: admin / admin123')
    
    app.run(debug=True, host='0.0.0.0', port=5000)
