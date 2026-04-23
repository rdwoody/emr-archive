#!/usr/bin/env python3
"""
CCDA (XML) Parser for EMR Archive
Extracts patient data from Continuity of Care Documents
"""
import os
import csv
import xml.etree.ElementTree as ET
import sys

# CCDA namespaces
NS = {'ccda': 'urn:hl7-org:v3'}

def parse_ccda(xml_file):
    """Parse CCDA XML file and extract all data sections"""
    
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    data = {
        'demographics': [],
        'medications': [],
        'allergies': [],
        'encounters': [],
        'labs': []
    }
    
    # Try to find MRN - varies by system, check common locations
    mrn = find_mrn(root)
    
    # Parse demographics (patient role)
    demo = parse_demographics(root, mrn)
    if demo:
        data['demographics'].append(demo)
    
    # Parse medications (substance administration)
    data['medications'] = parse_medications(root, mrn)
    
    # Parse allergies (adverse observations)
    data['allergies'] = parse_allergies(root, mrn)
    
    # Parse encounters
    data['encounters'] = parse_encounters(root, mrn)
    
    # Parse labs (observations)
    data['labs'] = parse_labs(root, mrn)
    
    return data

def find_mrn(root):
    """Find MRN in various CCDA locations"""
    # Check common ID locations
    for id_elem in root.iter('{urn:hl7-org:v3}id'):
        extension = id_elem.get('extension')
        root_id = id_elem.get('root')
        if extension and ('MRN' in str(root_id).upper() or 'ID' in str(root_id).upper()):
            return extension
        if extension:
            return extension
    return 'UNKNOWN'

def parse_demographics(root, mrn):
    """Extract patient demographics"""
    # Find patient role
    patient_role = root.find('.//ccda:patientRole', NS)
    if patient_role is None:
        # Try without namespace
        patient_role = root.find('.//patientRole')
    
    if patient_role is None:
        return {'mrn': mrn}
    
    demo = {'mrn': mrn}
    
    # Name
    name_elem = patient_role.find('.//ccda:name', NS)
    if name_elem is None:
        name_elem = patient_role.find('.//name')
    
    if name_elem is not None:
        given = name_elem.find('.//ccda:given', NS)
        if given is None:
            given = name_elem.find('.//given')
        family = name_elem.find('.//ccda:family', NS)
        if family is None:
            family = name_elem.find('.//family')
        
        demo['First Name'] = given.text if given is not None and given.text else ''
        demo['Last Name'] = family.text if family is not None and family.text else ''
    
    # Address
    addr_elem = patient_role.find('.//ccda:addr', NS)
    if addr_elem is None:
        addr_elem = patient_role.find('.//addr')
    
    if addr_elem is not None:
        street = ''
        city = ''
        state = ''
        zip_code = ''
        for line in addr_elem:
            if 'street' in line.tag.lower():
                street += line.text or ''
            elif 'city' in line.tag.lower():
                city = line.text or ''
            elif 'state' in line.tag.lower():
                state = line.text or ''
            elif 'postal' in line.tag.lower():
                zip_code = line.text or ''
        demo['Address'] = street
        demo['City'] = city
        demo['State'] = state
        demo['ZIP'] = zip_code
    
    # Phone
    phone_elem = patient_role.find('.//ccda:telecom[@use="HP"]', NS)
    if phone_elem is None:
        phone_elem = patient_role.find('.//telecom[@use="HP"]')
    if phone_elem is None:
        phone_elem = patient_role.find('.//telecom')
    demo['Phone'] = phone_elem.get('value', '') if phone_elem is not None else ''
    
    # Gender
    gender_elem = root.find('.//ccda:administrativeGenderCode', NS)
    if gender_elem is None:
        gender_elem = root.find('.//administrativeGenderCode')
    demo['Gender'] = gender_elem.get('code', '') if gender_elem is not None else ''
    
    # DOB
    dob_elem = root.find('.//ccda:birthTime', NS)
    if dob_elem is None:
        dob_elem = root.find('.//birthTime')
    if dob_elem is not None:
        dob = dob_elem.get('value', '')
        if len(dob) >= 8:
            demo['DOB'] = f"{dob[4:6]}/{dob[6:8]}/{dob[:4]}"
        else:
            demo['DOB'] = dob
    
    return demo

def parse_medications(root, mrn):
    """Extract medication list"""
    meds = []
    
    # Find substance administrations (medications)
    for med in root.findall('.//ccda:substanceAdministration[@classCode="SBADM"]', NS):
        if med is None:
            med = root.findall('.//substanceAdministration[@classCode="SBADM"]')
        
        entry = {'MRN': mrn}
        
        # Medication name
        name_elem = med.find('.//ccda:name', NS)
        if name_elem is None:
            name_elem = med.find('.//name')
        entry['Medication'] = name_elem.text if name_elem is not None and name_elem.text else ''
        
        # Dose
        dose_elem = med.find('.//ccda:doseQuantity', NS)
        if dose_elem is None:
            dose_elem = med.find('.//doseQuantity')
        if dose_elem is not None:
            entry['Dosage'] = f"{dose_elem.get('value', '')} {dose_elem.get('unit', '')}"
        
        # Frequency
        freq_elem = med.find('.//ccda:frequency', NS)
        if freq_elem is None:
            freq_elem = med.find('.//frequency')
        entry['Frequency'] = freq_elem.text if freq_elem is not None and freq_elem.text else ''
        
        # Start date
        time_elem = med.find('.//ccda:effectiveTime[@key="startTime"]', NS)
        if time_elem is None:
            time_elem = med.find('.//effectiveTime[@key="startTime"]')
        if time_elem is not None:
            start_date = time_elem.get('value', '')
            if len(start_date) >= 8:
                entry['Start Date'] = f"{start_date[4:6]}/{start_date[6:8]}/{start_date[:4]}"
            else:
                entry['Start Date'] = start_date
        
        # End date
        end_elem = med.find('.//ccda:effectiveTime[@key="endTime"]', NS)
        if end_elem is None:
            end_elem = med.find('.//effectiveTime[@key="endTime"]')
        if end_elem is not None:
            end_date = end_elem.get('value', '')
            if len(end_date) >= 8:
                entry['End Date'] = f"{end_date[4:6]}/{end_date[6:8]}/{end_date[:4]}"
            else:
                entry['End Date'] = end_date
        
        # Prescriber
        prescriber = med.find('.//ccda:author/ccda:name', NS)
        if prescriber is None:
            prescriber = med.find('.//author/name')
        if prescriber is not None:
            entry['Prescriber'] = f"{prescriber.findtext('.//given', '')} {prescriber.findtext('.//family', '')}"
        
        if entry.get('Medication'):
            meds.append(entry)
    
    return meds

def parse_allergies(root, mrn):
    """Extract allergy list"""
    allergies = []
    
    for allergy in root.findall('.//ccda:observation[@classCode="OBS"]', NS):
        # Try to identify as allergy
        entry = {'MRN': mrn}
        
        # Allergen
        name_elem = allergy.find('.//ccda:name', NS)
        if name_elem is None:
            name_elem = allergy.find('.//name')
        entry['Allergen'] = name_elem.text if name_elem is not None and name_elem.text else ''
        
        # Reaction
        reaction_elem = allergy.find('.//ccda:value', NS)
        if reaction_elem is None:
            reaction_elem = allergy.find('.//value')
        entry['Reaction'] = reaction_elem.get('displayName', '') if reaction_elem is not None else ''
        
        # Severity
        severity_elem = allergy.find('.//ccda:severity', NS)
        if severity_elem is None:
            severity_elem = allergy.find('.//severity')
        entry['Severity'] = severity_elem.get('displayName', '') if severity_elem is not None else ''
        
        entry['Status'] = 'Active'
        
        if entry.get('Allergen'):
            allergies.append(entry)
    
    return allergies

def parse_encounters(root, mrn):
    """Extract encounter history"""
    encounters = []
    
    for enc in root.findall('.//ccda:encounter[@classCode="ENC"]', NS):
        entry = {'MRN': mrn}
        
        # Date
        date_elem = enc.find('.//ccda:effectiveTime', NS)
        if date_elem is None:
            date_elem = enc.find('.//effectiveTime')
        if date_elem is not None:
            enc_date = date_elem.get('value', '')
            if len(enc_date) >= 8:
                entry['Date'] = f"{enc_date[4:6]}/{enc_date[6:8]}/{enc_date[:4]}"
            else:
                entry['Date'] = enc_date
        
        # Type
        code_elem = enc.find('.//ccda:code', NS)
        if code_elem is None:
            code_elem = enc.find('.//code')
        entry['Type'] = code_elem.get('displayName', '') if code_elem is not None else ''
        
        # Provider
        provider = enc.find('.//ccda:performer/ccda:name', NS)
        if provider is None:
            provider = enc.find('.//performer/name')
        if provider is not None:
            entry['Provider'] = f"{provider.findtext('.//given', '')} {provider.findtext('.//family', '')}"
        
        # Reason/Diagnosis
        reason = enc.find('.//ccda:reasonCode/ccda:name', NS)
        if reason is None:
            reason = enc.find('.//reasonCode/name')
        entry['Diagnosis'] = reason.text if reason is not None and reason.text else ''
        
        encounters.append(entry)
    
    return encounters

def parse_labs(root, mrn):
    """Extract lab results"""
    labs = []
    
    # Find observations that are lab results
    for obs in root.findall('.//ccda:observation[@classCode="OBS"]', NS):
        entry = {'MRN': mrn}
        
        # Check if it's a lab (look for loinc code or lab-specific elements)
        code_elem = obs.find('.//ccda:code', NS)
        if code_elem is None:
            code_elem = obs.find('.//code')
        
        if code_elem is not None and code_elem.get('codeSystem') in ['2.16.840.1.113883.6.1', 'LOINC']:
            entry['Test Name'] = code_elem.get('displayName', '')
        
        # Result
        value_elem = obs.find('.//ccda:value', NS)
        if value_elem is None:
            value_elem = obs.find('.//value')
        
        if value_elem is not None:
            if value_elem.get('value'):
                entry['Result'] = value_elem.get('value')
            else:
                entry['Result'] = value_elem.text or ''
            entry['Unit'] = value_elem.get('unit', '')
        
        # Reference range
        ref_elem = obs.find('.//ccda:referenceRange/ccda:value', NS)
        if ref_elem is None:
            ref_elem = obs.find('.//referenceRange/value')
        if ref_elem is not None:
            entry['Reference Range'] = f"{ref_elem.get('low', '')} - {ref_elem.get('high', '')}"
        
        # Date
        date_elem = obs.find('.//ccda:effectiveTime', NS)
        if date_elem is None:
            date_elem = obs.find('.//effectiveTime')
        if date_elem is not None:
            test_date = date_elem.get('value', '')
            if len(test_date) >= 8:
                entry['Date'] = f"{test_date[4:6]}/{test_date[6:8]}/{test_date[:4]}"
            else:
                entry['Date'] = test_date
        
        entry['Status'] = 'Final'
        
        if entry.get('Test Name'):
            labs.append(entry)
    
    return labs

def export_to_csv(data, output_dir):
    """Export parsed data to CSV files"""
    os.makedirs(output_dir, exist_ok=True)
    
    files = {}
    
    for data_type, records in data.items():
        if not records:
            continue
            
        filepath = os.path.join(output_dir, f"{data_type}.csv")
        
        # Get all unique headers
        headers = set()
        for record in records:
            headers.update(record.keys())
        
        headers = sorted(list(headers))
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(records)
        
        files[data_type] = filepath
    
    return files

if __name__ == '__main__':
    if len(sys.argv) > 1:
        xml_file = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else '.'
        
        print(f"Parsing CCDA: {xml_file}")
        data = parse_ccda(xml_file)
        
        files = export_to_csv(data, output_dir)
        
        print(f"Generated files:")
        for dtype, path in files.items():
            print(f"  {dtype}: {path}")
    else:
        print("Usage: python ccda_parser.py <ccda_file.xml> [output_dir]")
