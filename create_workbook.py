# create_workbook.py
import pandas as pd, os
TABLES = {
    'users': ['id','email','name','role','password_hash','api_token','created_at'],
    'labs': ['id','name','contact_email','is_active','created_at'],
    'cases': ['id','case_number','offence_type','description','priority_score','status','created_at','created_by','lab_assigned'],
    'samples': ['id','case_number','code','qr_path','status','created_at'],
    'custody_events': ['id','case_number','sample_code','actor','action','timestamp','note','prev_hash','hash'],
    'lab_results': ['id','case_number','sample_code','lab_user','result_summary','result_file','created_at']
}
os.makedirs('instance/data', exist_ok=True)
path = 'instance/data/data_workbook.xlsx'
with pd.ExcelWriter(path, engine='openpyxl') as writer:
    for sheet, cols in TABLES.items():
        pd.DataFrame(columns=cols).to_excel(writer, sheet_name=sheet, index=False)
print("Workbook created at", path)
