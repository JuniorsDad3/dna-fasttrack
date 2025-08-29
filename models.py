import pandas as pd
from datetime import datetime
import hashlib
from werkzeug.security import generate_password_hash, check_password_hash
import os

EXCEL_FILE = "forensic_cases.xlsx"

ROLE_ADMIN = "admin"
ROLE_LAB = "lab"
ROLE_OFFICER = "officer"

# Ensure Excel sheets exist
if not os.path.exists(EXCEL_FILE):
    with pd.ExcelWriter(EXCEL_FILE) as writer:
        pd.DataFrame(columns=['id','email','name','role','password_hash','api_token']).to_excel(writer, sheet_name='users', index=False)
        pd.DataFrame(columns=['id','name','contact_email','is_active']).to_excel(writer, sheet_name='labs', index=False)
        pd.DataFrame(columns=['id','case_number','offence_type','description','priority_score','status','created_at','created_by_id','lab_id']).to_excel(writer, sheet_name='cases', index=False)
        pd.DataFrame(columns=['id','case_id','code','qr_path','status']).to_excel(writer, sheet_name='samples', index=False)
        pd.DataFrame(columns=['id','case_id','sample_id','actor_id','action','timestamp','note','prev_hash','hash']).to_excel(writer, sheet_name='custody_events', index=False)

def load_sheet(sheet_name):
    return pd.read_excel(EXCEL_FILE, sheet_name=sheet_name)

def save_sheet(df, sheet_name):
    with pd.ExcelWriter(EXCEL_FILE, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

# -------------------
# User
# -------------------
class User:
    def __init__(self, email, name, role="officer", password_hash=None, api_token=None, id=None):
        self.id = id
        self.email = email
        self.name = name
        self.role = role
        self.password_hash = password_hash
        self.api_token = api_token

    def set_password(self, raw):
        self.password_hash = generate_password_hash(raw)
        self.save()

    def check_password(self, raw):
        return check_password_hash(self.password_hash, raw)

    def save(self):
        df = load_sheet('users')
        if self.id is None:
            self.id = df['id'].max() + 1 if not df.empty else 1
            df = pd.concat([df, pd.DataFrame([self.__dict__])], ignore_index=True)
        else:
            df.loc[df['id'] == self.id, df.columns] = pd.Series(self.__dict__)
        save_sheet(df, 'users')

    @staticmethod
    def get_by_email(email):
        df = load_sheet('users')
        row = df[df['email'] == email]
        if not row.empty:
            return User(**row.iloc[0].to_dict())
        return None

# -------------------
# Case
# -------------------
class Case:
    def __init__(self, case_number, offence_type, description="", priority_score=0, status="created", created_at=None, created_by_id=None, lab_id=None, id=None):
        self.id = id
        self.case_number = case_number
        self.offence_type = offence_type
        self.description = description
        self.priority_score = priority_score
        self.status = status
        self.created_at = created_at or datetime.utcnow()
        self.created_by_id = created_by_id
        self.lab_id = lab_id

    def save(self):
        df = load_sheet('cases')
        if self.id is None:
            self.id = df['id'].max() + 1 if not df.empty else 1
            df = pd.concat([df, pd.DataFrame([self.__dict__])], ignore_index=True)
        else:
            df.loc[df['id'] == self.id, df.columns] = pd.Series(self.__dict__)
        save_sheet(df, 'cases')

# -------------------
# Sample
# -------------------
class Sample:
    def __init__(self, case_id, code, qr_path="", status="sealed", id=None):
        self.id = id
        self.case_id = case_id
        self.code = code
        self.qr_path = qr_path
        self.status = status

    def save(self):
        df = load_sheet('samples')
        if self.id is None:
            self.id = df['id'].max() + 1 if not df.empty else 1
            df = pd.concat([df, pd.DataFrame([self.__dict__])], ignore_index=True)
        else:
            df.loc[df['id'] == self.id, df.columns] = pd.Series(self.__dict__)
        save_sheet(df, 'samples')

# -------------------
# CustodyEvent
# -------------------
class CustodyEvent:
    def __init__(self, case_id, actor_id, action, sample_id=None, note="", prev_hash="", hash_val="", id=None):
        self.id = id
        self.case_id = case_id
        self.sample_id = sample_id
        self.actor_id = actor_id
        self.action = action
        self.timestamp = datetime.utcnow()
        self.note = note
        self.prev_hash = prev_hash
        self.hash = hash_val or self.compute_hash(prev_hash, f"{case_id}-{actor_id}-{action}-{self.timestamp}")

    @staticmethod
    def compute_hash(prev_hash, payload):
        h = hashlib.sha256()
        h.update((prev_hash or "").encode("utf-8"))
        h.update(payload.encode("utf-8"))
        return h.hexdigest()

    def save(self):
        df = load_sheet('custody_events')
        if self.id is None:
            self.id = df['id'].max() + 1 if not df.empty else 1
            df = pd.concat([df, pd.DataFrame([self.__dict__])], ignore_index=True)
        else:
            df.loc[df['id'] == self.id, df.columns] = pd.Series(self.__dict__)
        save_sheet(df, 'custody_events')

class Lab:
    def __init__(self, lab_id, name, location):
        self.lab_id = lab_id
        self.name = name
        self.location = location
