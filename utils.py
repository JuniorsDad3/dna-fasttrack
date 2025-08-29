# utils.py
import hashlib
import qrcode
import os
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from io import BytesIO

OFFENCE_WEIGHTS = {
    'murder': 100,
    'rape': 100,
    'armed_robbery': 80,
    'assault': 60,
    'burglary': 40,
    'other': 20
}

def compute_priority(offence_type, age_days=0, suspect_in_custody=False):
    base = OFFENCE_WEIGHTS.get(offence_type, OFFENCE_WEIGHTS['other'])
    age_bonus = min(age_days // 7, 50)
    custody_bonus = 30 if suspect_in_custody else 0
    return int(base + age_bonus + custody_bonus)

def make_qr(code, static_folder='static'):
    qr_dir = os.path.join(static_folder, 'qrcodes')
    os.makedirs(qr_dir, exist_ok=True)
    path = os.path.join(qr_dir, f"{code}.png")
    img = qrcode.make(code)
    img.save(path)
    return os.path.relpath(path, static_folder)

def compute_event_hash(prev_hash, payload_dict):
    payload = (prev_hash or '') + '|' + '|'.join(f"{k}={v}" for k,v in sorted(payload_dict.items()))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()

def hash_password(raw):
    return generate_password_hash(raw)

def check_password_hash_stored(pw_hash, raw):
    return check_password_hash(pw_hash, raw)
