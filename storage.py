# storage.py  (S3/MinIO enabled Excel backend with file locking)
import os
import pandas as pd
from filelock import FileLock
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import io

TABLES = {
    'users': ['id','email','name','role','password_hash','api_token','created_at'],
    'labs': ['id','name','contact_email','is_active','created_at'],
    'cases': ['id','case_number','offence_type','description','priority_score','status','created_at','created_by','lab_assigned'],
    'samples': ['id','case_number','code','qr_path','status','created_at'],
    'custody_events': ['id','case_number','sample_code','actor','action','timestamp','note','prev_hash','hash'],
    'lab_results': ['id','case_number','sample_code','lab_user','result_summary','result_file','created_at']
}

class Storage:
    def __init__(self, file_path='instance/data/forensic_cases.xlsx', use_excel=True):
        self.use_excel = use_excel
        self.xlsx_path = file_path
        self.data_dir = os.path.dirname(file_path)
        if self.data_dir:  # avoid empty string
            os.makedirs(self.data_dir, exist_ok=True)
        self.lock_path = os.path.join(self.data_dir, 'data.lock') if self.data_dir else 'data.lock'
        self.lock = FileLock(self.lock_path)
        # S3 config
        self.s3_enabled = bool(int(os.getenv('S3_ENABLED','0')))
        self.s3_bucket = os.getenv('S3_BUCKET','')
        self.s3_key = os.getenv('S3_WORKBOOK_KEY','fasttrack/data_workbook.xlsx')
        self.s3_endpoint = os.getenv('S3_ENDPOINT_URL','') or None
        self.s3_region = os.getenv('S3_REGION','us-east-1')

        # initialize s3 client if enabled
        if self.s3_enabled:
            import boto3
            from botocore.exceptions import ClientError
            session = boto3.session.Session()
            s3_params = {'region_name': self.s3_region}
            if self.s3_endpoint:
                s3_params['endpoint_url'] = self.s3_endpoint
            self.s3 = session.client('s3',
                                     aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                     aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                     **s3_params)
            self._download_from_s3_if_exists()

        self._ensure_tables()


    def _download_from_s3_if_exists(self):
        try:
            # create data dir if missing
            os.makedirs(self.data_dir, exist_ok=True)
            # attempt get object
            resp = self.s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
            body = resp['Body'].read()
            with open(self.xlsx_path, 'wb') as f:
                f.write(body)
            # downloaded successfully
        except ClientError as e:
            # if not found, silence; workbook will be created locally on first write
            code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
            if code in ('NoSuchKey', '404', 'NoSuchBucket'):
                return
            # other errors rethrow
            # print('S3 download error', e)
            return

    def _upload_to_s3(self):
        if not self.s3_enabled:
            return
        try:
            with open(self.xlsx_path, 'rb') as f:
                self.s3.put_object(Bucket=self.s3_bucket, Key=self.s3_key, Body=f.read())
        except Exception as e:
            # best-effort: log to stdout so Render shows it
            print("S3 upload failed:", e)

    def _ensure_tables(self):
        if self.use_excel:
            if not os.path.exists(self.xlsx_path):
                with pd.ExcelWriter(self.xlsx_path, engine='openpyxl') as writer:
                    for t, cols in TABLES.items():
                        df = pd.DataFrame(columns=cols)
                        df.to_excel(writer, sheet_name=t, index=False)
                # if s3 enabled, upload initial workbook
                if self.s3_enabled:
                    self._upload_to_s3()
        else:
            for t, cols in TABLES.items():
                path = os.path.join(self.data_dir, f"{t}.csv")
                if not os.path.exists(path):
                    df = pd.DataFrame(columns=cols)
                    df.to_csv(path, index=False)

    def _read(self, table):
        # if S3 enabled, refresh local copy before read
        if self.s3_enabled:
            # try to download fresh copy (non-blocking)
            try:
                self._download_from_s3_if_exists()
            except Exception:
                pass
        if self.use_excel:
            try:
                df = pd.read_excel(self.xlsx_path, sheet_name=table, engine='openpyxl')
                return df.fillna('')
            except Exception:
                return pd.DataFrame(columns=TABLES[table])
        else:
            path = os.path.join(self.data_dir, f"{table}.csv")
            try:
                df = pd.read_csv(path, dtype=str)
                return df.fillna('')
            except Exception:
                return pd.DataFrame(columns=TABLES[table])

    def _write(self, table, df):
        with self.lock:
            if self.use_excel:
                with pd.ExcelWriter(self.xlsx_path, engine='openpyxl') as writer:
                    # preserve other sheets by reading them first
                    existing = {}
                    try:
                        if os.path.exists(self.xlsx_path):
                            for t in TABLES.keys():
                                if t == table:
                                    continue
                                try:
                                    other = pd.read_excel(self.xlsx_path, sheet_name=t, engine='openpyxl')
                                except Exception:
                                    other = pd.DataFrame(columns=TABLES[t])
                                existing[t] = other
                    except Exception:
                        existing = {}
                    # write target
                    df.to_excel(writer, sheet_name=table, index=False)
                    # write preserved sheets
                    for t, other in existing.items():
                        other.to_excel(writer, sheet_name=t, index=False)
                # after local write, upload to S3 (if enabled)
                if self.s3_enabled:
                    self._upload_to_s3()
            else:
                path = os.path.join(self.data_dir, f"{table}.csv")
                df.to_csv(path, index=False)

    def all(self, table):
        df = self._read(table)
        return df.to_dict(orient='records')

    def find(self, table, **kwargs):
        df = self._read(table)
        if df.empty:
            return None
        mask = pd.Series([True] * len(df))
        for k, v in kwargs.items():
            if k in df.columns:
                mask = mask & (df[k].astype(str) == str(v))
            else:
                return None
        res = df[mask]
        if res.empty:
            return None
        return res.iloc[0].to_dict()

    def filter(self, table, **kwargs):
        df = self._read(table)
        if df.empty:
            return []
        mask = pd.Series([True] * len(df))
        for k, v in kwargs.items():
            if k in df.columns:
                mask = mask & (df[k].astype(str) == str(v))
            else:
                return []
        res = df[mask]
        return res.to_dict(orient='records')

    def append(self, table, row: dict):
        df = self._read(table)
        new = row.copy()
        # id assignment
        if 'id' in TABLES[table]:
            if df.empty:
                new_id = 1
            else:
                try:
                    maxid = pd.to_numeric(df['id'], errors='coerce').max()
                    new_id = int(maxid) + 1 if not pd.isna(maxid) else 1
                except Exception:
                    new_id = len(df) + 1
            new['id'] = new_id
        # timestamps
        if 'created_at' in TABLES[table] and 'created_at' not in new:
            new['created_at'] = datetime.utcnow().isoformat()
        df = pd.concat([df, pd.DataFrame([new])], ignore_index=True, sort=False)
        self._write(table, df)
        return new

    def update(self, table, id_field, id_value, updates: dict):
        df = self._read(table)
        if id_field not in df.columns:
            return False
        mask = df[id_field].astype(str) == str(id_value)
        if not mask.any():
            return False
        for k, v in updates.items():
            if k not in df.columns:
                df[k] = ''
            df.loc[mask, k] = v
        self._write(table, df)
        return True

    def last_event_hash(self, case_number):
        events = self.filter('custody_events', case_number=case_number)
        if not events:
            return ''
        events_sorted = sorted(events, key=lambda e: e.get('timestamp',''))
        return events_sorted[-1].get('hash','')

    def next_case_sequence(self):
        df = self._read('cases')
        if df.empty:
            return 1
        try:
            maxid = pd.to_numeric(df['id'], errors='coerce').max()
            return int(maxid) + 1
        except Exception:
            return len(df) + 1
