import requests

from config import SERVER_URL, UPLOAD_TOKEN

def upload_txt(text):
    target = f"{SERVER_URL}/upload_text"
    r = requests.post(target,
                      headers={"Content-Type": "text/plain", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=text)
    return r.text

def upload_json(json):
    target = f"{SERVER_URL}/upload_json"
    r = requests.post(target,
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=json)
    return r.text

def upload_loc(json):
    target = f"{SERVER_URL}/upload_loc"
    r = requests.post(target,
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=json)
    return r.text

def upload_fx(json):
    target = f"{SERVER_URL}/upload_fx"
    r = requests.post(target,
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=json)
    return r.text