import requests

from config import SERVER_URL, UPLOAD_TOKEN

def upload_txt(text):
    """
    Upload plain text to the server
    :param text: str. the text to be uploaded
    :return: The HTTP response
    """
    target = f"{SERVER_URL}/upload_text"
    r = requests.post(target,
                      headers={"Content-Type": "text/plain", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=text)
    return r.text

def upload_json(json):
    """
        Upload JSON to the server
        :param json: dict. the json to be uploaded
        :return: The HTTP response
        """
    target = f"{SERVER_URL}/upload_json"
    r = requests.post(target,
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=json)
    return r.text

def upload_loc(json):
    """
    Upload a location to the server
    :param json: dict. the location to be uploaded. Must contain "lat", "lon" and "timestamp"
    :return: The HTTP response
    """
    target = f"{SERVER_URL}/upload_loc"
    r = requests.post(target,
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=json)
    return r.text

def upload_fx(json):
    """
    Upload FX data to the server
    :param json: str. the json response of a FX query
    :return: The HTTP response
    """
    target = f"{SERVER_URL}/upload_fx"
    r = requests.post(target,
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=json)
    return r.text

def get_recent_locations(num_days=7):
    """
    Get the location history for the last `num_days` days
    :param num_days: int. The number of days to look for. Default is 7.
    :return: List of JSON location entries
    """
    return requests.get(f"{SERVER_URL}/locations/recent?days={num_days}").json()