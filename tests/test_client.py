import json
import requests
REQUEST_SERVICE_URL = 'http://localhost:8000'

def pretty(title, data):
    print(f'\n--- {title} ---')
    print(json.dumps(data, indent=2))

def main():
    r = requests.post(f'{REQUEST_SERVICE_URL}/request_blood', json={'blood_group': 'O+', 'units': 2, 'urgency': 'normal'})
    pretty('Normal request (should be served locally by Pune)', r.json())
    r = requests.post(f'{REQUEST_SERVICE_URL}/request_blood', json={'blood_group': 'AB-', 'units': 5, 'urgency': 'normal'})
    pretty('P2P fallback request (Pune has only 1 unit of AB-, needs 5)', r.json())
    r = requests.post(f'{REQUEST_SERVICE_URL}/request_blood', json={'blood_group': 'B-', 'units': 1, 'urgency': 'emergency'})
    pretty('Emergency request (also broadcast via message queue)', r.json())
    r = requests.get(f'{REQUEST_SERVICE_URL}/api/dashboard_data')
    pretty('Dashboard aggregate data', r.json())
if __name__ == '__main__':
    main()
