import csv
import requests
import time, sys, os, random
from concurrent.futures import ThreadPoolExecutor
import datetime

# Specify the file path of the CSV
csv_file_path = "source.csv"

# Define the endpoints and their payload templates
endpoints = [
    {
        "url": "https://albatross-performance.recruitcrm.io/v1/candidates",
        "method": "POST",
        "payload": lambda row: {
            "candidate": {
                "firstname": "Sample",
                "lastname": "Last",
                "emailid": "same@yopmail.com"
            }
        },
        "store_response": False
    },
    {
        "url": "https://albatross-performance.recruitcrm.io/v1/candidates",
        "method": "POST",
        "payload": lambda row: {
            "candidate": {
                "firstname": "Sample",
                "lastname": "Last",
                "emailid": "diff@yopmail.com"
            }
        },
        "store_response": True
    },
    # {
    #     "url": lambda row: f"https://performanceapi.recruitcrm.io/v1/candidates/{row['slug']}/request-update",
    #     "method": "GET",
    #     "payload": lambda row: {},  # GET requests usually do not send a payload
    #     "store_response": False
    # },
    # {
    #     "url": "https://albatross-performance.recruitcrm.io/v1/external-pages/delete-candidate",
    #     "method": "POST",
    #     "payload": lambda row: {"authcode": "rcrm_"+ row.get('slug', 'default-slug'), "filesInfo":"", "accountid": row.get("encryptedAccountId")},
    #     "store_response": False
    # },
    {
        "url": "https://albatross-performance.recruitcrm.io/v1/external-pages/apply-update-candidate",
        "method": "POST",
        "payload": lambda row: {
            "candidate": {
                "firstname": "Rahul",
                "lastname": "",
                "emailid": f"email{random.randint(1, 9999999)}@email.com",
                "contactnumber": "",
                "resumefilename": "",
                "city": "",
                "state": "",
                "country": "",
                "salary": 0,
                "is_currently_working": 0,
                "slug": "",
                "genderid": 0,
                "locality": "",
                "address": "",
                "qualificationid": 0,
                "specialization": "",
                "workexpyr": 0,
                "workexpmonth": "",
                "relevantexperience": 0,
                "position": "",
                "candidatedob": "",
                "availablefrom": "",
                "currentsalary": 0,
                "lastorganisation": "",
                "skill": "",
                "willingtorelocate": 0,
                "salaryexpectation": 0,
                "salarytype": 2,
                "currentstatus": "",
                "noticeperiod": 0,
                "currencyid": 11,
                "profilefacebook": "",
                "profiletwitter": "",
                "profilelinkedin": "",
                "profilegithub": "",
                "profilexing": "",
                "source": "",
                "resume": "",
                "languageskills": "",
                "sourceadded": "",
                "resumeaddedon": 0,
                "resumeupdatedon": 0,
                "ownerid": "",
                "hotlist": "",
                "off_limit_status": ""
            },
            "accountid": row.get("encryptedAccountId"),
            "address_changed": "",
            "token": "",
            "filesInfo": {},
            "associated_files": {},
            "jobslug": "",
            "answers": [],
            "talentpoolurl": row.get("talentPoolUrl"),
            "educationhistory": [],
            "workhistory": [],
            "deleteWork": [],
            "deleteEducation": [],
            "candidateSummary": "",
            "pagesource": "talentpool",
            "vonqTrackingId": {}
        },
        "store_response": False,
        "headers": {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "origin": "https://performanceweb.recruitcrm.io",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://performanceweb.recruitcrm.io/",
            "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0",
            "x-request-attempt-at": "0"
        }
    }

]

# Function to hit a single endpoint with payload
def hit_endpoint(endpoint, payload, token, apiToken, row):
    headers = endpoint.get("headers", {})
    
    # Evaluate the URL if it's a lambda function
    url = endpoint["url"](row) if callable(endpoint["url"]) else endpoint["url"]

    if "performanceapi" in url:
        headers["Authorization"] = f"Bearer {apiToken}"
    else:
        headers["Authorization"] = f"Bearer {token}"

    try:
        if endpoint["method"] == "POST":
            response = requests.post(url, json=payload, headers=headers, timeout=5)
        elif endpoint["method"] == "GET":
            response = requests.get(url, params=payload, headers=headers, timeout=5)
        else:
            print(f"Unsupported HTTP method: {endpoint['method']}")
            return

        # Generate timestamp
        current_time = datetime.datetime.now()
        print(f"[{current_time}] For user {row['username']} Request to {url} with payload {payload}: {response.status_code}")

        # Store response data if required
        if endpoint.get("store_response"):
            response_data = response.json()
            row["slug"] = response_data["data"]["candidate"]["slug"]
    except Exception as e:
        current_time = datetime.datetime.now()
        print(f"[{current_time}] Error for endpoint {url} : {e}")
        print(e)


def process_account(row, endpoints):
    for endpoint in endpoints:
        # Skip endpoints that require 'slug' if it's missing
        if "slug" in endpoint["url"] and not row.get("slug"):
            print(f"Skipping endpoint {endpoint['url']} due to missing 'slug'")
            continue
        
        # Generate payload and hit the endpoint
        payload = endpoint["payload"](row)
        hit_endpoint(endpoint, payload, row.get("getToken"), row.get("apiToken"), row)
    current_time = datetime.datetime.now()
    print(f"[{current_time}] Completed processing for account {row.get('id')}.")


# Main function to read CSV and start processing accounts in parallel
def process_csv_parallel(csv_file_path, endpoints):
    try:
        # Read the CSV rows into memory
        with open(csv_file_path, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            accounts = list(reader)  # Load all accounts into memory

        # Use ThreadPoolExecutor to process accounts in parallel
        with ThreadPoolExecutor(max_workers=300) as executor:
            for account in accounts:
                time.sleep(1)
                executor.submit(process_account, account, endpoints)

    except FileNotFoundError:
        print(f"File not found: {csv_file_path}")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

# Call the main function
while True:
    process_csv_parallel(csv_file_path, endpoints)
    time.sleep(30)
