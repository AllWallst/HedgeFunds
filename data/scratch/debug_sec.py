
import requests
import json

USER_AGENT = "HedgeFlow Research App contact@hedgeflow.app"
SEC_BASE_URL = "https://data.sec.gov"
SEC_EDGAR_URL = "https://www.sec.gov"

def test_sec_access():
    cik = "0001067983" # Berkshire
    padded_cik = cik.zfill(10)
    
    # 1. Test submissions API (data.sec.gov)
    url_sub = f"{SEC_BASE_URL}/submissions/CIK{padded_cik}.json"
    print(f"Testing Submissions API: {url_sub}")
    try:
        resp = requests.get(url_sub, headers={"User-Agent": USER_AGENT})
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            print("Successfully fetched submissions.")
            acc_no = data['filings']['recent']['accessionNumber'][0]
            print(f"Latest Accession: {acc_no}")
            
            # 2. Test Archive Index API (www.sec.gov)
            acc_no_dashes = acc_no.replace("-", "")
            
            # CURRENT CODE USES SEC_BASE_URL (data.sec.gov) - THIS MIGHT FAIL
            url_idx_bad = f"{SEC_BASE_URL}/Archives/edgar/data/{padded_cik}/{acc_no_dashes}/index.json"
            print(f"Testing Archive Index (BAD BASE): {url_idx_bad}")
            resp_bad = requests.get(url_idx_bad, headers={"User-Agent": USER_AGENT})
            print(f"Status (BAD): {resp_bad.status_code}")
            
            # CORRECT BASE SHOULD BE SEC_EDGAR_URL (www.sec.gov)
            url_idx_good = f"{SEC_EDGAR_URL}/Archives/edgar/data/{padded_cik}/{acc_no_dashes}/index.json"
            print(f"Testing Archive Index (GOOD BASE): {url_idx_good}")
            resp_good = requests.get(url_idx_good, headers={"User-Agent": USER_AGENT})
            print(f"Status (GOOD): {resp_good.status_code}")
            
        else:
            print(f"Failed to fetch submissions: {resp.text[:200]}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_sec_access()
