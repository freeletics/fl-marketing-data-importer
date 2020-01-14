import boto3
import json
from datetime import timedelta, datetime

# Paths
S3_BUCKET = "freeletics-dea"
CERT_FILE = "freeletics_data_analytics.pem"
KEY_FILE = "freeletics_data_analytics.key"
CERT_FILE_PATH = "ua_reporting/api/credentials/apple_search_ads/freeletics_data_analytics.pem"
KEY_FILE_PATH = "ua_reporting/api/credentials/apple_search_ads/freeletics_data_analytics.key"
ASA_CAMPAIGNS_RAW_DATA_PATH = "ua_reporting/data/asa/raw/{date}/campaigns_asa_api_data.json"
ASA_ADGROUPS_RAW_DATA_PATH = "ua_reporting/data/asa/raw/{date}/adgroup_{campaign_id}_asa_api_data.json"
ASA_COMBINED_DATA_PATH = "ua_reporting/data/asa/combined/{date}.json"

ACCESS_TOKEN_FILE = "access_token.json"
ACCESS_TOKEN_PATH = "ua_reporting/api/credentials/facebook/access_token.json"
FB_AD_IDS_RAW_DATA_PATH = "ua_reporting/data/fb/raw/{date}/ad_ids_fb_api_data_{uuid}.json"
FB_AD_IDS_COMBINED_DATA_PATH = "ua_reporting/data/fb/combined/ad_ids/{date}/ad_ids_{account_id}_{date}.json"
FB_ADS_RAW_DATA_PATH = "ua_reporting/data/fb/raw/{date}/ads_insight_fb_api_data_{uuid}.json"
FB_ADS_COMBINED_DATA_PATH = "ua_reporting/data/fb/combined/ads/{date}/fbads_{account_id}_{date}_{data_type}.json"
FB_ERRORS_DATA_PATH = "ua_reporting/data/fb/errors/fb_{uuid}.json"


def generate_dates(**kwargs):
    """
      param lookback: the number of days to lookback from today (int)
      param start_date: the start date for the date generation "YYYY-MM-DD" (string)
      param end_date: the end date for the date generation "YYYY-MM-DD" (string)
    """
    lookback = kwargs.get("lookback")
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    dates_list = []
    if lookback:
        for i in range(1, lookback + 1):
            dates_list.append(str(datetime.today() - timedelta(days=i))[:10])
    elif start_date and end_date:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        if start > end:
            print("start date should be older than end date")
        else:
            i = 0
            while True:
                to_append = start + timedelta(days=i)
                dates_list.append(str(to_append)[:10])
                i += 1
                if to_append == end:
                    break
    else:
        print("one of the parameters: lookback, start_date, end_date should be provided")
    return dates_list


def ndjsondump(objects):
    return '\n'.join(json.dumps(obj) for obj in objects)


def write_newline_json(data, file_name):
    '''
    :param data: dict data
    :param file_name: file_name with absoulte path and without bucket name
    '''
    s3 = boto3.resource("s3")
    binary_data = ndjsondump(data)
    object = s3.Object(S3_BUCKET, file_name)
    object.put(Body=binary_data)


def write_dict_to_json(data, file_name):
    '''
    :param data: dict data
    :param file_name: file_name with absoulte path and without bucket name
    '''
    s3 = boto3.resource("s3")
    binary_data = json.dumps(data)
    object = s3.Object(S3_BUCKET, file_name)
    object.put(Body=binary_data)


def backoff_hdlr(details):
    print(details)
    print("Backing off {wait:0.1f} seconds after {tries} tries. "
          "Calling function {target} with args {args} and kwargs "
          "{kwargs}".format(**details))


def download_certs():
    s3 = boto3.client('s3')
    s3.download_file(S3_BUCKET, CERT_FILE_PATH, CERT_FILE)
    s3.download_file(S3_BUCKET, KEY_FILE_PATH, KEY_FILE)


def download_access_token():
    s3 = boto3.client('s3')
    s3.download_file(S3_BUCKET, ACCESS_TOKEN_PATH, ACCESS_TOKEN_FILE)


class FBNoDataReturnedException(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        # Now for your custom code...
        #self.errors = errors


def check_for_data_available(request_data, response_data, batch_size):
    # account insights endpoint json has data directly in the dict as a key. For Ads batch insight result, its a list
    if isinstance(response_data, dict):
        if response_data.get("data", "key_not_found") == "key_not_found":
            write_dict_to_json(response_data, FB_ERRORS_DATA_PATH.format(uuid=str(uuid.uuid1())))
            raise FBNoDataReturnedException("No data parameter available. Mostly due to too much data asked")
        else:
            return response_data
    else:
        # filtering out successful responses from the failed ones in the batch request
        filtered_response_data = []
        i = 0
        for item in response_data:
            body = item["body"]
            body = json.loads(body)
            if body.get("data", "key_not_found") == "key_not_found":
                write_dict_to_json(response_data, (FB_ERRORS_DATA_PATH[:-5] + "_response.json").format(uuid=str(uuid.uuid1())))
                write_dict_to_json(request_data, (FB_ERRORS_DATA_PATH[:-5] + "_request.json").format(uuid=str(uuid.uuid1())))
                ad_links_failed_queue.put(request_data["batch"][i]["relative_url"])

                # Raising the error to retry and fail only if the batch size is 1
                # That happens when the already failed requests are retried
                if batch_size == 1:
                    write_dict_to_json(request_data, (FB_ERRORS_DATA_PATH[:-5] + "_failed_request.json").format(uuid=str(uuid.uuid1())))
                    raise FBNoDataReturnedException("No data parameter available. Mostly due to too much data asked")
            else:
                filtered_response_data.append(item)
            i += 1
        return filtered_response_data
