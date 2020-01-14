import backoff
import requests
import uuid
import json
import urllib
import queue
from helper_functions import FBNoDataReturnedException, check_for_data_available, write_dict_to_json,\
    FB_AD_IDS_RAW_DATA_PATH, FB_ADS_RAW_DATA_PATH, backoff_hdlr

# Global queues
ad_links_queue = queue.Queue()
ad_links_failed_queue = queue.Queue()

class FacebookAPI(object):
    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url = "https://graph.facebook.com/"
        self.ad_insight_endpoint = "v5.0/{ad_id}/insights"
        self.account_insight_endpoint = "v5.0/{account_id}/insights"

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.HTTPError, requests.exceptions.Timeout,
                           requests.exceptions.ConnectionError, FBNoDataReturnedException),
                          max_time=60 * 10,  # 10mins
                          on_backoff=backoff_hdlr,
                          jitter=None)
    def _send_call(self, method, url, batch_size, data={}):
        if method == "GET":
            response = requests.get(url)
        elif method == "POST":
            response = requests.post(url, json=data)

        # checking for status code
        response.raise_for_status()
        response_data = response.json()
        # checking if all data is returned
        response_data = check_for_data_available(data, response_data, batch_size)
        return response_data

    def _make_request(self, method, endpoint, batch_size, input_queue):
        if method == "GET":
            response_data = self._send_call(method, endpoint, batch_size)
        elif method == "POST":
            data = self._make_batch(input_queue=input_queue, batch_limit=batch_size)
            response_data = self._send_call(method, endpoint, batch_size, data)
        return response_data

    def _paginating(self, endpoint, date):
        next_url = None
        all_data = []

        # paginating
        while True:
            if next_url:
                response_data = self._make_request("GET", next_url, batch_size=None, input_queue=None)
            else:
                response_data = self._make_request("GET", endpoint, batch_size=None, input_queue=None)
            write_dict_to_json(response_data, FB_AD_IDS_RAW_DATA_PATH.format(date=date, uuid=str(uuid.uuid1())))

            next_url = response_data.get("paging", {}).get("next")
            all_data = all_data + response_data.get("data", [])
            if not next_url:
                break
        return all_data

    def _paginating_batch(self, input_queue, date, batch_size):
        all_data_campaigns_info = []
        all_data_spend = []
        all_data_reach = []
        all_data_impressions = []
        all_data_clicks = []

        # paginating
        while not input_queue.empty():
            response_data = self._make_request("POST", self.base_url, batch_size, input_queue)
            # writing received data to a json file
            write_dict_to_json(response_data, FB_ADS_RAW_DATA_PATH.format(date=date, uuid=str(uuid.uuid1())))

            # de-serializing and flattening batch files
            deserialised_body = [json.loads(item["body"])["data"] for item in response_data]
            self._segregate_data(deserialised_body, all_data_campaigns_info, all_data_spend, all_data_reach,
                                 all_data_impressions, all_data_clicks)

            self._fill_queue(input_queue, response_data)

        # this is the final result. the failed data of ads when retried append to the same data
        return {
            "all_data_campaigns_info": all_data_campaigns_info,
            "all_data_spend": all_data_spend,
            "all_data_reach": all_data_reach,
            "all_data_impressions": all_data_impressions,
            "all_data_clicks": all_data_clicks
        }

    def _segregate_data(self, deserialised_body, all_data_campaigns_info, all_data_spend, all_data_reach,
                        all_data_impressions, all_data_clicks):
        for batch_item in deserialised_body:
            for item in batch_item:
                if "campaign_name" in item.keys():
                    all_data_campaigns_info.append(item)
                if "spend" in item.keys():
                    all_data_spend.append(item)
                if "reach" in item.keys():
                    all_data_reach.append(item)
                if "impressions" in item.keys():
                    all_data_impressions.append(item)
                if "clicks" in item.keys():
                    all_data_clicks.append(item)

    def _make_batch(self, input_queue, batch_limit):
        batch_data = []
        i = 0
        while not input_queue.empty() and i < batch_limit:
            relative_url = input_queue.get()
            data = {"method": "GET",
                    "relative_url": relative_url
                    }
            batch_data.append(data)
            i += 1

        return {
            "access_token": self.access_token,
            "batch": batch_data
        }

    def _build_ad_request_urls(self, input_queue, ad_ids, date):
        for ad_id in ad_ids:
            endpoint = self.ad_insight_endpoint.format(ad_id=ad_id)

            # campaign info
            relative_url = "{endpoint}?fields=account_name,account_id,campaign_name," \
                           "campaign_id,adset_name,adset_id,ad_name,ad_id&" \
                           "level=ad&" \
                           "time_range[since]={date}&" \
                           "time_range[until]={date}&" \
                           "breakdowns=country&" \
                           "limit=1000".format(endpoint=endpoint, date=date)
            input_queue.put(relative_url)

            # spend
            relative_url = "{endpoint}?fields=ad_id,spend&" \
                           "level=ad&" \
                           "time_range[since]={date}&" \
                           "time_range[until]={date}&" \
                           "breakdowns=country&" \
                           "limit=1000".format(endpoint=endpoint, date=date)
            input_queue.put(relative_url)

            # reach
            relative_url = "{endpoint}?fields=ad_id,reach&" \
                           "level=ad&" \
                           "time_range[since]={date}&" \
                           "time_range[until]={date}&" \
                           "breakdowns=country&" \
                           "limit=1000".format(endpoint=endpoint, date=date)
            input_queue.put(relative_url)

            # impressions
            relative_url = "{endpoint}?fields=ad_id,impressions&" \
                           "level=ad&" \
                           "time_range[since]={date}&" \
                           "time_range[until]={date}&" \
                           "breakdowns=country&" \
                           "limit=1000".format(endpoint=endpoint, date=date)
            input_queue.put(relative_url)

            # clicks
            relative_url = "{endpoint}?fields=ad_id,clicks,inline_link_clicks&" \
                           "level=ad&" \
                           "time_range[since]={date}&" \
                           "time_range[until]={date}&" \
                           "breakdowns=country&" \
                           "limit=1000".format(endpoint=endpoint, date=date)
            input_queue.put(relative_url)

    def _fill_queue(self, input_queue, data):
        next_links = [json.loads(item["body"])["paging"]["next"] for item in data if
                      json.loads(item["body"])["paging"].get("next")]
        # removing the base url part from the next links
        next_links = list(map(lambda x: x[26:], next_links))
        for next_link in next_links:
            input_queue.put(next_link)

    def collect_ad_ids(self, date, account_id):
        data = {
            "access_token": self.access_token,
            "level": "ad",
            # "filtering":    '[{field:"ad.impressions",operator:"GREATER_THAN",value:0}]',
            "time_range": '{{since:"{date}",until:"{date}"}}'.format(date=date)
        }
        encoded_data = urllib.parse.urlencode(data)
        endpoint = self.base_url + self.account_insight_endpoint.format(account_id=account_id) + "?" + encoded_data
        ads_data = self._paginating(endpoint=endpoint, date=date)
        ad_ids = [ad["ad_id"] if ad.get("ad_id") else None for ad in ads_data]
        return ad_ids

    def collect_ad_level_data(self, ad_ids, date):
        # thread safety not considered when clearing the queue
        ad_links_queue.queue.clear()
        ad_links_failed_queue.queue.clear()
        # First doing a batch size of 20
        self._build_ad_request_urls(ad_links_queue, ad_ids, date)
        ads_data = self._paginating_batch(ad_links_queue, date, batch_size=20)

        if ad_links_failed_queue.qsize() > 0:
            # Doing a batch size of 1 for failed requests
            print("length of failed queue=", ad_links_failed_queue.qsize())
            ads_data_failed = self._paginating_batch(ad_links_failed_queue, date, batch_size=1)

            # combining both results
            for key in ads_data.keys():
                ads_data[key] = ads_data[key] + ads_data_failed[key]
        return ads_data
