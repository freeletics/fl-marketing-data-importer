from helper_functions import CERT_FILE, KEY_FILE, write_dict_to_json, ASA_ADGROUPS_RAW_DATA_PATH,\
    ASA_CAMPAIGNS_RAW_DATA_PATH, backoff_hdlr
import backoff
import requests


class AppleSearchAdsAPI(object):
    def __init__(self):
        self.campaigns_url = "https://api.searchads.apple.com/api/v1/reports/campaigns"
        self.adgroups_url = "https://api.searchads.apple.com/api/v1/reports/campaigns/{campaign_id}/adgroups"
        self.cert = (CERT_FILE, KEY_FILE)

    def _generate_payload(self, date):
        payload = {"startTime": date,
                   "endTime": date,
                   "selector": {
                       "orderBy": [
                           {
                               "field": "localSpend",
                               "sortOrder": "DESCENDING"
                           }
                       ],
                       "pagination": {
                           "offset": 0,
                           "limit": 1000
                       }
                   },
                   "groupBy": [
                       "countryCode"
                   ],
                   "returnRowTotals": "false",
                   "returnGrandTotals": "false",
                   "returnRecordsWithNoMetrics": "false",
                   "granularity": "DAILY"
                   }
        return payload

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.HTTPError, requests.exceptions.Timeout,
                           requests.exceptions.ConnectionError),
                          max_time=60 * 10,  # 10mins
                          on_backoff=backoff_hdlr,
                          jitter=None)
    def _make_request(self, url, date):
        print("calling api", url)
        headers = {"Authorization": "orgId=126640",
                   "Content-Type": "application/json"
                   }
        payload = self._generate_payload(date)
        response = requests.post(url, cert=self.cert, json=payload, headers=headers)
        print(response.status_code)
        response.raise_for_status()
        return response

    def _make_adgroups_requests(self, date, campaigns_info):
        campaign_ids = campaigns_info["campaign_ids"]
        campaign_names = campaigns_info["campaign_names"]

        all_data = []
        for campaign_id in campaign_ids:
            formatted_url = self.adgroups_url.format(campaign_id=campaign_id)
            response = self._make_request(formatted_url, date)
            adgroups_data = response.json()
            write_dict_to_json(adgroups_data, ASA_ADGROUPS_RAW_DATA_PATH.format(campaign_id=campaign_id,
                                                                                date=date))

            adgroups_data = adgroups_data.get("data", {}).get("reportingDataResponse").get("row", [])

            # adding campaign info
            for item in adgroups_data:
                item["metadata"]["campaignId"] = campaign_id
                item["metadata"]["campaignName"] = campaign_names[campaign_id]

            all_data = all_data + adgroups_data
        return all_data

    def collect_campaigns_data(self, date):
        response = self._make_request(self.campaigns_url, date)
        campaigns_data = response.json()
        write_dict_to_json(campaigns_data, ASA_CAMPAIGNS_RAW_DATA_PATH.format(date=date))
        campaigns_data = campaigns_data.get("data", {}).get("reportingDataResponse").get("row", [])
        return campaigns_data

    def find_campaign_ids(self, campaigns_data):
        campaign_ids = []
        campaign_names = {}

        # only if campaigns data exists, the mapping is run
        if len(campaigns_data) > 0:
            for campaign_data in campaigns_data:
                campaign_id = campaign_data["metadata"]["campaignId"]
                campaign_ids.append(campaign_id)
                if campaign_id not in campaign_names:
                    campaign_names[campaign_id] = campaign_data["metadata"]["campaignName"]

        campaign_ids = list(set(campaign_ids))
        return {
            "campaign_ids": campaign_ids,
            "campaign_names": campaign_names
        }

    def collect_adgroup_data(self, date, campaign_ids=[]):
        if len(campaign_ids) > 0:
            # if campaign_ids are provided, then runs only the adgroups api request
            adgroups_data = self._make_adgroups_requests(date, campaign_ids)
        else:
            # if campaign_ids are not provided, first runs the campaigns api to get the campaign ids
            campaigns_data = self.collect_campaigns_data(date)
            if len(campaigns_data) > 0:
                campaigns_info = self.find_campaign_ids(campaigns_data)
                adgroups_data = self._make_adgroups_requests(date, campaigns_info)
            else:
                adgroups_data = []

        return adgroups_data
