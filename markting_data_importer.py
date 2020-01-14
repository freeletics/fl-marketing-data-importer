from helper_functions import generate_dates, write_newline_json, ASA_COMBINED_DATA_PATH, ACCESS_TOKEN_FILE
from helper_functions import write_dict_to_json, FB_AD_IDS_COMBINED_DATA_PATH, FB_ADS_COMBINED_DATA_PATH, download_certs, download_access_token
from FacebookAPI import FacebookAPI
from AppleSearchAdsAPI import AppleSearchAdsAPI
import json

def run_asa():
    dates = generate_dates(lookback=7)
    # dates = generate_dates(start_date="2019-11-01",end_date="2019-12-31")
    # date = "2019-11-01"
    for date in dates:
        print("executing for date=", date)
        asa = AppleSearchAdsAPI()
        adgroups_data = asa.collect_adgroup_data(date)
        if len(adgroups_data) > 0:
            write_newline_json(adgroups_data, ASA_COMBINED_DATA_PATH.format(date=date))


def run_fb(access_token):
    account_ids = ["act_1071945382819295", "act_303967266891476", "act_2274347652847156", "act_585275221916929",
                   "act_268787333809205", "act_1071912799489220", "act_1057564360924064", "act_252744172283162",
                   "act_192416424968080", "act_1792336860780140", "act_1385327305066440", "act_1088998151114018",
                   "act_1088997634447403", "act_1554162461264249", "act_1614079798605848"]
    dates = generate_dates(lookback=7)
    # dates = generate_dates(start_date="2019-12-19", end_date="2019-12-28")
    # dates = ["2020-01-07"]
    # account_ids = ["act_268787333809205"]
    for date in dates:
        print("-" * 20)
        print("executing for date=", date)
        for account_id in account_ids:
            print("account_id=", account_id)
            fb = FacebookAPI(access_token)
            print("collecting ad ids")
            ad_ids = fb.collect_ad_ids(date=date, account_id=account_id)
            write_dict_to_json(ad_ids, FB_AD_IDS_COMBINED_DATA_PATH.format(date=date, account_id=account_id))

            if len(ad_ids) > 0:
                print("collecting ad level insights data")
                ads_data = fb.collect_ad_level_data(ad_ids, date)
                write_newline_json(ads_data["all_data_campaigns_info"], FB_ADS_COMBINED_DATA_PATH.format(date=date,
                                                                                                         account_id=account_id,
                                                                                                         data_type="campaigns"))
                write_newline_json(ads_data["all_data_spend"], FB_ADS_COMBINED_DATA_PATH.format(date=date,
                                                                                                account_id=account_id,
                                                                                                data_type="spend"))
                write_newline_json(ads_data["all_data_reach"], FB_ADS_COMBINED_DATA_PATH.format(date=date,
                                                                                                account_id=account_id,
                                                                                                data_type="reach"))
                write_newline_json(ads_data["all_data_impressions"], FB_ADS_COMBINED_DATA_PATH.format(date=date,
                                                                                                      account_id=account_id,
                                                                                                      data_type="impressions"))
                write_newline_json(ads_data["all_data_clicks"], FB_ADS_COMBINED_DATA_PATH.format(date=date,
                                                                                                 account_id=account_id,
                                                                                                 data_type="clicks"))


def main():
    print("download certs")
    download_certs()
    download_access_token()
    print("calling Apple")
    # Apple Srearch Ads API
    run_asa()
    print("Calling Facebook")
    with open(ACCESS_TOKEN_FILE) as fp:
        access_token = json.load(fp)["access_token"]
    run_fb(access_token)


if __name__ == "__main__":
    main()

print("Guru99")
