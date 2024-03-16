from typing import Dict, Any, Tuple, Callable
import time


timestamp = str(int(time.mktime(time.localtime())))


def format_output(did, profiles, broadcasted_vars={}):
    return did + "\t" + ",".join(profiles)


def fname_idfa(counter, **kwargs):
    bundle_id = kwargs.get("bundle_id", "")
    return "ftp_dpm_90988_{}_{}.sync.{}.gz".format(
        bundle_id,
        timestamp,
        counter,
    )


def fname_adid(counter, **kwargs):
    bundle_id = kwargs.get("bundle_id", "")
    return "ftp_dpm_90987_{}_{}.sync.{}.gz".format(
        bundle_id,
        timestamp,
        counter,
    )


def get_config():
    output_datasets_config = {}
    output_datasets_config["idfa_final"] = {
        "name": "dataset_a.tsv.gz",
        "filters": ["idfa"],
        "formatter": format_output,
        "name_provider": fname_idfa,
    }

    output_datasets_config["adid_final"] = {
        "name": "dataset_b.tsv.gz",
        "filters": ["adid"],
        "formatter": format_output,
        "name_provider": fname_adid,
    }

    return output_datasets_config


def generate_new_output_datasets(
    output_datasets: Dict[str, Any]
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Callable[[str, str], bool]]]:
    filter_value_seperator = ","
    new_output_datasets = {}
    partner_extra_details = [
        {
            "key": "6567576",
            "value": "pf_a18,pf_a25,pf_a35,pf_a45,pf_a55,pf_a65,pf_m,pf_f,pf_975,pf_976,pf_977,pf_978,pf_979,pf_980,pf_981",
            "type": "bundle_id",
        },
        {
            "key": "989878787",
            "value": "prg_028,prg_029,prg_030,prg_031",
            "type": "bundle_id",
        },
        {
            "key": "43565767",
            "value": "pf_150,pf_250,pf_251,pf_me,pf_cfe,pf_fd,pf_food,pf_s_gc,pf_s_lx,pf_lt,prg_139,pf_mcy,prg_150,prg_144,pf_aapl,pf_msft,prg_064,prg_063,prg_059,pf_125,pf_124,pf_045,pf_046",
            "type": "bundle_id",
        },
        {
            "key": "3243567",
            "value": "pforcx_071,pforcx_061,pforcx_090,pforcx_091,pforcx_089,pforcx_107,pforcx_024,pforcx_025,pforcx_085,pforcx_026,pforcx_027,pforcx_028,pforcx_029,pforcx_033,pforcx_030,pforcx_031,pforcx_032,pforcx_092,pforcx_093,pforcx_011,pforcx_012,pforcx_013,pforcx_086,pforcx_014,pforcx_010,pforcx_015,pforcx_017,pforcx_018,pforcx_023,pforcx_019,pforcx_020,pforcx_021,pforcx_022,pforcx_016,pforcx_072,pforcx_087,pforcx_064,pforcx_088,pforcx_007,pforcx_067,pforcx_068,pforcx_075,pforcx_001,pforcx_003,pforcx_004,pforcx_097,pforcx_109,pforcx_096,pforcx_098,pforcx_082,pforcx_083,pforcx_106,pforcx_063,pforcx_095,pforcx_081,pforcx_062,pforcx_101,pforcx_099,pforcx_102,pforcx_103,pforcx_104,pforcx_105,pforcx_108,pforcx_070,pforcx_112,pforcx_005,pforcx_006,pforcx_113,pforcx_094,pforcx_078,pforcx_079",
            "type": "bundle_id",
        },
    ]
    filters_definitions = {}
    extra_details_key_value = {}
    for details_item in partner_extra_details:
        filter_key = details_item["key"].strip()
        filter_value = [segment.strip() for segment in details_item["value"].split(filter_value_seperator)]
        filter_type = details_item["type"].strip()
        extra_details_key_value[filter_key] = filter_value

        filter_function = lambda did, profile: profile in filter_value
        filters_definitions[filter_key] = filter_function

        for output_dataset_key, output_dataset in output_datasets.items():
            new_output_dataset = dict(output_dataset)
            new_output_dataset["filters"] = new_output_dataset["filters"] + [filter_key]
            new_output_dataset["extra_details"] = {filter_type: filter_key}
            new_output_dataset_key = "{}_{}".format(filter_key, output_dataset_key)
            new_output_datasets[new_output_dataset_key] = new_output_dataset

    return new_output_datasets, filters_definitions


if __name__ == "__main__":
    config = get_config()
    result, defintions = generate_new_output_datasets(config)
    for k, v in result.items():
        print(k, " => ", v["filters"])

    print(defintions)
    print(defintions["6567576"]("123", "pf_a55"))
    print(defintions["989878787"]("123", "prg_028"))
    print(defintions["43565767"]("123", "pf_251"))
    print(defintions["3243567"]("123", "pforcx_091"))
