import time
import json


def formatter(did, profiles, broadcasted_vars={}):
    keywords = [{"Name": y, "TtlInMinutes": 64800} for y in profiles]
    return json.dumps({"DAID": did, "Data": keywords}) + ","


def fname(counter):
    return "chunk_%s_%04d_%s" % (
        time.strftime("%Y%m%d"),
        counter,
        time.strftime("%H%M%S"),
    )


def fname_adid(counter):
    return "adid_chunk_%s_%04d_%s" % (
        time.strftime("%Y%m%d"),
        counter,
        time.strftime("%H%M%S"),
    )


def fname_idfa(counter):
    return "idfa_chunk_%s_%04d_%s" % (
        time.strftime("%Y%m%d"),
        counter,
        time.strftime("%H%M%S"),
    )


def get_config():
    output_datasets_config = {}
    output_datasets_config["final_a"] = {
        "name": "dataset_a",
        "filters": ["idfa"],
        "formatter": formatter,
        "name_provider": fname_idfa,
    }

    output_datasets_config["final_b"] = {
        "name": "dataset_b",
        "filters": ["adid"],
        "formatter": formatter,
        "name_provider": fname_adid,
    }

    output_datasets_config["final_c"] = {
        "name": "dataset_c",
        "filters": ["idfa", "adid"],
        "formatter": formatter,
        "name_provider": fname,
        "writer_config": {
            "format": "parquet",
            "compression": "snappy",
        },
    }

    return output_datasets_config


def main():
    config = get_config()

    serialized = json.dumps(config)
    print(serialized)
    # save this in a file

    config = json.loads(serialized)


if __name__ == "__main__":
    main()
