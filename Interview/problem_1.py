# Import necessary libraries
import time
import json


def formatter(did, profiles, broadcasted_vars={}):
    """
    Formats data points for output, likely to some system.

    Args:
        did: Device ID.
        profiles: List of data points with specific attributes.
        broadcasted_vars: Optional additional variables (default empty).

    Returns:
        A JSON string representing the formatted data, including device ID and keywords.
    """
    keywords = [{"Name": y, "TtlInMinutes": 64800} for y in profiles]
    return json.dumps({"DAID": did, "Data": keywords}) + ","


def fname(counter):
    """
    Generates a consistent filename with timestamps and a counter.

    Args:
        counter: An integer counter for differentiating batches.

    Returns:
        A string representing the filename in format YYYYMMDD_XXXX_HHMMSS.
    """
    return "chunk_%s_%04d_%s" % (
        time.strftime("%Y%m%d"),
        counter,
        time.strftime("%H%M%S"),
    )


def fname_adid(counter):
    """
    Similar to `fname` but adds "adid_" to the filename for differentiation.
    """
    return "adid_chunk_%s_%04d_%s" % (
        time.strftime("%Y%m%d"),
        counter,
        time.strftime("%H%M%S"),
    )


def fname_idfa(counter):
    """
    Similar to `fname` but adds "idfa_" to the filename for differentiation.
    """
    return "idfa_chunk_%s_%04d_%s" % (
        time.strftime("%Y%m%d"),
        counter,
        time.strftime("%H%M%S"),
    )


def get_config():
    """
    Defines configurations for different output datasets.

    Returns:
        A dictionary with three entries ("final_a", "final_b", "final_c") containing configurations for each dataset.
    """
    output_datasets_config = {}
    output_datasets_config["final_a"] = {
        "name": "dataset_a",
        "filters": ["idfa"],  # Apply filter for "idfa" data during writing.
        "formatter": formatter,  # Use the `formatter` function to prepare data.
        "name_provider": fname_idfa,  # Use `fname_idfa` to generate filenames.
    }

    output_datasets_config["final_b"] = {
        "name": "dataset_b",
        "filters": ["adid"],  # Apply filter for "adid" data during writing.
        "formatter": formatter,  # Use the `formatter` function to prepare data.
        "name_provider": fname_adid,  # Use `fname_adid` to generate filenames.
    }

    output_datasets_config["final_c"] = {
        "name": "dataset_c",
        "filters": ["idfa", "adid"],  # Apply filters for both "idfa" and "adid" data.
        "formatter": formatter,  # Use the `formatter` function to prepare data.
        "name_provider": fname,  # Use `fname` for general filenames.
        "writer_config": {  # Additional configuration for writing "final_c" data.
            "format": "parquet",  # Specify output format as parquet.
            "compression": "snappy",  # Use snappy compression for the parquet file.
        },
    }

    return output_datasets_config


def write_dataset(df, dataset_name, writer_config):
    """
    Writes the data to a file based on configurations.

    Args:
        df: The dataframe containing the data to write.
        dataset_name: The name of the dataset to write to.
        writer_config: Configuration for formatting and writing the data.

    Returns:
        None
    """
    writer_config = writer_config.pop("format")  # Extract "format" from config.

    # Apply any filters based on configuration
    if "filters" in writer_config:
        for data_type in writer_config["filters"]:
            df = df[df[data_type].notna()]  # Filter by non-null values for specific data types.

    # Format the data using the provided function
    formatted_data = [formatter(did, row) for _, row in df.iterrows()]  # Apply formatter to each row.

    # Write the data to the file based on format specification
    if writer_config == "parquet":
        df_writer = df.to_parquet(
            dataset_name, compression=writer_config["compression"]
        )  # Write parquet file with compression.
    elif writer_config == "text":
        with open(dataset_name, "w") as f:
            f.writelines(formatted_data)  # Write each formatted line to the text file.
    else:
        raise NotImplementedError(f"Unsupported format: {writer_config}")  # Handle other formats if needed.

    return None  # Currently not returning anything, might change depending on implementation.




def main():
    default_writer_config = {
        "format": "text",
        "header": "false",
        "compression": "org.apache.hadoop.io.compress.GzipCodec",
    }

    output_datasets = get_config()

    for output_dataset_name, output_dataset_value in output_datasets.items():
        if "writer_config" in output_dataset_value:
            writer_config = output_dataset_value["writer_config"]
        else:
            writer_config = default_writer_config

        df = None
        write_dataset(df, output_dataset_name, writer_config)


if __name__ == "__main__":
    main()
