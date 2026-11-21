"""
Step 2c: store the MapReduce result in blob storage ("processed" container),
so the data is kept in blob storage after processing as well as before.
"""
from src.common import blob, config


def main():
    local = config.LOCAL_STAGING_DIR / "facts.tsv"
    blob.ensure_container(config.PROCESSED_CONTAINER)
    blob.upload_file(local, config.PROCESSED_CONTAINER, "mapreduce/facts.tsv")
    print(f"Stored as {config.PROCESSED_CONTAINER}/mapreduce/facts.tsv")


if __name__ == "__main__":
    main()
