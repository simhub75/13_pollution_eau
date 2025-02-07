import logging

import duckdb
from datetime import datetime

from ._config_edc import get_edc_config
from ._common import DUCKDB_FILE
from .build_database import (
    extract_dataset_datetime, process_edc_datasets, check_table_existence,

)

logger = logging.getLogger(__name__)
edc_config = get_edc_config()


def update_edc_datasets():
    """
    Check EDC dataset into database are up to date according to www.data.gouv.fr
    Compare the dataset datetime between database and www.data.gouv.fr and
    update if necesssary;
    """
    available_years = edc_config["source"]["available_years"]
    update_years = []

    logger.info("Check EDC dataset in the database are up to date according to www.data.gouv.fr")

    for year in available_years:
        data_url = (
            edc_config["source"]["base_url"]
            + edc_config["source"]["yearly_files_infos"][year]["id"]
        )
        logger.info(f"   Check EDC dataset datetime for {year}")

        conn = duckdb.connect(DUCKDB_FILE)

        files = edc_config["files"]

        for file_info in files.values():
            if check_table_existence(conn=conn, table_name=f"{file_info['table_name']}"):
                query = f"""
                    SELECT de_dataset_datetime
                    FROM {file_info['table_name']}
                    WHERE de_partition = CAST(? as INTEGER)
                    ;
                """
                conn.execute(query, (year,))
                current_dataset_datetime = conn.fetchone()[0]
                logger.info(
                    f"   Database - current EDC dataset datetime for {year}: "
                    f"{current_dataset_datetime}")

                format_str = "%Y%m%d-%H%M%S"
                last_data_gouv_dataset_datetime = extract_dataset_datetime(data_url)
                logger.info(
                    f"   Datagouv - current EDC dataset datetime for {year}: "
                    f"{last_data_gouv_dataset_datetime}")

                last_data_gouv_dataset_datetime = datetime.strptime(
                    last_data_gouv_dataset_datetime, format_str)
                current_dataset_datetime = datetime.strptime(current_dataset_datetime, format_str)

                if last_data_gouv_dataset_datetime > current_dataset_datetime:
                    update_years.append(year)

            else:
                # EDC table will be created with process_edc_datasets
                update_years.append(year)
            # Only one check of a file is needed because the update is done for the whole
            break

        if update_years:
            logger.info(f"   EDC dataset update is necessary for {update_years}")
            process_edc_datasets(refresh_type="custom", custom_years=[year])
        else:
            logger.info("   All EDC dataset are already up to date")


def execute():
    update_edc_datasets()
