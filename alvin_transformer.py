import json
import logging
from typing import Any

import requests
from databuilder.models.dashboard.dashboard_metadata import DashboardMetadata
from databuilder.models.dashboard.dashboard_table import DashboardTable
from databuilder.models.table_lineage import TableLineage, ColumnLineage
from databuilder.models.table_metadata import TableMetadata
from databuilder.transformer.base_transformer import Transformer

from pyhocon import ConfigTree

logging.basicConfig(
    filename="./error.log",
    level=logging.INFO
)


class AlvinTransformer(Transformer):
    ALVIN_PLATFORM_NAME: str = "platform_id"
    ALVIN_PLATFORM_TYPE: str = "platform_type"
    ALVIN_API_KEY = 'api_key'
    ALVIN_INSTANCE_URL = "alvin_instance_url"
    TABLEAU_SITE_NAME = "tableau_site_name"

    def __init__(self):
        self.conf = None

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf

    def transform(self, record: Any):
        yield record

        # preparing for multiple urls to request lineage from.
        reqs = []

        if isinstance(record, TableMetadata):
            reqs.append(self._generate_query_params(record._get_table_key(), "TABLE"))
            for col in record.columns:
                reqs.append(self._generate_query_params(record._get_col_key(col), "COLUMN"))

        elif isinstance(record, DashboardMetadata):
            reqs.append(self._generate_query_params(record._get_dashboard_key(), "WORKBOOK"))

        responses = []

        for data in reqs:
            responses.extend(self._get_alvin_lineage(*data))

        # Want to add more models? Just add another if statement here
        for res in responses:
            if res.get("model", None) == "DashboardTable":
                yield DashboardTable(
                    dashboard_group_id=res.get("dashboardGroupId"),
                    dashboard_id=res.get("dashboardId"),
                    table_ids=res.get("tableIds"),
                    product=res.get("product", ""),
                    cluster="None"
                )
            elif res.get("model", None) == "TableLineage":
                yield TableLineage(
                    table_key=res.get("tableKey"),
                    downstream_deps=res.get("downstreamDeps")
                )
            elif res.get("model", None) == "ColumnLineage":
                yield ColumnLineage(
                    column_key=res.get("columnKey"),
                    downstream_deps=res.get("downstreamDeps")
                )

    def _generate_query_params(self, entity_id, entity_type):
        """

        :param entity_id: the Amundsen-formatted entity ID
        :param entity_type: the ALVIN entity type (TABLE, COLUMN, WORKBOOK, SHEET, etc).
                            Mapped automatically in transform()
        :return: tuple of URL and query params for the Alvin API request
        """
        return f"{self.conf.get(AlvinTransformer.ALVIN_INSTANCE_URL)}/api/v1/amundsen_table_lineage", {
            "platformId": self.conf.get(AlvinTransformer.ALVIN_PLATFORM_NAME),
            "entityId": entity_id,
            "entityType": entity_type,
            "dashboardSiteName": self.conf.get(AlvinTransformer.TABLEAU_SITE_NAME, ''),
            "platformType": self.conf.get(AlvinTransformer.ALVIN_PLATFORM_TYPE)
        }

    def _get_alvin_lineage(self, url, params):
        payload = {}
        headers = {
            'Authorization': self.conf.get(AlvinTransformer.ALVIN_API_KEY)
        }

        response = requests.request("GET", url, params=params, headers=headers, data=payload)

        data = json.loads(response.text)

        # A "not found" error will return { "detail": "Not Found" }, while a successful response will return a list
        return data if isinstance(data, list) else []  # error comes in a python dict, success comes with a list

    def get_scope(self) -> str:
        return 'transformer.alvin_transformer'
