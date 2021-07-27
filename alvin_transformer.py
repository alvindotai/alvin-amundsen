# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0
import json
from typing import Any, Dict, Sequence

import requests
from databuilder.models.dashboard.dashboard_metadata import DashboardMetadata
from databuilder.models.dashboard.dashboard_table import DashboardTable
from databuilder.models.table_lineage import ColumnLineage, TableLineage
from databuilder.models.table_metadata import TableMetadata
from databuilder.transformer.base_transformer import Transformer
from pyhocon import ConfigTree

from sentinel_task import SentinelTask

API_ENDPOINT = "/api/v1/amundsen-lineage"
BATCH_SIZE = 10

AMUNDSEN_DASHBOARD_TABLE = "DashboardTable"
AMUNDSEN_TABLE_LINEAGE = "TableLineage"
AMUNDSEN_COLUMN_LINEAGE = "ColumnLineage"


def _map_alvin_object_to_amundsen_node(res) -> Any:
    if res.get("model", None) == AMUNDSEN_DASHBOARD_TABLE:
        return DashboardTable(
            dashboard_group_id=res.get("dashboardGroupId"),
            dashboard_id=res.get("dashboardId"),
            table_ids=res.get("tableIds"),
            product=res.get("product", ""),
            cluster="None"
        )
    elif res.get("model", None) == AMUNDSEN_TABLE_LINEAGE:
        return TableLineage(
            table_key=res.get("tableKey"),
            downstream_deps=res.get("downstreamDeps")
        )
    elif res.get("model", None) == AMUNDSEN_COLUMN_LINEAGE:
        return ColumnLineage(
            column_key=res.get("columnKey"),
            downstream_deps=res.get("downstreamDeps")
        )


class AlvinTransformer(Transformer):
    ALVIN_PLATFORM_NAME: str = "platform_id"
    ALVIN_PLATFORM_TYPE: str = "platform_type"
    ALVIN_API_KEY = 'api_key'
    ALVIN_INSTANCE_URL = "alvin_instance_url"
    TABLEAU_SITE_NAME = "tableau_site_name"

    def __init__(self) -> None:
        super().__init__()
        self.conf = None
        self.records = []

    # noinspection PyAttributeOutsideInit
    def init(self, conf: ConfigTree) -> None:
        self.conf = conf
        self.alvin_instance = self.conf.get(AlvinTransformer.ALVIN_INSTANCE_URL)
        self.alvin_api_key = self.conf.get(AlvinTransformer.ALVIN_API_KEY)
        self.dashboard_config = self.conf.get(AlvinTransformer.TABLEAU_SITE_NAME, None)
        self.alvin_platform_config = {
            "platform_id": self.conf.get(AlvinTransformer.ALVIN_PLATFORM_NAME),
            "platform_type": self.conf.get(AlvinTransformer.ALVIN_PLATFORM_TYPE)
        }

    def transform(self, record: Any) -> Any:
        if record != SentinelTask.SENTINEL_VALUE:
            self.records.append(record)
            yield record
        else:
            query_objects = []
            for rec in self.records:
                query_objects.extend(self._convert_amundsen_object_to_alvin_api_call(rec))

            batches = [query_objects[i:i + BATCH_SIZE] for i in range(0, len(query_objects), BATCH_SIZE)]

            for batch in batches:
                response = self._call_alvin_api(request_entities=batch)
                for res in response:
                    obj = _map_alvin_object_to_amundsen_node(res)
                    yield obj

    def _generate_query_object(self, entity_id, entity_type) -> Dict[str, str]:
        """

        :param entity_id: the Amundsen-formatted entity ID
        :param entity_type: the ALVIN entity type (TABLE, COLUMN, WORKBOOK, SHEET, etc).
                            Mapped automatically in transform()
        :return: tuple of URL and query params for the Alvin API request
        """
        d: Dict = {
            "entity_id": entity_id,
            "entity_type": entity_type
        }
        d.update(self.alvin_platform_config)
        if self.dashboard_config:
            d.update({"dashboard_site_name": self.dashboard_config})
        return d

    def _convert_amundsen_object_to_alvin_api_call(self, record: Any) -> Sequence[Dict[str, str]]:
        reqs = []
        if isinstance(record, TableMetadata):
            reqs.append(self._generate_query_object(record._get_table_key(), "TABLE"))
            for col in record.columns:
                reqs.append(self._generate_query_object(record._get_col_key(col), "COLUMN"))
        elif isinstance(record, DashboardMetadata):
            reqs.append(self._generate_query_object(record._get_dashboard_key(), "WORKBOOK"))
        return reqs

    def _call_alvin_api(self, request_entities):

        payload = {
            "entities": request_entities
        }
        headers = {
            'X-API-KEY': self.alvin_api_key
        }

        response = requests.request("GET", f"{self.alvin_instance}{API_ENDPOINT}",
                                    headers=headers, data=json.dumps(payload))

        data = json.loads(response.text)

        # A "not found" error will return { "detail": "Not Found" }, while a successful response will return a list
        return data if isinstance(data, list) else []  # error comes in a python dict, success comes with a list

    def get_scope(self) -> str:
        return "transformer.alvin_transformer"
