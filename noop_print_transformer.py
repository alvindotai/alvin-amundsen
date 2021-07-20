# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0
from typing import Any

from databuilder.models.dashboard.dashboard_metadata import DashboardMetadata
from databuilder.models.table_metadata import TableMetadata
from databuilder.transformer.base_transformer import Transformer
from pyhocon import ConfigTree


class NoopPrintTransformer(Transformer):
    def init(self, conf: ConfigTree) -> None:
        pass

    def transform(self, record: Any) -> Any:
        if isinstance(record, TableMetadata):
            print("This is a table metadata object: ", record)
        elif isinstance(record, DashboardMetadata):
            print("This is a dashboard metadata object: ", record)
        else:
            print("This is a something else: ", record)

        yield record

    def get_scope(self) -> str:
        return "transformer.noop_print_transformer"
