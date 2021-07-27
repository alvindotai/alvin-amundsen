from typing import Any
from typing import Iterator

from databuilder.task.task import DefaultTask


class SentinelTask(DefaultTask):
    SENTINEL_VALUE = "*** SENTINEL_VALUE ***"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(SentinelTask, self).__init__(*args, **kwargs)

    def process_record(self, record: Any) -> bool:
        record = self.transformer.transform(record)
        if not record:
            return False
        results = record if isinstance(record, Iterator) else [record]
        for result in results:
            if result:
                self.loader.load(result)
        return True

    def run(self) -> None:
        try:
            record = self.extractor.extract()

            while record:
                if not self.process_record(record):
                    record = self.extractor.extract()
                    continue
                record = self.extractor.extract()
        finally:
            self.process_record(self.SENTINEL_VALUE)
            self._closer.close()
