import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import List, Any, Optional

from dataclasses import dataclass


@dataclass
class BatchConfig:
    """배치 처리 설정"""
    chunk_size: int = 5000
    batch_size: int = 10000
    logger: Optional[logging.Logger] = None
    error_threshold: int = 100  # 연속 에러 허용 횟수

class BatchProcessor(ABC):
    """배치 처리를 위한 추상 기본 클래스"""

    def __init__(self, config: BatchConfig):
        self.config = config
        self.logger = config.logger or logging.getLogger(__name__)
        self._batch_data: List[Any] = []
        self.error_count = 0

    @abstractmethod
    def prepare_data(self, item: Any) -> List[Any]:
        """단일 아이템을 처리하여 배치 데이터로 변환"""
        pass

    @abstractmethod
    def process_chunk(self, chunk: List[Any]) -> bool:
        """청크 단위 데이터 처리"""
        pass

    @contextmanager
    def transaction_scope(self):
        """트랜잭션 컨텍스트 매니저"""
        try:
            yield
        except Exception as e:
            self.logger.error(f"Transaction error: {e}")
            raise

    def add_item(self, item: Any) -> None:
        """배치에 아이템 추가 및 필요시 처리"""
        prepared_data = self.prepare_data(item)
        self._batch_data.extend(prepared_data)

        if len(self._batch_data) >= self.config.batch_size:
            self.process_batch()

    def add_items(self, items: List[Any]) -> None:
        """여러 아이템을 한번에 배치에 추가"""
        for item in items:
            self.add_item(item)

    def process_batch(self) -> None:
        """현재 배치 데이터 처리"""
        if not self._batch_data:
            return

        total_chunks = (len(self._batch_data) + self.config.chunk_size - 1) // self.config.chunk_size

        for chunk_idx in range(total_chunks):
            start_idx = chunk_idx * self.config.chunk_size
            end_idx = start_idx + self.config.chunk_size
            current_chunk = self._batch_data[start_idx:end_idx]

            try:
                with self.transaction_scope():
                    success = self.process_chunk(current_chunk)
                    if success:
                        self.error_count = 0  # 성공 시 에러 카운트 리셋
                    else:
                        self.error_count += 1

                if self.error_count >= self.config.error_threshold:
                    raise Exception(f"Error threshold exceeded: {self.error_count} consecutive errors")

            except Exception as e:
                self.logger.error(f"Error processing chunk {chunk_idx + 1}/{total_chunks}: {e}")
                raise

            self.logger.debug(f"Processed chunk {chunk_idx + 1}/{total_chunks}")

        self._batch_data = []  # 배치 데이터 클리어

    def finalize(self) -> None:
        """남은 배치 데이터 처리"""
        if self._batch_data:
            self.process_batch()

class DuckDBBatchProcessor(BatchProcessor):
    """DuckDB 특화 배치 처리기 기본 클래스"""

    def __init__(self, connection, config: BatchConfig):
        super().__init__(config)
        self.connection = connection

    @contextmanager
    def transaction_scope(self):
        """DuckDB 트랜잭션 관리"""
        try:
            self.connection.execute("BEGIN TRANSACTION")
            yield
            self.connection.execute("COMMIT")
        except Exception as e:
            self.connection.execute("ROLLBACK")
            self.logger.error(f"Transaction failed, rolling back: {e}")
            raise