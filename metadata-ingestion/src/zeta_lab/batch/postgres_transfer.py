import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Any, Optional, Dict

import psycopg2
import psycopg2.extras
from dataclasses import dataclass, field

from zeta_lab.batch.batch_processor import BatchProcessor, BatchConfig
from zeta_lab.batch.table_config import PostgresTableConfig, TableConfigFactory


@dataclass
class PartitionedTransferConfig(BatchConfig):
    """파티션 처리 설정"""
    # BatchConfig의 필수 필드들
    chunk_size: int = 5000
    batch_size: int = 10000
    logger: Optional[logging.Logger] = None
    error_threshold: int = 100

    # PartitionedTransferConfig의 추가 필드들
    connection_pool: Any = field(default=None)  # psycopg2.pool.SimpleConnectionPool
    table_name: str = field(default="")
    table_config: PostgresTableConfig = field(default=None)
    partition_size: int = field(default=100000)
    max_workers: int = field(default=4)

class TablePartitioner:
    """DuckDB 테이블 순차 파티셔닝 처리기"""

    def __init__(self, duckdb_conn, table_name: str, batch_size: int):
        self.conn = duckdb_conn
        self.table_name = table_name
        self.batch_size = batch_size

    def get_total_count(self) -> int:
        """전체 레코드 수 조회"""
        return self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]

    def get_batch(self, offset: int) -> List[tuple]:
        """배치 단위로 데이터 조회"""
        query = f"""
            SELECT *
            FROM {self.table_name}
            LIMIT {self.batch_size}
            OFFSET {offset}
        """
        return self.conn.execute(query).fetchall()

class PartitionedPostgresProcessor(BatchProcessor):
    """파티션 단위 PostgreSQL 데이터 처리기"""

    def __init__(self, config: PartitionedTransferConfig):
        super().__init__(config)
        self.pool = config.connection_pool
        self.table_name = config.table_name
        self.table_config = config.table_config

        # src_prj_id와 tgt_prj_id 인덱스 초기화
        if self.table_config.needs_sequence:
            self.table_config.src_prj_id_idx = self.table_config.source_columns.index('src_prj_id')
            self.table_config.tgt_prj_id_idx = self.table_config.source_columns.index('tgt_prj_id')

    def prepare_data(self, items: Any) -> List[Any]:
        """데이터 준비"""
        if isinstance(items, (list, tuple)):
            if self.table_config.needs_sequence and self.table_config.needs_system_tgt_srv_id:
                return self._prepare_sequence_data([items])
            return self._prepare_standard_data([items])
        return []

    def process_chunk(self, chunk: List[Any]) -> bool:
        """청크 단위 데이터 처리"""
        return self._process_data(chunk)

    def process_partition(self, partition_data: List[Any]) -> bool:
        """파티션 단위 데이터 처리 (process_chunk와 동일)"""
        return self._process_data(partition_data)

    def _process_data(self, data: List[Any]) -> bool:
        """실제 데이터 처리 로직"""
        if not data:
            return True

        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                if self.table_config.needs_sequence and self.table_config.needs_system_tgt_srv_id:
                    insert_query = self._build_sequence_insert_query()
                    modified_data = self._prepare_sequence_data(data)
                else:
                    insert_query = self._build_standard_insert_query()
                    modified_data = self._prepare_standard_data(data)

                psycopg2.extras.execute_batch(
                    cur,
                    insert_query,
                    modified_data,
                    page_size=1000
                )

                conn.commit()
                return True

        except Exception as e:
            self.logger.error(f"Error processing data for {self.table_name}: {e}")
            conn.rollback()
            return False

        finally:
            self.pool.putconn(conn)

    def _prepare_sequence_data(self, data: List[tuple]) -> List[tuple]:
        """시퀀스 테이블용 데이터 준비"""
        prepared_data = []

        for row in data:
            # 원본 데이터를 딕셔너리로 변환
            row_dict = dict(zip(self.table_config.source_columns, row))

            # 변환된 데이터 준비
            transformed_row = self._transform_row(row_dict)

            # src_prj_id와 tgt_prj_id를 앞쪽에 추가
            src_prj_id = row[self.table_config.src_prj_id_idx]
            tgt_prj_id = row[self.table_config.tgt_prj_id_idx]

            prepared_data.append((src_prj_id, tgt_prj_id, *transformed_row))

        return prepared_data

    def _prepare_standard_data(self, data: List[tuple]) -> List[tuple]:
        """일반 테이블용 데이터 준비"""
        prepared_data = []

        for row in data:
            # 원본 데이터를 딕셔너리로 변환
            row_dict = dict(zip(self.table_config.source_columns, row))

            # 변환된 데이터 준비
            transformed_row = self._transform_row(row_dict)
            prepared_data.append(transformed_row)

        return prepared_data

    def _transform_row(self, row_dict: dict) -> tuple:
        """행 데이터 변환"""
        transformed_values = []

        for mapping in self.table_config.column_mappings:
            if mapping.transform_func:
                # 변환 함수가 있는 경우
                value = mapping.transform_func(row_dict)
            elif mapping.source_name:
                # 단순 매핑인 경우
                value = row_dict.get(mapping.source_name)
            else:
                # 소스가 없는 경우 (새로운 컬럼)
                value = None

            transformed_values.append(value)

        return tuple(transformed_values)

    def _build_sequence_insert_query(self) -> str:
        """시퀀스를 포함한 INSERT 쿼리 생성"""
        target_cols = ['seq_id', 'src_system_tgt_srv_id', 'tgt_system_tgt_srv_id']
        target_cols.extend(self.table_config.target_columns)

        return f"""
            INSERT INTO {self.table_name} (
                {', '.join(target_cols)}
            )
            VALUES (
                nextval('seq_{self.table_name}'),
                AP_COMMON_FN_SYSTEM_TGTSRVID(%s)::varchar(100),
                AP_COMMON_FN_SYSTEM_TGTSRVID(%s)::varchar(100),
                {', '.join(['%s' for _ in self.table_config.target_columns])}
            )
            ON CONFLICT DO NOTHING
        """

    def _build_standard_insert_query(self) -> str:
        """일반 INSERT 쿼리 생성"""
        return f"""
            INSERT INTO {self.table_name} (
                {', '.join(self.table_config.target_columns)}
            )
            VALUES (
                {', '.join(['%s' for _ in self.table_config.target_columns])}
            )
            ON CONFLICT DO NOTHING
        """

class PartitionedPostgresBatchProcessor:
    """PostgreSQL 병렬 처리기"""

    def __init__(self, pg_pool, table_name: str, table_config: PostgresTableConfig,
                 max_workers: int, logger: Optional[logging.Logger] = None):
        self.pg_pool = pg_pool
        self.table_name = table_name
        self.table_config = table_config
        self.max_workers = max_workers
        self.logger = logger or logging.getLogger(__name__)

        # 한 번만 processor 생성
        self.processor = self._create_processor()

    def _create_processor(self) -> PartitionedPostgresProcessor:
        """PostgreSQL 프로세서 생성"""
        config = PartitionedTransferConfig(
            connection_pool=self.pg_pool,
            table_name=self.table_name,
            table_config=self.table_config,
            logger=self.logger
        )
        return PartitionedPostgresProcessor(config)

    def process_batches(self, batches: List[List[tuple]]) -> bool:
        """여러 배치 병렬 처리"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for batch in batches:
                # 미리 생성된 processor 재사용
                futures.append(executor.submit(self.processor.process_partition, batch))

            # 결과 확인
            for future in futures:
                try:
                    if not future.result():
                        return False
                except Exception as e:
                    self.logger.error(f"Error in batch processing: {e}")
                    return False

            return True

class PartitionedTransferManager:
    """DuckDB에서 PostgreSQL로의 순차적 데이터 전송 관리자"""

    def __init__(self, config: Dict):
        self.duckdb_conn = config['duckdb_conn']
        self.pg_pool = config['pg_pool']
        self.logger = config.get('logger') or logging.getLogger(__name__)
        self.max_workers = config.get('max_workers', 4)
        self.batch_size = config.get('batch_size', 10000)
        self.pg_batch_count = config.get('pg_batch_count', 5)  # 한 번에 PostgreSQL에 보낼 배치 수
        self.table_configs = self._initialize_table_configs()

    def transfer_table(self, table_name: str):
        """테이블 데이터 전송"""
        self.logger.info(f"Starting transfer for {table_name}")

        try:
            # Initialize partitioner for sequential reading
            partitioner = TablePartitioner(
                self.duckdb_conn,
                table_name,
                self.batch_size
            )

            # Get table config
            table_config = self.table_configs.get(table_name)
            if not table_config:
                raise Exception(f"No configuration found for table {table_name}")

            # Initialize PostgreSQL batch processor
            pg_processor = PartitionedPostgresBatchProcessor(
                self.pg_pool,
                table_name,
                table_config,
                self.max_workers,
                self.logger
            )

            # Get total count
            total_count = partitioner.get_total_count()
            if total_count == 0:
                self.logger.info(f"No data to transfer for {table_name}")
                return

            # Process in batches
            offset = 0
            batches = []
            batch_count = 0

            while offset < total_count:
                # Read from DuckDB (순차적으로)
                batch = partitioner.get_batch(offset)
                if not batch:
                    break

                batches.append(batch)
                batch_count += 1
                offset += len(batch)

                # PostgreSQL에 병렬로 처리할 만큼 배치가 쌓였으면 처리
                if batch_count >= self.pg_batch_count:
                    success = pg_processor.process_batches(batches)
                    if not success:
                        raise Exception(f"Failed to process batches at offset {offset}")
                    batches = []
                    batch_count = 0

                self.logger.info(f"Processed {offset}/{total_count} records for {table_name}")

            # 남은 배치 처리
            if batches:
                success = pg_processor.process_batches(batches)
                if not success:
                    raise Exception(f"Failed to process remaining batches")

            self.logger.info(f"Completed transfer for {table_name}")

        except Exception as e:
            self.logger.error(f"Error transferring {table_name}: {e}")
            raise

    def _initialize_table_configs(self) -> Dict[str, PostgresTableConfig]:
        """테이블별 설정 초기화"""
        configs = {}
        for table_name in ['ais0102', 'ais0103', 'ais0112', 'ais0113', 'ais0080', 'ais0081']:
            duckdb_columns = self._get_duckdb_columns(table_name)
            # 여기를 수정: self.create_config -> TableConfigFactory.create_config
            configs[table_name] = TableConfigFactory.create_config(table_name, duckdb_columns)
        return configs

    def _get_duckdb_columns(self, table_name: str) -> List[str]:
        """DuckDB 테이블의 컬럼 목록 조회"""
        query = f"DESCRIBE {table_name}"
        columns = self.duckdb_conn.execute(query).fetchall()
        return [col[0] for col in columns]