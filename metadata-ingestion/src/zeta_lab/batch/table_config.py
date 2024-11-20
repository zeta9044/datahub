from opcode import cmp_op

from dataclasses import dataclass
from typing import List, Any, Optional, Callable, Dict
import logging
from datahub.utilities.urns.dataset_urn import DatasetUrn
from zeta_lab.utilities.tool import NameUtil


@dataclass
class ColumnMapping:
    """컬럼 매핑 정보"""
    source_name: str
    target_name: str
    transform_func: Optional[Callable] = None


@dataclass
class PostgresTableConfig:
    """PostgreSQL 테이블 설정"""
    source_columns: List[str]  # DuckDB의 컬럼
    target_columns: List[str]  # PostgreSQL의 컬럼
    column_mappings: List[ColumnMapping]  # 컬럼 매핑 정보
    needs_sequence: bool = False  # 시퀀스 사용 여부
    needs_system_tgt_srv_id: bool = False  # system_tgt_srv_id 함수 사용 여부
    partition_key: str = 'table_id'  # 파티셔닝 키
    src_prj_id_idx: int = -1  # src_prj_id 컬럼 인덱스
    tgt_prj_id_idx: int = -1  # tgt_prj_id 컬럼 인덱스


class TableConfigFactory:
    """테이블 설정 팩토리"""

    @staticmethod
    def create_ais0102_config(duckdb_columns: List[str]) -> PostgresTableConfig:
        """AIS0102 테이블 설정 생성"""

        def transform_query_type(row: Dict[str, Any]) -> str:
            """
            :param duckdb_columns: List of column names from DuckDB that need to be configured for PostgreSQL.
            :return: A PostgreSQL table configuration object.


            """
            sql_type_map = {
                'INSERT': 'C',
                'COPY': 'C',
                'CREATE': 'C',
                'SELECT': 'R',
                'UPDATE': 'U',
                'MERGE': 'U',
                'DELETE': 'D',
                'DROP': 'D',
                'TRUNCATE': 'D'
            }
            compared_value = row.get('query_type', 'N').upper()
            for key, single_char in sql_type_map.items():
                if compared_value.startswith(key):
                    return single_char
            return compared_value

        def extract_from_urn(row: Dict[str, Any], field: str) -> str:
            """URN에서 필드 추출"""
            try:
                table_urn = row.get('table_urn')
                if not table_urn:
                    return None

                dataset_urn = DatasetUrn.from_string(table_urn)
                table_content = dataset_urn.get_dataset_name()

                if field == 'table_name':
                    return NameUtil.get_table_name(table_content)
                elif field == 'caps_table_name':
                    return NameUtil.get_table_name(table_content).upper()
                elif field == 'owner_name':
                    return NameUtil.get_schema(table_content)
                elif field == 'unique_owner_name':
                    return NameUtil.get_unique_owner_name(table_content)
                elif field == 'unique_owner_tgt_srv_id':
                    return NameUtil.get_unique_owner_tgt_srv_id(table_content)

            except Exception as e:
                logging.error(f"Error extracting {field} from URN {table_urn}: {e}")
            return None

        # 컬럼 매핑 정의
        mappings = [
            ColumnMapping('prj_id', 'prj_id'),
            ColumnMapping('file_id', 'file_id'),
            ColumnMapping('sql_id', 'sql_id'),
            ColumnMapping('table_id', 'table_id'),
            ColumnMapping('obj_id', 'obj_id'),
            ColumnMapping('func_id', 'func_id'),
            ColumnMapping('table_urn', 'table_name',
                          lambda row: extract_from_urn(row, 'table_name')),
            ColumnMapping('table_urn', 'caps_table_name',
                          lambda row: extract_from_urn(row, 'caps_table_name')),
            ColumnMapping('table_urn', 'owner_name',
                          lambda row: extract_from_urn(row, 'owner_name')),
            ColumnMapping('query_type', 'query_type',
                          lambda row: transform_query_type(row)),
            ColumnMapping(None, 'query_line_no', lambda _: None),
            ColumnMapping('sql_obj_type', 'sql_obj_type'),
            ColumnMapping(None, 'inlineview_yn', lambda _: None),
            ColumnMapping(None, 'dblink_name', lambda _: None),
            ColumnMapping(None, 'table_alias_name', lambda _: None),
            ColumnMapping(None, 'inlineview_src', lambda _: None),
            ColumnMapping(None, 'sql_state', lambda _: None),
            ColumnMapping(None, 'column_no', lambda _: None),
            ColumnMapping(None, 'table_depth', lambda _: None),
            ColumnMapping(None, 'table_order_no', lambda _: None),
            ColumnMapping(None, 'rel_table_id', lambda _: None),
            ColumnMapping(None, 'rel_flow_id', lambda _: None),
            ColumnMapping(None, 'dbc_mapping_yn', lambda _: None),
            ColumnMapping(None, 'teradata_sql_id', lambda _: None),
            ColumnMapping('table_urn', 'unique_owner_name',
                          lambda row: extract_from_urn(row, 'unique_owner_name')),
            ColumnMapping('table_urn', 'unique_owner_tgt_srv_id',
                          lambda row: extract_from_urn(row, 'unique_owner_tgt_srv_id')),
            ColumnMapping(None, 'sql_name', lambda _: None),
            ColumnMapping(None, 'system_biz_id', lambda _: None),
            ColumnMapping(None, 'fl_tbl_uid', lambda _: None)
        ]

        return PostgresTableConfig(
            source_columns=duckdb_columns,
            target_columns=[m.target_name for m in mappings],
            column_mappings=mappings,
            partition_key='table_id'
        )

    @staticmethod
    def create_sequence_config(table_name: str, duckdb_columns: List[str]) -> PostgresTableConfig:
        """시퀀스 테이블(ais0080, ais0081) 설정 생성"""
        # 소스 컬럼과 타겟 컬럼이 동일한 경우
        mappings = [ColumnMapping(col, col) for col in duckdb_columns]

        return PostgresTableConfig(
            source_columns=duckdb_columns,
            target_columns=duckdb_columns,
            column_mappings=mappings,
            needs_sequence=True,
            needs_system_tgt_srv_id=True,
            partition_key='src_mte_table_id'
        )

    @staticmethod
    def create_standard_config(table_name: str, duckdb_columns: List[str]) -> PostgresTableConfig:
        """표준 테이블(ais0103, ais0112, ais0113) 설정 생성"""
        # 소스 컬럼과 타겟 컬럼이 동일한 경우
        mappings = [ColumnMapping(col, col) for col in duckdb_columns]

        return PostgresTableConfig(
            source_columns=duckdb_columns,
            target_columns=duckdb_columns,
            column_mappings=mappings,
            partition_key='table_id'
        )

    @staticmethod
    def create_config(table_name: str, duckdb_columns: List[str]) -> PostgresTableConfig:
        """테이블 설정 생성"""
        if table_name == 'ais0102':
            return TableConfigFactory.create_ais0102_config(duckdb_columns)
        elif table_name in ('ais0080', 'ais0081'):
            return TableConfigFactory.create_sequence_config(table_name, duckdb_columns)
        else:
            return TableConfigFactory.create_standard_config(table_name, duckdb_columns)
