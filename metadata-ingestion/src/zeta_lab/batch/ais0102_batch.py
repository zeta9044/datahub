from typing import List, Dict, Tuple

from datahub.utilities.urns.dataset_urn import DatasetUrn
from zeta_lab.batch.batch_processor import DuckDBBatchProcessor
from zeta_lab.utilities.tool import (
    NameUtil, get_owner_srv_id, get_sql_obj_type)


class AIS0102BatchProcessor(DuckDBBatchProcessor):
    """AIS0102 테이블 배치 처리기"""

    def prepare_data(self, item: Dict) -> List[Tuple]:
        """
        AIS0102용 데이터 준비

        Args:
            item: Dict containing:
                - urn: dataset URN
                - table_id: table identifier
                - query_custom_keys: dict of custom keys
                - properties: dataset properties
        """
        prepared_data = []

        # Extract common data from dataset URN
        dataset = DatasetUrn.from_string(item['urn'])
        content = dataset.get_dataset_name()
        table = NameUtil.get_table_name(content)
        sql_obj_type = get_sql_obj_type(table)


        # Create tuple for insertion
        prepared_data.append((
            item['query_custom_keys'].get('prj_id', ''),
            int(item['query_custom_keys'].get('file_id', 0)),
            int(item['query_custom_keys'].get('sql_id', 0)),
            item['table_id'],
            int(item['query_custom_keys'].get('obj_id', 0)),
            int(item['query_custom_keys'].get('func_id', 0)),
            item['query_custom_keys'].get('query_type', ''),
            sql_obj_type,
            item['urn'],
            item['properties'].get('system_biz_id','[owner_undefined]')
        ))

        return prepared_data

    def process_chunk(self, chunk: List[Tuple]) -> bool:
        """AIS0102 청크 처리"""
        try:
            self.connection.executemany("""
                INSERT OR REPLACE INTO ais0102 
                (
                 prj_id, file_id, sql_id, table_id, obj_id, 
                 func_id, query_type, sql_obj_type, table_urn, system_biz_id
                 )
                VALUES (?, ?, ?, ?, ?,  
                        ?, ?, ?, ?, ?
                        )
            """, chunk)
            return True
        except Exception as e:
            self.logger.error(f"Error processing AIS0102 chunk: {e}")
            return False
