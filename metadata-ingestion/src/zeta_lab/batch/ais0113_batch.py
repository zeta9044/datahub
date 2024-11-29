from typing import List, Dict, Tuple

from datahub.utilities.urns.dataset_urn import DatasetUrn
from zeta_lab.batch.batch_processor import DuckDBBatchProcessor
from zeta_lab.utilities.tool import (
    NameUtil, get_sql_obj_type)


class AIS0113BatchProcessor(DuckDBBatchProcessor):
    """AIS0113 테이블 배치 처리기"""

    def prepare_data(self, item: Dict) -> List[Tuple]:
        """
        AIS0113용 데이터 준비

        Args:
            item: Dict containing:
                - upstream: {urn, properties, table_id, query_custom_keys}
                - downstream: {urn, properties, table_id}
                - upstream_col: {name, table_id, col_id}
                - downstream_col: {name, table_id, col_id}
                - transform_operation: string
                - col_order_no: int
                - call_col_order_no: int
        Returns:
            List[Tuple]: 데이터베이스에 삽입할 데이터 튜플 리스트
        """
        try:
            prepared_data = []

            # Extract required data
            upstream = item['upstream']
            downstream = item['downstream']
            upstream_col = item['upstream_col']
            downstream_col = item['downstream_col']
            transform_operation = item.get('transform_operation', '')
            col_order_no = item.get('col_order_no', 0)
            call_col_order_no = item.get('call_col_order_no', 0)

            # Process upstream dataset
            upstream_dataset = DatasetUrn.from_string(upstream['urn'])
            upstream_content = upstream_dataset.get_dataset_name()
            upstream_owner = NameUtil.get_schema(upstream_content).upper()
            upstream_table = NameUtil.get_table_name(upstream_content)
            upstream_sql_obj_type = get_sql_obj_type(upstream_table)
            upstream_unique_owner = NameUtil.get_unique_owner_name(upstream_content).upper()
            upstream_unique_owner_tgt_srv_id = NameUtil.get_unique_owner_tgt_srv_id(upstream_content).upper()

            # Process downstream dataset
            downstream_dataset = DatasetUrn.from_string(downstream['urn'])
            downstream_content = downstream_dataset.get_dataset_name()
            downstream_owner = NameUtil.get_schema(downstream_content).upper()
            downstream_table = NameUtil.get_table_name(downstream_content)
            downstream_sql_obj_type = get_sql_obj_type(downstream_table)
            downstream_unique_owner = NameUtil.get_unique_owner_name(downstream_content).upper()
            downstream_unique_owner_tgt_srv_id = NameUtil.get_unique_owner_tgt_srv_id(downstream_content).upper()

            # Get query custom keys
            query_custom_keys = upstream['query_custom_keys']

            for query in query_custom_keys:
                # Create tuple for insertion
                prepared_data.append((
                    query.get('prj_id', ''),
                    int(query.get('file_id', 0)),
                    int(query.get('sql_id', 0)),
                    upstream['table_id'],
                    upstream_col['col_id'],
                    int(query.get('obj_id', 0)),
                    int(query.get('func_id', 0)),
                    upstream_owner,
                    upstream_table,
                    upstream_table.upper(),
                    upstream_sql_obj_type,
                    upstream_col['name'],
                    upstream_col['name'].upper(),
                    'N',  # col_value_yn
                    transform_operation,
                    upstream_col['name'],
                    upstream_col['name'].upper(),
                    upstream_unique_owner,
                    upstream_unique_owner_tgt_srv_id,
                    upstream['properties']['aspect']['datasetProperties']['customProperties'].get('system_biz_id'),
                    query.get('prj_id', ''),
                    int(query.get('file_id', 0)),
                    int(query.get('sql_id', 0)),
                    downstream['table_id'],
                    downstream_col['col_id'],
                    int(query.get('obj_id', 0)),
                    int(query.get('func_id', 0)),
                    downstream_owner,
                    downstream_table,
                    downstream_table.upper(),
                    downstream_sql_obj_type,
                    downstream_col['name'],
                    downstream_col['name'].upper(),
                    'N',  # call_col_value_yn
                    '',  # call_col_expr
                    downstream_col['name'],
                    downstream_col['name'].upper(),
                    downstream_unique_owner,
                    downstream_unique_owner_tgt_srv_id,
                    downstream['properties']['aspect']['datasetProperties']['customProperties'].get('system_biz_id'),
                    col_order_no,
                    call_col_order_no,
                    col_order_no,  # adj_col_order_no
                    call_col_order_no,  # call_adj_col_order_no
                    1,  # cond_mapping
                    None,  # data_maker
                    ''  # mapping_kind
                ))

            return prepared_data

        except KeyError as e:
            self.logger.error(f"Missing required field in item: {e}")
            self.logger.error(f"Item content: {item}")
            raise

        except Exception as e:
            self.logger.error(f"Error preparing data for AIS0113: {e}")
            self.logger.error(f"Item content: {item}")
            raise

    def process_chunk(self, chunk: List[Tuple]) -> bool:
        """AIS0113 청크 처리"""
        try:
            self.connection.executemany("""
                INSERT OR REPLACE INTO ais0113 
                (prj_id, file_id, sql_id, table_id, col_id, obj_id, func_id,
                 owner_name, table_name, caps_table_name, sql_obj_type,
                 col_name, caps_col_name, col_value_yn, col_expr,
                 col_name_org, caps_col_name_org,
                 unique_owner_name, unique_owner_tgt_srv_id,
                 system_biz_id,
                 call_prj_id, call_file_id, call_sql_id, call_table_id, call_col_id,
                 call_obj_id, call_func_id, call_owner_name, call_table_name,
                 call_caps_table_name, call_sql_obj_type,
                 call_col_name, call_caps_col_name, call_col_value_yn, call_col_expr,
                 call_col_name_org, call_caps_col_name_org,
                 call_unique_owner_name, call_unique_owner_tgt_srv_id,
                 call_system_biz_id,
                 col_order_no, call_col_order_no, adj_col_order_no, call_adj_col_order_no,
                 cond_mapping, data_maker, mapping_kind)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?)
            """, chunk)
            return True
        except Exception as e:
            self.logger.error(f"Error processing AIS0113 chunk: {e}")
            return False
