import hashlib
import logging
import time
from typing import Optional, Tuple, List

import requests

import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata._schema_classes import (
    MetadataChangeEventClass,
    DatasetSnapshotClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    AuditStampClass,
)
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass
)
from datahub.metadata.schema_classes import StringTypeClass
from datahub.utilities.urns.dataset_urn import DatasetUrn
from sqlglot import exp
from sqlglot.optimizer import build_scope
from zeta_lab.utilities.tool import infer_type_from_native

logger = logging.getLogger(__name__)


def find_stars_in_expr(expr: exp.Expression, parent_select: exp.Select) -> list:
    """
    Within the given expr,
    recursively collect exp.Star nodes whose ancestor is parent_select.
    (In other words, star nodes that do not extend into inner subqueries, etc.)
    """

    stars = []
    for star in expr.find_all(exp.Star):
        if star.find_ancestor(exp.Select) == parent_select:
            stars.append(star)
    return stars


def merge_mappings(map1: dict, map2: dict) -> dict:
    """
    Merge two mapping dicts ({source_table: set(columns)})
    and return them in the form of {source_table: sorted_list_of_columns}.
    """

    merged = {}
    for table, cols in map1.items():
        merged.setdefault(table, set()).update(cols)
    for table, cols in map2.items():
        merged.setdefault(table, set()).update(cols)
    return {table: sorted(list(cols)) for table, cols in merged.items()}


def resolve_alias_origin(table_node: exp.Expression, alias: str) -> str:
    """
    For the table_node in the scope:
    if it is a base table (exp.Table), return its name;
    if it is a subquery (or CTE), extract the name from the result of the inner SELECT
    (in the case of a single source table).
    If determination is difficult, return the alias as is.
    """

    if isinstance(table_node, exp.Table):
        return table_node.name
    elif isinstance(table_node, exp.Subquery):
        # Recursively analyze inner SELECT statements.
        sub_mapping = get_expanded_columns_map(table_node.this)
        if len(sub_mapping) == 1:
            return list(sub_mapping.keys())[0]
    return alias


def process_select(select: exp.Select) -> dict:
    """
    For a single SELECT statement, use build_scope to expand stars (*)
    and explicit columns into a source (table, column) mapping.

    Return type: {source_table: set(columns)} (sorting handled later in merge_mappings)
    """

    mapping = {}
    scope = build_scope(select)

    # The tables property returns a list of Tables.
    alias_to_table = {}
    for table_node in scope.tables:
        alias = table_node.alias_or_name  # Retrieve the alias or name of the table.
        origin = resolve_alias_origin(table_node, alias)
        alias_to_table[alias.lower()] = origin

    def add_mapping(orig_table: str, orig_column: str):
        if orig_table:
            mapping.setdefault(orig_table, set()).add(orig_column)

    # Target: All expressions within select.expressions (explicit columns, stars, stars within functions/UDTFs, etc.)
    for expr in select.args.get('expressions', []):
        # 1. Handle direct column references
        if isinstance(expr, exp.Column):
            if expr.table:
                for a, t in alias_to_table.items():
                    if expr.table.lower() == a:
                        add_mapping(t, expr.name)
                        break
            else:
                if len(alias_to_table) == 1:
                    add_mapping(list(alias_to_table.values())[0], expr.name)
        elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Column):
            col = expr.this
            if col.table:
                for a, t in alias_to_table.items():
                    if col.table.lower() == a:
                        add_mapping(t, col.name)
                        break
            else:
                if len(alias_to_table) == 1:
                    add_mapping(list(alias_to_table.values())[0], col.name)

        # 2. Directly handle top-level stars
        if isinstance(expr, exp.Star):
            qualifier = expr.this
            if qualifier:
                alias = qualifier.name.lower()
                orig_table = alias_to_table.get(alias)
                if orig_table:
                    # Add column references within this SELECT scope that match the alias.
                    for col in select.find_all(exp.Column):
                        if col.table and col.table.lower() == alias:
                            add_mapping(orig_table, col.name)
            else:
                for col in select.find_all(exp.Column):
                    if col.table:
                        for a, t in alias_to_table.items():
                            if col.table.lower() == a:
                                add_mapping(t, col.name)
                    else:
                        if len(alias_to_table) == 1:
                            add_mapping(list(alias_to_table.values())[0], col.name)

        # 3. Handle stars included in inner expressions, such as functions or UDTFs.
        inner_stars = find_stars_in_expr(expr, select)
        for star in inner_stars:
            qualifier = star.this
            if qualifier:
                alias = qualifier.name.lower()
                orig_table = alias_to_table.get(alias)
                if orig_table:
                    for col in select.find_all(exp.Column):
                        if col.table and col.table.lower() == alias:
                            add_mapping(orig_table, col.name)
            else:
                for col in select.find_all(exp.Column):
                    if col.table:
                        for a, t in alias_to_table.items():
                            if col.table.lower() == a:
                                add_mapping(t, col.name)
                    else:
                        if len(alias_to_table) == 1:
                            add_mapping(list(alias_to_table.values())[0], col.name)

    return mapping


def get_expanded_columns_map(expr: exp.Expression) -> dict:
    """
    If the parsed SQL AST is a SELECT statement, UNION, or composite structure,
    it performs star (*) expansion for each SELECT statement (or each branch of the UNION)
    and finally returns a mapping dict in the form of {source_table: [source_column, ...], ...}.

    - If it is a UNION, the left and right branches are processed recursively.
    - SELECT statements within subqueries or CTEs are also processed recursively.
    """

    mapping = {}

    if isinstance(expr, exp.Union):
        left = expr.args.get("this")
        right = expr.args.get("expression")
        mapping = merge_mappings(get_expanded_columns_map(left), get_expanded_columns_map(right))
    elif isinstance(expr, exp.Select):
        mapping = process_select(expr)
        # Recursion: Process subqueries (or SELECT statements within CTEs) within the current SELECT as well
        for sub_select in expr.find_all(exp.Select):
            if sub_select is not expr:
                sub_mapping = get_expanded_columns_map(sub_select)
                mapping = merge_mappings(mapping, sub_mapping)
    else:
        # Handle all cases where a SELECT is included within other nodes
        for select in expr.find_all(exp.Select):
            mapping = merge_mappings(mapping, get_expanded_columns_map(select))

    return mapping


def get_expanded_table_columns(expr: exp.Expression) -> list[tuple[exp.Table, list[exp.Column]]]:
    """
    Parses and expands the given SQL expression to extract table and column information, including schema and catalog details.

    :param expr: The SQL expression that may contain tables and associated columns.
    :type expr: exp.Expression
    :return: A list of tuples, where each tuple contains a Table object (with schema/catalog metadata) and a list of associated Column objects.
    :rtype: list[tuple[exp.Table, list[exp.Column]]]
    """
    raw_mapping = get_expanded_columns_map(expr)
    result = []

    # Finding table information from the original query
    table_info = {}
    for table in expr.find_all(exp.Table):
        if isinstance(table.this, exp.Identifier):
            qualified_name = table.name
            if qualified_name not in table_info:
                table_info[qualified_name] = {
                    'db': table.text('db'),
                    'catalog': table.text('catalog')
                }

    for table_name, column_names in raw_mapping.items():
        # Create a Table object (including db and schema information)
        table_parts = table_name.split('.')
        if len(table_parts) == 3:  # catalog.db.table
            catalog, db, table_name = table_parts
            table = exp.Table(
                this=exp.Identifier(this=table_name),
                db=exp.Identifier(this=db),
                catalog=exp.Identifier(this=catalog)
            )
        elif len(table_parts) == 2:  # db.table
            db, table_name = table_parts
            table = exp.Table(
                this=exp.Identifier(this=table_name),
                db=exp.Identifier(this=db)
            )
        else:  # table only
            table = exp.Table(this=exp.Identifier(this=table_name))
            # Check additional information in table_info
            if table_name in table_info:
                info = table_info[table_name]
                if info['db']:
                    table.set('db', exp.Identifier(this=info['db']))
                if info['catalog']:
                    table.set('catalog', exp.Identifier(this=info['catalog']))

        # Create a list of Column objects
        columns = [
            exp.Column(
                this=exp.Identifier(this=col_name),
                table=exp.Identifier(this=table_name),
                db=exp.Identifier(this=table.text('db')) if table.text('db') else None,
                catalog=exp.Identifier(this=table.text('catalog')) if table.text('catalog') else None
            )
            for col_name in column_names
        ]

        result.append((table, columns))

    return result


def get_columns_by_table(target_table: exp.Table, infer_table_columns: list[tuple[exp.Table, list[exp.Column]]]) -> \
        list[exp.Column] | None:
    """
    :param target_table: The table for which columns need to be retrieved.
    :param infer_table_columns: A list of tuples, where each tuple contains a table and its corresponding list of columns.
    :return: The list of columns for the specified target_table if found, else None.
    """
    for table, columns in infer_table_columns:
        if table == target_table:
            return columns
    return None


def extract_table_info(statement: exp.Expression, ctx: PipelineContext) -> Optional[Tuple[str, str, str]]:
    """SQL 문에서 테이블 정보(database, schema, table)를 추출"""
    default_db = ctx.pipeline_config.source.config.get('default_db')
    default_schema = ctx.pipeline_config.source.config.get('default_schema')

    if isinstance(statement, exp.Create):
        table = statement.find(exp.Table)
    elif isinstance(statement, exp.Insert):
        table = statement.find(exp.Table)
    else:
        return None

    if not table:
        return None

    return (
        table.catalog or default_db,
        table.db or default_schema,
        table.name
    )


def extract_schema_fields(statement: exp.Expression) -> List[SchemaFieldClass]:
    """SQL 문에서 스키마 필드 정보 추출"""
    fields = []

    if isinstance(statement, exp.Create):
        columns = statement.find_all(exp.ColumnDef)
        for column in columns:
            native_type = column.kind.this.value if column.kind.this else "TEXT"
            type_class = SchemaFieldDataTypeClass(
                type=infer_type_from_native(native_type)
            )

            fields.append(SchemaFieldClass(
                fieldPath=column.name.lower(),
                type=type_class,
                nativeDataType=native_type,
                description=column.comments if column.comments else ""
            ))
    elif isinstance(statement, exp.Insert):
        # INSERT INTO의 경우 컬럼명만 추출
        columns = statement.this.expressions
        if columns:
            fields.extend([
                SchemaFieldClass(
                    fieldPath=col.name.lower(),
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="unknown",
                )
                for col in columns
            ])

    return fields


def check_schema_exists(ctx: PipelineContext, dataset_urn: str) -> bool:
    """GMS에 스키마가 이미 존재하는지 확인"""
    try:
        url = f"{ctx.pipeline_config.datahub_api.server}/aspects/{DatasetUrn.url_encode(dataset_urn)}?aspect=schemaMetadata&version=0"
        with requests.get(url, timeout=ctx.pipeline_config.datahub_api.timeout_sec) as response:
            if response.status_code == 200:
                return True
            else:
                return False
    except Exception as e:
        logger.debug(f"Error check_schema_exists for {dataset_urn}: {e}")
        return False


def send_to_gms(metadata_change_proposals, gms_server):
    """
    :param metadata_change_proposals: A list of dictionaries where each dictionary represents a metadata change proposal containing the 'aspect' and 'entityUrn'.
    :param gms_server: A string representing the GMS server address.
    :return: None
    """
    emitter = DatahubRestEmitter(gms_server)
    for proposal in metadata_change_proposals:
        try:
            aspect = proposal['aspect']

            # Create MetadataChangeEventClass
            mce = MetadataChangeEventClass(
                proposedSnapshot=DatasetSnapshotClass(
                    urn=proposal['entityUrn'],
                    aspects=[aspect]
                )
            )

            emitter.emit_mce(mce)
            logging.debug(f"Successfully sent proposal for {proposal['entityUrn']}")
        except Exception as e:
            logging.error(f"Failed to send proposal for {proposal['entityUrn']}: {e}")


def register_inferred_schema(statement: exp.Expression, ctx: PipelineContext):
    """SQL 문에서 스키마를 추론하여 DataHub GMS에 등록"""
    # GMS URL과 Emitter 설정
    gms_url = ctx.pipeline_config.datahub_api.server

    # 테이블 정보 추출
    table_info = extract_table_info(statement, ctx)
    if not table_info:
        return

    # Dataset URN 생성
    platform = ctx.pipeline_config.source.config.get('platform')
    platform_instance = ctx.pipeline_config.source.config.get('platform_instance')
    default_db = ctx.pipeline_config.source.config.get('default_db')
    default_schema = ctx.pipeline_config.source.config.get('default_schema')
    owner_srv_id = ctx.pipeline_config._raw_dict.get('owner_srv_id')
    system_biz_id = ctx.pipeline_config._raw_dict.get('system_biz_id')

    if not table_info[0] and table_info[1]:
        name = f"{platform_instance}.{default_db}.{table_info[1]}.{table_info[2]}"
    elif not table_info[0] and not table_info[1]:
        name = f"{platform_instance}.{default_db}.{default_schema}.{table_info[2]}"
    else:
        name = f"{platform_instance}.{table_info[0]}.{table_info[1]}.{table_info[2]}"

    name = name.lower()
    dataset_urn = builder.make_dataset_urn(platform, name)

    # 이미 스키마가 존재하는지 확인
    if check_schema_exists(ctx, dataset_urn):
        return

    # 스키마 필드 추출
    fields = extract_schema_fields(statement)
    if not fields:
        return

    metadata_change_proposals = []
    schema_metadata_dict = {
        "schemaName": name,
        "platform": platform,
        "version": 0,
        "fields": fields,
        "platformSchema": {
            "com.linkedin.schema.MySqlDDL": {
                "tableSchema": ""
            }
        },
        "hash": hashlib.md5(str(fields).encode()).hexdigest(),
        "lastModified": AuditStampClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
        ),
    }
    schema_metadata = SchemaMetadataClass(**schema_metadata_dict)

    metadata_change_proposals.append({
        "entityType": "dataset",
        "entityUrn": dataset_urn,
        "aspectName": "schemaMetadata",
        "aspect": schema_metadata
    })

    # DatasetProperties aspect 생성
    dataset_properties = DatasetPropertiesClass(
        description="",
        name=name,
        customProperties={
            "owner_srv_id": owner_srv_id,
            "system_biz_id": system_biz_id,
        }
    )

    metadata_change_proposals.append({
        "entityType": "dataset",
        "entityUrn": dataset_urn,
        "aspectName": "datasetProperties",
        "aspect": dataset_properties
    })

    send_to_gms(metadata_change_proposals, gms_url)
