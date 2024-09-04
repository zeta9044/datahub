import logging
import re
import time
import chardet
from functools import wraps
from datahub.metadata.schema_classes import (
    SchemaFieldDataTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NullTypeClass,
    EnumTypeClass,
    ArrayTypeClass,
    MapTypeClass,
    UnionTypeClass,
    RecordTypeClass
)

logger = logging.getLogger(__name__)


def read_file_with_utf8(file_path):
    """
    파일을 UTF-8 인코딩으로 읽는 함수.
    파일이 UTF-8이 아니면 UTF-8로 변환 후 읽음.

    Parameters:
    file_path (str): 파일 경로

    Returns:
    str: 파일 내용 (UTF-8로 인코딩된)
    """
    # 파일의 현재 인코딩 감지
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        current_encoding = result['encoding']

    # 파일을 현재 인코딩으로 읽기
    with open(file_path, 'r', encoding=current_encoding) as f:
        content = f.read()

    # 현재 인코딩이 UTF-8이 아니면 UTF-8로 변환하여 내용 반환
    if current_encoding.lower() != 'utf-8':
        content = content.encode(current_encoding).decode('utf-8')

    return content


def extract_dataset_info(entity_urn: str):
    # Define the regex pattern to match the platform, dataset part, and environment
    pattern = r'urn:li:dataset:\(urn:li:dataPlatform:([^,]+),([^,]+),([^)]+)\)'

    # Search for the pattern in the urn string
    match = re.search(pattern, entity_urn)

    # If a match is found, return the platform, dataset part, and environment
    if match:
        platform = match.group(1)
        dataset_info = match.group(2)
        env = match.group(3)
        return platform, dataset_info, env
    else:
        return None, None, None


def create_get_urn_query(entity_urn: str):
    platform, dataset_info, env = extract_dataset_info(entity_urn)
    if dataset_info:
        parts = dataset_info.split('.')
        # Create query with wildcard for 4-part pattern. except schema
        query_exact = (f"SELECT urn FROM metadata_aspect_v2 "
                       f"WHERE urn = '{entity_urn}' "
                       f"AND aspect_name='schemaMetadata' AND version=0")
        query_like = (f"SELECT urn FROM metadata_aspect_v2 "
                      f"WHERE urn LIKE 'urn:li:dataset:(urn:li:dataPlatform:{platform},%.%.%.{parts[3]},{env})' "
                      f"AND aspect_name='schemaMetadata' AND version=0")
    else:
        raise ValueError("Invalid URN format")
    return query_exact, query_like


def infer_type_from_native(native_data_type: str) -> SchemaFieldDataTypeClass:
    """
    :param native_data_type: The native data type to infer the DataHub type from.
    :return: An instance of the SchemaFieldDataTypeClass corresponding to the inferred DataHub type.

    This function takes a native data type and returns the corresponding DataHub type. It does this by mapping the native data type to a DataHub type class using a dictionary called type_mappings.
    If the native data type is not found in the type_mappings dictionary, a ValueError is raised.
    The function returns an instance of the SchemaFieldDataTypeClass with the inferred type.
    """
    # Define mappings from native data types to DataHub types
    type_mappings = {
        "BIGINT": NumberTypeClass,
        "BINARY": BytesTypeClass,
        "BIT": BooleanTypeClass,
        "CHAR": StringTypeClass,
        "DATE": TimeTypeClass,
        "DATETIME": TimeTypeClass,
        "DATETIME2": TimeTypeClass,
        "DATETIMEOFFSET": TimeTypeClass,
        "DECIMAL": NumberTypeClass,
        "FLOAT": NumberTypeClass,
        "IMAGE": BytesTypeClass,
        "INT": NumberTypeClass,
        "INTEGER": NumberTypeClass,
        "MEDIUMTEXT": StringTypeClass,
        "MONEY": NumberTypeClass,
        "NCHAR": StringTypeClass,
        "NTEXT": StringTypeClass,
        "NUMBER": NumberTypeClass,
        "NUMERIC": NumberTypeClass,
        "NVARCHAR": StringTypeClass,
        "REAL": NumberTypeClass,
        "SMALLDATETIME": TimeTypeClass,
        "SMALLINT": NumberTypeClass,
        "TEXT": StringTypeClass,
        "TIME": TimeTypeClass,
        "TIMESTAMP_NTZ": TimeTypeClass,
        "TINYINT": NumberTypeClass,
        "UNIQUEIDENTIFIER": StringTypeClass,
        "VARBINARY": BytesTypeClass,
        "VARCHAR": StringTypeClass,
        "VARIANT": UnionTypeClass,
        "XML": StringTypeClass,
        "BOOLEAN": BooleanTypeClass,
        "NULL": NullTypeClass,
        "ENUM": EnumTypeClass,
        "ARRAY": ArrayTypeClass,
        "MAP": MapTypeClass,
        "UNION": UnionTypeClass,
        "RECORD": RecordTypeClass
    }


    # Extract the base type from the native data type
    base_type = native_data_type.split('(')[0].upper()

    # Get the corresponding DataHub type class
    type_class = type_mappings.get(base_type, None)

    if type_class is None:
        raise ValueError(f"Unsupported native data type: {native_data_type}")

    return SchemaFieldDataTypeClass(type=type_class())


def log_execution_time(log_file='execution_time.log'):
    # logging 설정
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        handlers=[
                            logging.FileHandler(log_file),
                            logging.StreamHandler()
                        ])

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time

            formatted_time = format_time(execution_time)
            logging.info(f"Executed {func.__name__} in {formatted_time}")

            return result

        return wrapper

    return decorator

def format_time(seconds):
    if seconds >= 3600:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = seconds % 60
        return f"{hours}h {minutes}m {seconds:.2f}s"
    elif seconds >= 60:
        minutes = int(seconds // 60)
        seconds = seconds % 60
        return f"{minutes}m {seconds:.2f}s"
    else:
        return f"{seconds:.2f}s"

def infer_type_from_native(native_data_type: str) -> SchemaFieldDataTypeClass:
    """
    :param native_data_type: The native data type to infer the DataHub type from.
    :return: An instance of the SchemaFieldDataTypeClass corresponding to the inferred DataHub type.

    This function takes a native data type and returns the corresponding DataHub type. It does this by mapping the native data type to a DataHub type class using a dictionary called type_mappings.
    If the native data type is not found in the type_mappings dictionary, a ValueError is raised.
    The function returns an instance of the SchemaFieldDataTypeClass with the inferred type.
    """
    # Define mappings from native data types to DataHub types
    type_mappings = {
        "BIGINT": NumberTypeClass,
        "BINARY": BytesTypeClass,
        "BIT": BooleanTypeClass,
        "CHAR": StringTypeClass,
        "DATE": TimeTypeClass,
        "DATETIME": TimeTypeClass,
        "DATETIME2": TimeTypeClass,
        "DATETIMEOFFSET": TimeTypeClass,
        "DECIMAL": NumberTypeClass,
        "FLOAT": NumberTypeClass,
        "IMAGE": BytesTypeClass,
        "INT": NumberTypeClass,
        "INTEGER": NumberTypeClass,
        "MEDIUMTEXT": StringTypeClass,
        "MONEY": NumberTypeClass,
        "NCHAR": StringTypeClass,
        "NTEXT": StringTypeClass,
        "NUMBER": NumberTypeClass,
        "NUMERIC": NumberTypeClass,
        "NVARCHAR": StringTypeClass,
        "REAL": NumberTypeClass,
        "SMALLDATETIME": TimeTypeClass,
        "SMALLINT": NumberTypeClass,
        "TEXT": StringTypeClass,
        "TIME": TimeTypeClass,
        "TIMESTAMP_NTZ": TimeTypeClass,
        "TINYINT": NumberTypeClass,
        "UNIQUEIDENTIFIER": StringTypeClass,
        "VARBINARY": BytesTypeClass,
        "VARCHAR": StringTypeClass,
        "VARIANT": UnionTypeClass,
        "XML": StringTypeClass,
        "BOOLEAN": BooleanTypeClass,
        "NULL": NullTypeClass,
        "ENUM": EnumTypeClass,
        "ARRAY": ArrayTypeClass,
        "MAP": MapTypeClass,
        "UNION": UnionTypeClass,
        "RECORD": RecordTypeClass
    }


    # Extract the base type from the native data type
    base_type = native_data_type.split('(')[0].upper()

    # Get the corresponding DataHub type class
    type_class = type_mappings.get(base_type, None)

    if type_class is None:
        raise ValueError(f"Unsupported native data type: {native_data_type}")

    return SchemaFieldDataTypeClass(type=type_class())