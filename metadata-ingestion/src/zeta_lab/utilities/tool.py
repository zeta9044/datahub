import logging
import re
import time
import xml.etree.ElementTree as ET
from functools import wraps

import chardet
import psutil

from datahub.metadata._urns.urn_defs import DatasetUrn
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
from zeta_lab.utilities.decrypt_file import DecryptFile

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

    # default type is StringTypeClass
    if type_class is None:
        return SchemaFieldDataTypeClass(type=StringTypeClass())

    return SchemaFieldDataTypeClass(type=type_class())


class NameUtil:
    @staticmethod
    def get_platform_instance(input_string):
        parts = input_string.split('.')
        return parts[-4]

    @staticmethod
    def get_db_name(input_string):
        parts = input_string.split('.')
        return parts[-3]

    @staticmethod
    def get_schema(input_string):
        parts = input_string.split('.')
        return parts[-2]

    @staticmethod
    def get_table_name(input_string):
        parts = input_string.split('.')
        return parts[-1]

    @staticmethod
    def get_unique_owner_name(input_string):
        # DB명과 스키마를 추출하여 결합
        parts = input_string.split('.')
        return f"{parts[-3]}.{parts[-2]}".upper()

    @staticmethod
    def get_unique_owner_tgt_srv_id(input_string):
        # 마지막 부분(테이블명) 제외하고 다시 합치기
        parts = input_string.split('.')
        result = '.'.join(parts[:-1])

        # result가 'NA.'으로 시작하면 [owner_undefined] 반환
        return "[owner_undefined]" if result.startswith("na.") else result.upper()


def get_system_biz_id(props):
    # If props is None, return "[owner_undefined]"
    if props is None:
        return "[owner_undefined]"

    aspect = props.get("aspect")
    if aspect is None:
        return "[owner_undefined]"

    dataset_properties = aspect.get("datasetProperties")
    if dataset_properties is None:
        return "[owner_undefined]"

    custom_properties = dataset_properties.get("customProperties")
    if custom_properties is None:
        return "[owner_undefined]"

    system_biz_id = custom_properties.get("system_biz_id")
    if system_biz_id is None:
        return "[owner_undefined]"

    return system_biz_id


def get_system_tgt_srv_id(props):
    # If props is None, return "[owner_undefined]"
    if props is None:
        return "NA"

    aspect = props.get("aspect")
    if aspect is None:
        return "NA"

    dataset_properties = aspect.get("datasetProperties")
    if dataset_properties is None:
        return "NA"

    custom_properties = dataset_properties.get("customProperties")
    if custom_properties is None:
        return "NA"

    tgt_srv_id = custom_properties.get("tgt_srv_id")
    if tgt_srv_id is None:
        return "NA"

    return tgt_srv_id


def get_owner_srv_id(props):
    # If props is None, return "[owner_undefined]"
    if props is None:
        return "[owner_undefined]"

    aspect = props.get("aspect")
    if aspect is None:
        return "[owner_undefined]"

    dataset_properties = aspect.get("datasetProperties")
    if dataset_properties is None:
        return "[owner_undefined]"

    custom_properties = dataset_properties.get("customProperties")
    if custom_properties is None:
        return "[owner_undefined]"

    owner_srv_id = custom_properties.get("owner_srv_id")
    if owner_srv_id is None:
        return "[owner_undefined]"

    return owner_srv_id


def get_system_id(props):
    # If props is None, return "[owner_undefined]"
    if props is None:
        return "[owner"

    aspect = props.get("aspect")
    if aspect is None:
        return "[owner"

    dataset_properties = aspect.get("datasetProperties")
    if dataset_properties is None:
        return "[owner"

    custom_properties = dataset_properties.get("customProperties")
    if custom_properties is None:
        return "[owner"

    system_id = custom_properties.get("system_id")
    if system_id is None:
        return "[owner"

    return system_id


def get_biz_id(props):
    # If props is None, return "[owner_undefined]"
    if props is None:
        return "undefined"

    aspect = props.get("aspect")
    if aspect is None:
        return "undefined"

    dataset_properties = aspect.get("datasetProperties")
    if dataset_properties is None:
        return "undefined"

    custom_properties = dataset_properties.get("customProperties")
    if custom_properties is None:
        return "undefined"

    biz_id = custom_properties.get("biz_id")
    if biz_id is None:
        return "undefined"

    return biz_id


def get_db_name(table_urn):
    dataset_urn = DatasetUrn.from_string(table_urn)
    table_content = dataset_urn.get_dataset_name()
    return NameUtil.get_db_name(table_content).upper()


def get_schema_name(table_urn):
    dataset_urn = DatasetUrn.from_string(table_urn)
    table_content = dataset_urn.get_dataset_name()
    return NameUtil.get_schema(table_content).upper()


def get_sql_obj_type(table_name):
    # 입력 문자열을 소문자로 변환
    table_name = table_name.lower()

    if table_name.startswith('s3://'):
        return 'fil'

    if (table_name.endswith('_temp') or
            table_name.endswith('_tmp') or
            table_name.endswith('_stage')):
        return '$tb'
    return 'tbl'


def extract_db_info(service_xml_path, security_properties_path):
    """
    Extract PostgreSQL connection information from XML file and security properties.
    :param service_xml_path: A string representation of the file path to the service XML file.
    :param security_properties_path: A string representation of the file path to the security properties file.
    :return: A tuple containing host_port, database, username, password, or None if required information is not found.
    """
    try:
        # Parse the XML file
        tree = ET.parse(service_xml_path)
        root = tree.getroot()
        # Find the ResourceParams tag
        resource_params = root.find('.//ResourceParams[@type="jdbc"]')
        if resource_params is None:
            raise ValueError("ResourceParams tag not found in the XML file")

        # Extract URL
        url_element = resource_params.find('url')
        if url_element is None or not url_element.text:
            raise ValueError("URL not found in the XML file")

        jdbc_url = url_element.text
        # Ensure it's a PostgreSQL URL
        if not jdbc_url.startswith('jdbc:postgresql://'):
            raise ValueError("The JDBC URL is not for PostgreSQL")

        # Extract host, port, and database from JDBC URL
        _, _, host_port_db = jdbc_url.partition('://')
        host_port, _, database = host_port_db.partition('/')

        # Get decrypted credentials
        username, password = DecryptFile.get_decrypted_credentials(security_properties_path)
        if not username or not password:
            raise ValueError("Failed to decrypt database credentials")

        return host_port, database, username, password

    except ET.ParseError:
        raise ValueError("Invalid XML file")
    except Exception as e:
        raise ValueError(f"Error extracting DB info: {str(e)}")


def extract_dsn_from_xml_file(service_xml_path, security_properties_path):
    """
    Extract PostgreSQL connection information from XML file and security properties,
    and construct a SQLAlchemy DSN.
    :param service_xml_path: A string representation of the file path to the service XML file.
    :param security_properties_path: A string representation of the file path to the security properties file.
    :return: A string containing the SQLAlchemy DSN for PostgreSQL, or None if required information is not found.
    """
    try:
        host_port, database, username, password = extract_db_info(service_xml_path, security_properties_path)
        # Construct SQLAlchemy DSN
        dsn = f"postgresql://{username}:{password}@{host_port}/{database}"
        return dsn
    except ValueError as e:
        print(f"Error extracting DSN: {str(e)}")
        return None


def get_server_pid():
    """
    :return: The process ID (PID) of the server running 'async_lite_gms.py' or 'async_lite_gms', or None if no such server process is found.
    """
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        if 'python' in proc.info['name'].lower() and 'async_lite_gms.py' in ' '.join(proc.info['cmdline']):
            return proc.info['pid']
        elif 'async_lite_gms' in proc.info['name'].lower():
            return proc.info['pid']
    return None

def extract_name_from_urn(urn: str) -> str:
    # URN을 콤마로 분리
    parts = urn.split(',')

    # 데이터셋 이름 추출
    if len(parts) > 1:
        dataset_name = parts[1]
        return dataset_name
    else:
        raise ValueError("Unable to extract dataset name from URN.")


def create_default_dataset_properties(system_biz_id:str, dataset_urn:str):
    return {
        'aspect': {
            'datasetProperties': {
                'customProperties': {
                    'system_biz_id': system_biz_id,
                    'system_id': system_biz_id.split('_')[0],
                    'biz_id': system_biz_id.split('_')[1],
                    'owner_srv_id': extract_name_from_urn(dataset_urn).split('.')[0]
                },
                'description': '',
                'name': extract_name_from_urn(dataset_urn),  # URN에서 이름 추출
                'tags': []
            }
        },
        'systemMetadata': {
            'lastObserved': 0,
            'lastRunId': 'no-run-id-provided',
            'properties': {
                'clientId': 'acryl-datahub',
                'clientVersion': '1!0.0.0.dev0'
            },
            'runId': 'no-run-id-provided'
        }
    }