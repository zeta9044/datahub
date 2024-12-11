import os
import logging
import sys

from datahub.ingestion.run.pipeline import Pipeline

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL 중 선택)
    format="%(asctime)s - %(levelname)s - %(message)s",  # 포맷 설정
)
logger = logging.getLogger(__name__)

def make_sqlsrc(prj_id):
    """
    :param prj_id: Project identifier used to locate specific project files in the repository.
    :return: None
    """

    try:
        logger.info(f"Starting sqlsrc creation for project {prj_id}")

        # repository path
        engine_home = os.getenv("LIAENG_HOME")
        if not engine_home:
            logger.error("Please define environment variable 'LIAENG_HOME' to your local Lia Engine home path.")
            sys.exit(1)
        base_path = os.path.join(engine_home, 'repositorys')
        if not os.path.exists(base_path):
            logger.error("Repository path does not exist.")
            sys.exit(1)
        logger.info(f"Repository base path validated: {base_path}")

        # sqlsrc.dat path
        input_file = os.path.join(base_path, str(prj_id), 'sqlsrc.dat')
        if not os.path.exists(input_file):
            logger.error("sqlsrc.dat file does not exist.")
            sys.exit(1)
        logger.info(f"Input file found: {input_file}")

        output_file = os.path.join(base_path, str(prj_id), 'sqlsrc.json')
        logger.info(f"Output will be written to: {output_file}")

        # pipeline setting
        sqlsrc_file_pipeline_config = {
            "source": {
                "type": "sqlsrc-to-json-converter",
                "config": {
                    "input_path": input_file,
                    "output_path": output_file
                }
            },
            "sink": {
                "type": "console",
            }
        }

        # run pipeline
        logger.info("Starting pipeline execution")
        pipeline = Pipeline.create(sqlsrc_file_pipeline_config)
        pipeline.run()
        pipeline.raise_from_status()
        logger.info("Pipeline execution completed successfully")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=False)
        sys.exit(1)