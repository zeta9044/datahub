import os
from zeta_lab.utilities.common_logger import setup_logging, cleanup_logger
from datahub.ingestion.run.pipeline import Pipeline


def make_sqlsrc(prj_id, log_file=None):
    """
    :param prj_id: Project identifier used to locate specific project files in the repository.
    :param log_file: Optional log file path. Defaults to {prj_id}_run.out if not specified.
    :return: None
    """
    log_file = log_file if log_file else f"{prj_id}_run.out"
    logger = setup_logging(log_file, __name__)

    try:
        logger.info(f"Starting sqlsrc creation for project {prj_id}")

        # repository path
        engine_home = os.getenv("LIAENG_HOME")
        if not engine_home:
            raise ValueError("Please define environment variable 'LIAENG_HOME' to your local Lia Engine home path.")
        base_path = os.path.join(engine_home,'repositorys')
        if not os.path.exists(base_path):
            raise ValueError("Repository path does not exist.")
        logger.info(f"Repository base path validated: {base_path}")

        # sqlsrc.dat path
        input_file = os.path.join(base_path,str(prj_id),'sqlsrc.dat')
        if not os.path.exists(input_file):
            raise ValueError("sqlsrc.dat file does not exist.")
        logger.info(f"Input file found: {input_file}")

        output_file = os.path.join(base_path,str(prj_id),'sqlsrc.json')
        logger.info(f"Output will be written to: {output_file}")

        # pipeline setting
        sqlsrc_file_pipeline_config = {
            "source": {
                "type": "sqlsrc-to-json-converter",
                "config": {
                    "input_path" : input_file,
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
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        raise
    finally:
        cleanup_logger(logger)

if __name__ == "__main__":
    # example: prj_id = '21'
    prj_id = 21
    try:
        make_sqlsrc(prj_id=prj_id)
    except Exception as e:
        print(e)