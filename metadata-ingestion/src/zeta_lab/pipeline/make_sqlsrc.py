import os
from zeta_lab.source.sqlsrc_to_json_converter import SqlsrcToJSONConverter
from datahub.ingestion.run.pipeline import Pipeline

def make_sqlsrc(prj_id):
    """
    :param prj_id: Project identifier used to locate specific project files in the repository.
    :return: None
    """
    try:
        # repository path
        engine_home = os.getenv("LIAENG_HOME")
        if not engine_home:
            raise ValueError("Please define environment variable 'LIAENG_HOME' to your local Lia Engine home path.")
        base_path = os.path.join(engine_home,'repositorys')
        if not os.path.exists(base_path):
            raise ValueError("Repository path does not exist.")

        # sqlsrc.dat path
        input_file = os.path.join(base_path,str(prj_id),'sqlsrc.dat')
        if not os.path.exists(input_file):
            raise ValueError("sqlsrc.dat file does not exist.")

        output_file = os.path.join(base_path,str(prj_id),'sqlsrc.json')

        # pipeline setting
        sqlsrc_file_pipeline_config = {
            "source": {
                "type": "sqlsrc_to_json_converter",
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
        pipeline = Pipeline.create(sqlsrc_file_pipeline_config)
        pipeline.run()
        pipeline.raise_from_status()
    except Exception as e:
        raise

if __name__ == "__main__":
    # example: prj_id = '21'
    prj_id = 21
    try:
        make_sqlsrc(prj_id=prj_id)
    except Exception as e:
        print(e)
