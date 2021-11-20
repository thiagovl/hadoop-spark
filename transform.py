import pyspark
from pyspark.sql import SparkSession
import logging # log
import logging.config # busca as configurações do arquivo resources/configs/loggin.conf
import sys

class Transform:

    # Especifica qual log irá aparacer onde estiver inserido logging.info('Method run_pipeline started')
    logging.config.fileConfig('resources/configs/loggin.conf') 

    def __init__(self, spark):
        self.spark = spark

    def tranform_data(self, df):

        try:
            logger = logging.getLogger("Transform")
            logging.info("Transform")
            # Captura de logs - inicio da classe tranform_data
            logging.info('Method tranform_data started...')
            df1 = df.na.drop() # Retira as linhas vazias
            logging.info('Method tranform_data finished...') # Captura de log - fim da classe tranform_data
            df1.orderBy("course_id").show()
            print(' ---- COMPLETED SUCCESSFULLY TRANSFORM !!! ---- \n -----------------------------------------------------------------------\n')
        except Exception as e:
            logger.error("An in occured class Transform >>> " + str(e) + '\n  ---- END ----  \n------------------------------------------------ \n')
            raise Exception('An error class Transform!!!') # Envia o erro para a classe Pipeline
            sys.exit(1)
        return df1
