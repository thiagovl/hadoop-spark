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
            # df1 = df.na.drop() # Retira as linhas vazias
            df1 = df.na.fill('Unknown', ['author_name']) # Irá substituir os valores nulos na coluna author_name por Unknown
            df2 = df1.na.fill('0', ['no_of_reviews']) # Irá substituir os valores nulos na coluna no_of_reviews por 0
            logging.info('Method tranform_data finished...') # Captura de log - fim da classe tranform_data
            df2.orderBy("course_id").show()
            print(' ---- COMPLETED SUCCESSFULLY TRANSFORM !!! ---- \n -----------------------------------------------------------------------\n')
        except Exception as e:
            logger.error("An in occured class Transform >>> " + str(e) + '\n  ---- END ----  \n------------------------------------------------ \n')
            raise Exception('An error class Transform!!!') # Envia o erro para a classe Pipeline
        return df2
