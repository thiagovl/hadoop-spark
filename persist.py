import pyspark
from pyspark.sql import SparkSession
import logging # log
import logging.config # busca as configurações do arquivo resources/configs/loggin.conf
import sys
class Persist:

    # Especifica qual log irá aparacer onde estiver inserido logging.info('Method run_pipeline started')
    logging.config.fileConfig('resources/configs/loggin.conf') 

    def __init__(self, spark):
        self.spark = spark

    def persist_data(self, df):

        try:
            # Captura de logs - inicio da classe tranform_data
            logger = logging.getLogger("Persist")
            logging.info('Persisting') 
            # coalesce(1) - quantidade de arquivos dividido
            # .csv("transformed_retailstore") - nome do diretorio que o Hadoop irá ler os dados transformados
            df.coalesce(1).write.option("header", "True").csv("transformed_retailstore")
            logging.info('Folder and file created' + '\n\n ---- COMPLETED SUCCESSFULLY PERSIST !!! ---- \n------------------------------------------------\n') # Captura de log - fim da classe persist_data
        except Exception as e:
            logger.error("An in occured class Persist >>> " + str(e) + '\n\n COMPLETED WITH ERROR PERSIST !!! \n------------------------------------------------\n')
            raise Exception('HDFS directory exists!!!') # Envia o erro para a classe Pipeline



