import pyspark
from pyspark.sql import SparkSession
import logging # log
import logging.config # busca as configurações do arquivo resources/configs/loggin.conf
import psycopg2


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
            # df.coalesce(1).write.option("header", "True").csv("transformed_retailstore")

            # Inserindo no banco de dados odados transformados
            df.write\
                .mode('append')\
                .format('jdbc')\
                .option('url', 'jdbc:postgresql://localhost:5432/futurexschema')\
                .option('dbtable', 'futures_course')\
                .option('user', 'postgres')\
                .option('password', '123')\
                .save()

            logging.info('Folder and file created' + '\n\n ---- COMPLETED SUCCESSFULLY PERSIST !!! ---- \n------------------------------------------------\n') # Captura de log - fim da classe persist_data
        except Exception as e:
            logger.error("An in occured class Persist >>> " + str(e) + '\n\n COMPLETED WITH ERROR PERSIST !!! \n------------------------------------------------\n')
            raise Exception('HDFS directory exists!!!') # Envia o erro para a classe Pipeline

    # Inserindo dados na tabela
    def insert_into_pg(self):
        logging.info('Insert data Database Postgres') 
        connection = psycopg2.connect(user='postgres', password='123', host='localhost', database='futurexschema')  
        cursor = connection.cursor()
        insert_query = "INSERT INTO public.futurex_course_catalog(course_id, course_name, author_name, course_section, creation_data) VALUES (%s, %s, %s, %s,%s);"
        insert_tuple = ('4', 'Spark e Hadoop', 'Thiago Lemes', "{}", '2021-07-01' )
        cursor.execute(insert_query, insert_tuple)
        cursor.close()
        connection.commit()


