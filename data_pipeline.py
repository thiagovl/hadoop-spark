import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

import ingest
import transform
import persist
import logging # log
import logging.config # busca as configurações do arquivo resources/configs/loggin.conf
import sys
import transform

class Pipeline:

    # Especifica qual log irá aparacer onde estiver inserido logging.info('Method run_pipeline started')
    logging.config.fileConfig('resources/configs/loggin.conf') 

    def run_pipeline(self):
        
        try:
            print('\n')
            logger = logging.getLogger("Run Pipeline...\n")
            logging.info('Pipeline') # Captura de log - inicio da classe run_pipeline
            logging.info('Method run_pipeline started...\n') # Captura de log - inicio da classe run_pipeline

            # Classe Ingest - ingestão de dados - captura de dados.
            ingest_process = ingest.Ingest(self.spark)
            ingest_df = ingest_process.ingest_data()
            # ingest_df.show()

            # Classe Transform - transformação dos dados
            transform_process = transform.Transform(self.spark)
            transform_df = transform_process.tranform_data(ingest_df)

            persist_process = persist.Persist(self.spark)
            persist_process.persist_data(transform_df)

            logging.info(' Method run_pipeline finished... \n COMPLETED SUCCESSFULLY PIPELINE !!! \n------------------------------------------------ \n') # Captura de log - fim da classe run_pipeline
            sys.exit(0)
        except Exception as e:
            logger.error("An in occured class Pipeline >>> " + str(e) + '\n COMPLETED WITH ERROR PIPELINE !!! \n------------------------------------------------ \n')
            sys.exit(1)
        return

    # Cria a sessão do Spark, ou seja, é a porta de entrada para os comandos no Spark
    def create_spark_session(self):
        self.spark = SparkSession.builder.appName('Meu app spark')\
                            .enableHiveSupport()\
                            .getOrCreate()

    # Criando o banco de dados no Hive
    def create_hive_table(self):
        self.spark.sql('create database if not exists fxxcoursedb')  # Cria o banco de dados fxxcoursedb no Hive
        
        # Criando a tabela fx_course_table
        self.spark.sql("create table if not exists "  
                       "fxxcoursedb.fx_course_table "
                       "(course_id string, course_name string, author_name string, no_of_reviews string)")

        # Inserindo dados na tabela fx_course_table
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(1, 'Java', 'Thiago Lemes', '')")
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(2, 'Python', 'Thiago Lemes', 65)")
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(3, 'Spring Boot', 'Thiago Lemes', '')")
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(4, 'PHP', 'Carina', 25)")
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(5, 'CMS', '', 11)")
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(6, 'Ansible', 'Spack', 35)")
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(7, 'Go Lang', '', 98)")
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(8, 'Chef', 'Carina', 47)")
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(9, 'Dot net', 'Rosimar', 123)")
        # self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES(10, 'Jekins', 'Thiago Lemes', 541)")

        # # Trata string vazias como nulas
        # self.spark.sql("alter table fxxcoursedb.fx_course_table set tblproperties('serialization.null.format'='')")

if __name__ == '__main__':
    
    pipeline = Pipeline()
    logging.info('Application starded...')
    pipeline.create_spark_session() 
    pipeline.create_hive_table()
    logging.info('Spark Session created...')
    pipeline.run_pipeline()