import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import logging # log
import logging.config # busca as configurações do arquivo resources/configs/loggin.conf
import pandas.io.sql as sqlio
import psycopg2

class Ingest:

    # Especifica qual log irá aparacer onde estiver inserido logging.info('Method run_pipeline started')
    logging.config.fileConfig('resources/configs/loggin.conf') 

    def __init__(self, spark):
        self.spark = spark
        
    def ingest_data(self):

        try:
            logger = logging.getLogger("Ingest")
            logging.info("Ingest")
            # Captura de logs - inicio da classe tranform_data
            logging.info('Method ingest_data started...') 
            # df = self.spark.read.csv("retailstore.csv", header=True) # lendo arquivo csv, header - mostra os cabeçalhos da tabela
            df = self.spark.sql("select * from fxxcoursedb.fx_course_table") # Lendo os dados da tabela criada no metodo create_hive_table do Pipeline
            logging.info('Data Frame created...') # Captura de log - fim da classe ingest_data
            df.show()
            print(' ---- COMPLETED SUCCESSFULLY INGEST !!! ---- \n -----------------------------------------------------------------------\n')
        except Exception as e:
            logger.error("An in occured class Ingest >>> " + str(e) + '\n  ---- END ----  \n------------------------------------------------ \n')
            raise Exception('An error class Ingest!!!') # Envia o erro para a classe Pipeline
        return df

        # # Imprime todos os dados
        # print("=== Imprime todos os dados ===")
        # df.show()

        # # Imprime os valores maximo, minimo, mediana
        # print("=== Imprime o describe ===")
        # df.describe().show()

        # # Seleciona uma coluna
        # print("=== Imprime a coluna Country ===")
        # df.select("Country").show()

        # # Agrupa pela coluna Country
        # print("=== Agrupa pela coluna Country ===")
        # df.groupBy("Country").count().show()

        # # Agrupa pela coluna Gender a media de Salary e os valores maximos
        # print("=== Agrupa pela coluna Gender a media de Salary e a age(idade) maxima ===")
        # df.groupBy("Gender").agg({"Salary": "avg", "age": "max"}).show()

        # # Seleciona a coluna Salary com valores maiores que 3.000
        # print("=== Seleciona a coluna Salary com valores maiores que 3.000 ===")
        # df.filter("Salary > 30000").show()

        # # Classifica pela coluna Salary
        # print("=== Classifica em ordem crescente pela coluna Salary ===")
        # df.orderBy("Salary").show()
    
    # Criando a conexão com o banco de dados Postgres para ler dados
    def read_from_pg(self):
        connection = psycopg2.connect(user='postgres', password='123', host='localhost', database='futurexschema')  
        cursor = connection.cursor()
        sql_query = "SELECT * FROM public.futurex_course_catalog;"
        pdDF = sqlio.read_sql_query(sql_query, connection)
        sparkDF = self.spark.createDataFrame(pdDF)
        sparkDF.show()
    
    # Criando a conexão com o banco de dados Postgres com jdbc
    def read_from_pg_using_jdbc_driver(self):
        jdbcDF = self.spark.read\
            .format('jdbc')\
            .option('url', 'jdbc:postgresql://localhost:5432/futurexschema')\
            .option('dbtable', 'futurex_course_catalog')\
            .option('user', 'postgres')\
            .option('password', '123')\
            .load()
        
        jdbcDF.show()