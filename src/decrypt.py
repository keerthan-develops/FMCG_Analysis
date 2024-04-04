import logging
from setLogger import setLogger
from pathlib import Path
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, udf, explode, lit, coalesce, sha2
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

logger = logging.getLogger(__name__)
logger_obj = setLogger(logger, 'decrypt')
logger = logger_obj.set_handler()
logger.debug('Module decrypt')

def decrypt(spark, publish_df_path, publish_encrypt_path):

    masked_df = spark.read.parquet(publish_df_path)
    logger.info(masked_df.count())
    masked_df.createOrReplaceTempView('masked_df')
    
    encrypr_df = spark.read.option('InferSchema', 'True')\
        .option('header', 'True')\
        .csv(publish_encrypt_path)
    encrypr_df.show(2,0)
    encrypr_df.createOrReplaceTempView('encryption_keys')
    logger.debug(encrypr_df.printSchema())

    # Define decrypt user defined function 
    
    def decrypt_val(cipher_text,MASTER_KEY):
        from cryptography.fernet import Fernet
        f = Fernet(MASTER_KEY)
        clear_val=f.decrypt(cipher_text.encode()).decode()
        return clear_val
    
    decrypt = udf(decrypt_val, StringType())

    #Decrypt

    encrypted = spark.sql('''select a.*, e.encryption_key 
                                        from masked_df as a 
                                    inner join encryption_keys as e 
                                    on e.placeId = a.placeId
                          ''')
    
    unmasked_df = encrypted.withColumn('address', col('address').withField('houseNumber', decrypt("address.houseNumber", col("encryption_Key"))))\
        .withColumn('address', col('address').withField('streetName', decrypt("address.streetName", col("encryption_Key"))))\
        .drop("encryption_Key")

    unmasked_df.select('placeId', 'address').show(2,0)

    return unmasked_df

if __name__ == '__main__':

    pwd = Path().resolve()
        
    repo_dir = str(pwd).replace('/src', '')
    repo_dir = Path(repo_dir).resolve()

    publish_df_path = str(repo_dir / 'publish' / 'fmcg_analysis')
    logger.info(Path(publish_df_path).is_dir())

    publish_encrypt_path = str(repo_dir / 'publish' / 'encryption_keys')
    logger.info(Path(publish_encrypt_path).is_dir())

    findspark.init()

    spark = (
                SparkSession
                .builder
                .appName("decrypt")
                .master("local")
                .config("spark.sql.repl.eagerEval.enabled", True)
                .getOrCreate()
    )
    
    unmasked_df = decrypt(spark, publish_df_path, publish_encrypt_path)