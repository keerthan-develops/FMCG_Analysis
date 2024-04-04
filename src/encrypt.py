import logging
from setLogger import setLogger
from pathlib import Path
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, udf, explode, lit, coalesce, sha2
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

logger = logging.getLogger(__name__)
logger_obj = setLogger(logger, 'encrypt')
logger = logger_obj.set_handler()
logger.debug('Module encrypt')

def generateEncryptionKeys(spark, df, publish_df_path, publish_encrypt_path):
    logger.info(f'Generate encryption key')
    
    def generate_encrypt_key():
        logger.info(f'generate_encrypt_key function called')
        from cryptography.fernet import Fernet
        key = Fernet.generate_key()
        return key.decode("utf-8")
    
        #spark.udf.register("decrypt_val", decrypt_val)
    
    df.createOrReplaceTempView('df')

    generate_key_using_Fernet = udf(generate_encrypt_key, StringType())
    
    df_distinct_record = spark.sql('''select distinct placeId from df''')
    df_distinct_record.count()
    df_distinct_record = df_distinct_record.withColumn("encryption_key", lit(generate_key_using_Fernet()))
    df_distinct_record.write.option('header', 'True').mode('overwrite').csv(publish_encrypt_path)
    logger.info(f'Encryption key dataset written to disk')

def encrypt(spark, publish_encrypt_path):

    logger.info(f'Perform Encryption on address attributes')
    
    encrypr_df = spark.read.option('InferSchema', 'True')\
        .option('header', 'True')\
        .csv(publish_encrypt_path)
    encrypr_df.createOrReplaceTempView('encryption_keys')


    # Define Encrypt User Defined Function 
    
    def encrypt_val(clear_text,MASTER_KEY):
        from cryptography.fernet import Fernet
        f = Fernet(MASTER_KEY)
        clear_text_b=bytes(clear_text, 'utf-8')
        cipher_text = f.encrypt(clear_text_b)
        cipher_text = str(cipher_text.decode('ascii'))
        return cipher_text

    # Register UDF's
    encrypt = udf(encrypt_val, StringType())

    logger.info('Encryption df join with main df')

    # Encrypt the data 
    place_enck_join_df = spark.sql('''select a.*, e.encryption_key 
                                        from df as a 
                                    inner join encryption_keys as e 
                                    on e.placeId = a.placeId''')
    
    logger.info('Final Encryption on columns')

    masked_df = place_enck_join_df.withColumn('address', col('address').withField('houseNumber', encrypt("address.houseNumber", col("encryption_Key"))))\
                     .withColumn('address', col('address').withField('streetName', encrypt("address.streetName", col("encryption_Key"))))\
                     .drop("encryption_Key")
    
    logger.info('Final Encryption transform finish')

    return masked_df

