from pathlib import Path
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, udf, explode, lit, coalesce
import logging
from setLogger import setLogger
from resolvePath import resolvePath
from transform import transform

if __name__ == "__main__":
    print('Main Function executed')

    findspark.init()

    spark = (
                SparkSession
                .builder
                .appName("cg-pyspark-assignment")
                .master("local")
                .config("spark.sql.repl.eagerEval.enabled", True)
                .getOrCreate()
    )
    
    # Create and configure logger
    logger = logging.getLogger(__name__)
    
    # Call the setLogger method to instantiate handler
    logger_obj = setLogger(logger, 'main')
    logger = logger_obj.set_handler()
    logger.info('Logger succesfully created')

    # Resolve paths
    path_obj = resolvePath()
    data_dir, clp_path, cogo_path, dats_path, okay_path, spar_path, log_path = path_obj.get_path()

    # Create a transform object and load CLP data into a dataframe
    clp_obj = transform(clp_path, spark)
    clp_df = clp_obj.get_data_by_brand('CLP')

    cogo_obj = transform(cogo_path, spark)
    cogo_df = cogo_obj.get_data_by_brand('COGO')

    dats_obj = transform(dats_path, spark)
    dats_df = dats_obj.get_data_by_brand('DATS')
    logger.info('DATS DATS DATS')
    logger.info(dats_df.printSchema())

    okay_obj = transform(okay_path, spark)
    okay_df = okay_obj.get_data_by_brand('OKAY')

    spar_obj = transform(spar_path, spark)
    spar_df = spar_obj.get_data_by_brand('SPAR')

    
    # temperoryClosures attribute transformation
    clp_df2= clp_obj.transform_tempClosure_attr(clp_df)
    logger.info(f'CLP DF2 count > {clp_df2.count()}')

    cogo_df2= cogo_obj.transform_tempClosure_attr(cogo_df)
    logger.info(f'COGO DF2 count > {cogo_df2.count()}')

    okay_df2= okay_obj.transform_tempClosure_attr(okay_df)
    logger.info(f'OKAY DF2 count > {okay_df2.count()}')

    spar_df2= spar_obj.transform_tempClosure_attr(spar_df)
    logger.info(f'SPAR DF2 count > {spar_df2.count()}')

    dats_df2= dats_obj.transform_tempClosure_attr(dats_df)
    logger.info(f'SPAR DF2 count > {dats_df2.count()}')

    
    # handoverServices attribute transformation
    clp_df3 = clp_obj.explode_array_attribute(clp_df2, 'handoverServices')
    cogo_df3 = cogo_obj.explode_array_attribute(cogo_df2, 'handoverServices')
    okay_df3 = okay_obj.explode_array_attribute(okay_df2, 'handoverServices')


    # sellingPartners attribute transformation
    clp_df4 = clp_obj.explode_array_attribute(clp_df3, 'sellingPartners')
    cogo_df4 = cogo_obj.explode_array_attribute(cogo_df3, 'sellingPartners')
    okay_df4 = okay_obj.explode_array_attribute(okay_df3, 'sellingPartners')

    # SPAR add missing columns
    spar_add_col_list = ['handoverServices', 'sellingPartners']
    spar_df3 = spar_obj.add_default_string_column(spar_df2, spar_add_col_list)

    # placeSearchOpeningHours attribute array explode
    clp_df5  = clp_obj.explode_array_attribute(clp_df4, 'placeSearchOpeningHours')
    cogo_df5 = cogo_obj.explode_array_attribute(cogo_df4, 'placeSearchOpeningHours')
    okay_df5 = okay_obj.explode_array_attribute(okay_df4, 'placeSearchOpeningHours')
    spar_df4 = spar_obj.explode_array_attribute(spar_df3, 'placeSearchOpeningHours')

    # placeSearchOpeningHours attribute struct columns extraction

    struct_placeSearchOpeningHours_attr_list = ['date', 'opens', 'closes', 'isToday', 'isOpenForTheDay']
    clp_df6 = clp_obj.extract_struct_attributes(clp_df5, 'placeSearchOpeningHours', struct_placeSearchOpeningHours_attr_list)
    cogo_df6 = cogo_obj.extract_struct_attributes(cogo_df5, 'placeSearchOpeningHours', struct_placeSearchOpeningHours_attr_list)
    okay_df6 = okay_obj.extract_struct_attributes(okay_df5, 'placeSearchOpeningHours', struct_placeSearchOpeningHours_attr_list)
    spar_df5 = spar_obj.extract_struct_attributes(spar_df4, 'placeSearchOpeningHours', struct_placeSearchOpeningHours_attr_list)


    # DATS > add all the missing columns with default values
                
    dats_missing_columns_list = ['handoverServices', 'sellingPartners', 'placeSearchOpeningHours_date', \
                                 'placeSearchOpeningHours_opens', 'placeSearchOpeningHours_opens',\
                                 'placeSearchOpeningHours_closes', 'placeSearchOpeningHours_isToday',\
                                 'placeSearchOpeningHours_isOpenForTheDay', 'placeSearchOpeningHours'\
                                 ]
    dats_df3 = dats_obj.add_default_string_column(dats_df2, dats_missing_columns_list)
    logger.info(f'DATS post adding default columns')
    logger.debug(dats_df3.printSchema())


    # Organising schema
    spar_df6 = spar_obj.organise_schema(spar_df5)
    dats_df4 = dats_obj.organise_schema(dats_df3)


    # Drop placeSearchOpeningHours 
    drop_col_list = ('placeSearchOpeningHours', 'placeSearchOpeningHours_isOpenForTheDay', 'placeSearchOpeningHours_isToday')
    clp_df7 = clp_obj.drop_columns(clp_df6, drop_col_list)
    cogo_df7 = cogo_obj.drop_columns(cogo_df6, drop_col_list)
    okay_df7 = okay_obj.drop_columns(okay_df6, drop_col_list)
    spar_df7 = spar_obj.drop_columns(spar_df6, drop_col_list)
    dats_df5 = dats_obj.drop_columns(dats_df4, drop_col_list)

    logger.debug(f'Final CLP schema for clp_df7')
    logger.debug(clp_df7.printSchema())
    logger.debug(f'Final DATS schema for dats_df5')
    logger.debug(dats_df5.printSchema())

    
    # Union of all brands
    clp_cogo_okay_spar_dats = transform.union_brands(clp_df7, cogo_df7, okay_df7, spar_df7, dats_df5)
    logger.info(f'clp_cogo_okay_spar_dats count > {clp_cogo_okay_spar_dats.count()}')
    
    
    
