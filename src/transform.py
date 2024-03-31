import logging
from setLogger import setLogger
from pyspark.sql.functions import from_json, to_json, col, udf, explode, lit, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json

logger = logging.getLogger(__name__)
logger_obj = setLogger(logger, 'transform')
logger = logger_obj.set_handler()
logger.info('Class transform global attribute section')

class transform:

    def __init__(self, abs_file_path, spark):
        self.abs_file_path = abs_file_path
        logger.info(f'Transform object instantiated > {self.abs_file_path}')
        self.spark = spark

    
    def get_data_by_brand(self, brand: str):
        
        self.brand = brand
        logger.info(f'Get data by Brand {self.brand} from path {self.abs_file_path}.')
        
        self.brand_df = self.spark.read.options(Header=True).json(self.abs_file_path)
        self.brand_val_df = self.brand_df.withColumn('brand', lit(self.brand))
        
        logger.debug(f'Debugging : Schema, sample 3 records and count for brand {self.brand} file')
        logger.debug(self.brand_val_df.printSchema())
        logger.debug(self.brand_val_df.show(3,0))
        logger.debug(self.brand_val_df.count())

        return self.brand_val_df
    
    
    def transform_tempClosure_attr(self, df):
        self.df = df

        if self.brand == 'CLP' or self.brand == 'OKAY' or self.brand == 'SPAR':
            # Separating empty and non empty arrays for exploding
            logger.info('Separating empty and non empty arrays for exploding')
        
            self.tmpCl_exists = self.df.filter("cast(temporaryClosures as string) != '[]'")
            self.tmpCl_not_exists = self.df.filter("cast(temporaryClosures as string) = '[]'")
            logger.debug(f'tmpCl_exists for {self.brand} > {self.tmpCl_exists.count()}')
            logger.debug(f'tmpCl_not_exists for {self.brand} > {self.tmpCl_not_exists.count()}')

            self.tmpCl_exists_exploded = self.explode_array_attribute(self.tmpCl_exists, 'temporaryClosures')

            self.struct_temporaryClosures_attr_list = ['from', 'till']
            self.tmpCl_exists_exploded = self.extract_struct_attributes(self.tmpCl_exists_exploded, 'temporaryClosures', self.struct_temporaryClosures_attr_list)
            self.tmpCl_exists_exploded = self.tmpCl_exists_exploded.drop('temporaryClosures')
        else:
            self.tmpCl_not_exists = self.df   

        self.tmpCl_not_exists = self.tmpCl_not_exists.withColumn('temporaryClosure_from', lit('')) \
        .withColumn('temporaryClosure_till', lit('')) \
            .drop('temporaryClosures')

        if self.brand == 'CLP' or self.brand == 'OKAY' or self.brand == 'SPAR':      
            # Union exists and not exists
            self.union_df = self.tmpCl_exists_exploded.union(self.tmpCl_not_exists)
            logger.debug(self.union_df.count())
        else:
            self.union_df = self.tmpCl_not_exists
        
        return self.union_df
    
    
    def explode_array_attribute(self, df, attr):
        self.df = df
        self.attr = attr
        logger.info(f'{self.brand} > Transforming {self.attr}')
        self.exploded_df = self.df.withColumn(f'{self.attr}', explode(col(f'{self.attr}')))
        logger.debug(f'{self.brand} > Count after exploding {self.attr} > {self.exploded_df.count()}')
        return self.exploded_df
    

    def extract_struct_attributes(self, df, struct_name, struct_attr_list):
        self.df = df
        self.attr_extracted_df = self.df
        self.struct_name = struct_name
        self.struct_attr_list = struct_attr_list

        logger.info(f'{self.brand} > Extracting struct attributes for struct {self.struct_name} > {self.struct_attr_list}')
        
        for self.struct_attr in self.struct_attr_list:
            
            self.extracted_attr_name = self.struct_name + '_' + self.struct_attr
            logger.info(f'{self.brand} > {self.extracted_attr_name}')
            self.attr_extracted_df = self.attr_extracted_df.withColumn(f'{self.extracted_attr_name}', col(f'{self.struct_name}.{self.struct_attr}'))
            self.attr_extracted_df.show(2,0)
        
        return self.attr_extracted_df
    
    def add_default_string_column(self, df, attr_list):
        self.df = df
        self.attr_added_df = self.df
        self.attr_list = attr_list
        self.default_value = ''

        logger.info(f'{self.brand} > Adding default string columns {self.attr_list}')

        for self.attr in self.attr_list:
        
            self.attr_added_df = self.attr_added_df.withColumn(f'{self.attr}', lit(self.default_value))
            logger.debug(self.attr_added_df.printSchema())
        
        return self.attr_added_df
        
    
    def union_brands(clp_df, cogo_df, okay_df, spar_df):
        
        clp_cogo = clp_df.union(cogo_df)
        clp_cogo_okay = clp_cogo.union(okay_df)
        clp_cogo_okay_spar = clp_cogo_okay.union(spar_df)

        return clp_cogo_okay_spar
    
class spar:
    
    def __init__(self, brand, abs_file_path, spark):
        self.brand = brand
        self.abs_file_path = abs_file_path
        self.spark = spark
        logger.info('spar class instanstiated to modify the schema of SPAR brand dataframe')

    def organise_spar_schema(self, df):
        self.df = df
        self.df.registerTempTable('spar')
        logger.info(f'{self.brand} > organise_spar_schema')
        
        spar_select_query = """
            select
                address,
                branchId,
                commercialName,
                ensign,
                geoCoordinates,
                handoverServices,
                isActive,
                moreInfoUrl,
                placeId,
                placeSearchOpeningHours,
                placeType,
                routeUrl,
                sellingPartners,
                sourceStatus,
                brand,
                temporaryClosures_from,
                temporaryClosures_till,
                placeSearchOpeningHours_date,
                placeSearchOpeningHours_opens,
                placeSearchOpeningHours_closes,
                placeSearchOpeningHours_isToday,
                placeSearchOpeningHours_isOpenForTheDay
            from spar
        """

        self.organised_schema_df = self.spark.sql(spar_select_query)
        logger.info(type(self.organised_schema_df))
        logger.info(self.organised_schema_df.count())
        return self.organised_schema_df