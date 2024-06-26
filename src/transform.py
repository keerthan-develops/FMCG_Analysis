import logging
from setLogger import setLogger
from pyspark.sql.functions import from_json, to_json, col, udf, explode, lit, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
import json

logger = logging.getLogger(__name__)
logger_obj = setLogger(logger, 'transform')
logger = logger_obj.set_handler()

class brands:

    def __init__(self, abs_file_path, spark, brand: str):
        self.abs_file_path = abs_file_path
        logger.info(f'Transform object instantiated > {self.abs_file_path}')
        self.spark = spark
        self.brand = brand

    
    def get_data_by_brand(self):
        
        logger.info(f'Get data by Brand {self.brand} from path {self.abs_file_path}.')
        
        self.brand_df = self.spark.read.options(Header=True).json(self.abs_file_path)
        self.brand_val_df = self.brand_df.withColumn('brand', lit(self.brand))
        
        logger.debug(f'Debugging : Schema, sample 3 records and count for brand {self.brand} file')
        #logger.debug(self.brand_val_df.printSchema())

        return self.brand_val_df
    
    
    def transform_tempClosure_attr(self, df):
        self.df = df
        logger.info(f'Transforming attribute : tempClosure for brand > {self.brand}')

        if self.brand == 'CLP' or self.brand == 'OKAY' or self.brand == 'SPAR' or self.brand == 'DATS':
            # Separating empty and non empty arrays for exploding
            logger.debug('Separating empty and non empty arrays for exploding')
        
            self.tmpCl_exists = self.df.filter("cast(temporaryClosures as string) != '[]'")
            self.tmpCl_not_exists = self.df.filter("cast(temporaryClosures as string) = '[]'")
            logger.debug(f'tmpCl_exists for {self.brand} > {self.tmpCl_exists.count()}')
            logger.debug(f'tmpCl_not_exists for {self.brand} > {self.tmpCl_not_exists.count()}')

            if self.tmpCl_exists.count() > 0:
                self.tmpCl_exists_exploded = self.explode_array_attribute(self.tmpCl_exists, 'temporaryClosures')

                self.struct_temporaryClosures_attr_list = ['from', 'till']
                self.tmpCl_exists_exploded = self.extract_struct_attributes(self.tmpCl_exists_exploded, 'temporaryClosures', self.struct_temporaryClosures_attr_list)
                self.tmpCl_exists_exploded = self.tmpCl_exists_exploded.drop('temporaryClosures')
        else:
            self.tmpCl_not_exists = self.df   

        self.tmpCl_not_exists = self.tmpCl_not_exists.withColumn('temporaryClosures_from', lit('')) \
        .withColumn('temporaryClosures_till', lit('')) \
            .drop('temporaryClosures')

        if self.brand == 'CLP' or self.brand == 'OKAY' or self.brand == 'SPAR' or self.brand == 'DATS':      
            # Union exists and not exists
            if self.tmpCl_exists.count() > 0:
                self.union_df = self.tmpCl_exists_exploded.union(self.tmpCl_not_exists)
                logger.debug(self.union_df.count())
            else:
                logger.debug(f'{self.brand} > empty array so not exploded')
                self.union_df = self.tmpCl_not_exists
                logger.debug(self.union_df.count())
        else:
            self.union_df = self.tmpCl_not_exists
        
        return self.union_df
    
    
    def explode_array_attribute(self, df, attr):
        self.df = df
        self.attr = attr
        logger.info(f'{self.brand} > Attempting to explode_array_attribute {self.attr}')
        self.exploded_df = self.df.withColumn(f'{self.attr}', explode(col(f'{self.attr}')))
        logger.debug(f'{self.brand} > Count after exploding {self.attr} > {self.exploded_df.count()}')
        try:    
            self.exploded_filtered_df = self.exploded_df.filter(f" trim({self.attr}) != '' ").dropDuplicates()
        except:
            self.exploded_filtered_df = self.exploded_df.dropDuplicates()

        logger.debug(f'{self.brand} > Count after filtering and dropping duplicates {self.attr} > {self.exploded_df.count()}')
        return self.exploded_filtered_df
    

    def extract_struct_attributes(self, df, struct_name, struct_attr_list):
        self.df = df
        self.attr_extracted_df = self.df
        self.struct_name = struct_name
        self.struct_attr_list = struct_attr_list

        logger.debug(f'{self.brand} > Extracting struct attributes for struct {self.struct_name} > {self.struct_attr_list}')
        
        for self.struct_attr in self.struct_attr_list:
            
            self.extracted_attr_name = self.struct_name + '_' + self.struct_attr
            logger.debug(f'{self.brand} > {self.extracted_attr_name}')
            self.attr_extracted_df = self.attr_extracted_df.withColumn(f'{self.extracted_attr_name}', col(f'{self.struct_name}.{self.struct_attr}'))
            #self.attr_extracted_df.show(2,0)
        
        return self.attr_extracted_df
    
    def add_default_string_column(self, df, attr_list):
        self.df = df
        self.attr_added_df = self.df
        self.attr_list = attr_list
        self.default_value = ''

        logger.info(f'{self.brand} > Adding default string columns {self.attr_list}')

        for self.attr in self.attr_list:
        
            self.attr_added_df = self.attr_added_df.withColumn(f'{self.attr}', lit(self.default_value))
            #logger.debug(self.attr_added_df.printSchema())
        
        return self.attr_added_df
    
    def drop_columns(self, df, attr_tuple):
        self.df = df
        self.attr_dropped_df = self.df
        self.attr_tuple = attr_tuple

        logger.info(f'{self.brand} > Dropping columns > {self.attr_tuple}')
        
        try:
            logger.debug(f'{self.brand} > In the try section to drop columns')
            self.attr_dropped_df = self.attr_dropped_df.drop(self.attr_tuple)
            #logger.debug(self.attr_dropped_df.printSchema())
        except:
            logger.debug(f'{self.brand} > In the except section to drop columns')
            for self.attr in self.attr_tuple:
                self.attr_dropped_df = self.attr_dropped_df.drop(f'{self.attr}')
                #logger.debug(self.attr_dropped_df.printSchema())
        
        
        return self.attr_dropped_df
        
    
    def union_brands(clp_df, cogo_df, okay_df, spar_df, dats_df):
        
        logger.info(f'union data from all brands')

        clp_cogo = clp_df.union(cogo_df)
        clp_cogo_okay = clp_cogo.union(okay_df)
        clp_cogo_okay_spar = clp_cogo_okay.union(spar_df)
        clp_cogo_okay_spar_dats = clp_cogo_okay_spar.union(dats_df)

        return clp_cogo_okay_spar_dats
    

    def organise_schema(self, df):
        self.df = df
        self.df.createOrReplaceTempView(f'{self.brand}')
        logger.debug(f'{self.brand} > organise_schema to keep in sync with other brands')
        
        select_query = f"""
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
            from {self.brand}
        """

        self.organised_schema_df = self.spark.sql(select_query)
        logger.debug(f'{self.brand} > Schema Check')
        logger.debug(self.organised_schema_df.count())
        return self.organised_schema_df
    

class datasetTransform:
    
    def __init__(self, spark):
        spark = spark
        logger.info('datasetTransform class is initiated')

    @staticmethod
    def extract_postal_code(df):
        logger.info('extract_postal_code from address function called')
        df = df
        postal_cd_df = df.withColumn('postalcode', col('address.postalcode'))
        postal_cd_df.count()
        return postal_cd_df

    @staticmethod
    def get_province_from_postal_config(df):
        df = df
        logger.info(f'get_province_from_postal_config function called.')
        
        def derive_province(postal_val):
            logger.info(f'derive_province function called for postalcode : {postal_val}')
            prov_post_dict = {    "Brussel": ["1000-1299"],
                                    "Waals-Brabant": ["1300-1499"],
                                    "Vlaams-Brabant": ["1500-1999","3000-3499"],
                                    "Antwerpen": ["2000-2999"],
                                    "Limburg": ["3500-3999"],
                                    "Luik": ["4000-4999"],
                                    "Namen": ["5000-5999"],
                                    "Henegouwen": ["6000-6599","7000-7999"],
                                    "Luxemburg": ["6600-6999"],
                                    "West-Vlaanderen": ["8000-8999"],
                                    "Oost-Vlaanderen": ["9000-9999"]
                                }
            for k,v in prov_post_dict.items():
                logger.debug(f'key -> {k}')
                logger.debug(f'value -> {v}')
                #logger.info(type(v))
                for i in v:
                    lower_end = int(i.split('-')[0])
                    upper_end = int(i.split('-')[1])
                    logger.debug(f'lower_end -> {lower_end}')
                    logger.debug(f'upper_end -> {upper_end}')
                    
                    if int(postal_val) in range(lower_end, upper_end):
                        logger.debug('Matched')
                        return str(k)
            return ' '
        
        derive_province_udf = udf(lambda x : derive_province(x), StringType())

        province_df = df.withColumn('province', derive_province_udf(col('postalcode')))
        return province_df
    
    @staticmethod
    def extract_lat_long(df):
        logger.info(f'Extract latitude and longitude.')
        df = df
        lat_long_df = df.withColumn('latitude', col('geoCoordinates.latitude')) \
                                    .withColumn('longitude', col('geoCoordinates.longitude'))
        return lat_long_df
    
    @staticmethod
    def one_hot_encode(df, attr):
        
        logger.info(f'Attempting one_hot_encode on {attr}')

        try:    
            df_filtered = df.filter(f" trim({attr}) != '' ").dropDuplicates()
        except:
            df_filtered = df.dropDuplicates()
        
        indexer = StringIndexer(inputCol=f'{attr}', outputCol=f'{attr}_numeric')
        indexer_fitted = indexer.fit(df_filtered)
        df_indexed = indexer_fitted.transform(df_filtered)

        encoder = OneHotEncoder(inputCols=[f'{attr}_numeric'], outputCols=[f'{attr}_onehot'])
        df_onehot = encoder.fit(df_indexed).transform(df_indexed)
        #df_onehot.show()
        df_onehot_str = df_onehot.withColumn(f'{attr}_numeric', col(f'{attr}_numeric').cast(StringType())) \
            .withColumn(f'{attr}_onehot', col(f'{attr}_onehot').cast(StringType()))

        # Merge filtered rows into onehot encode datafraame
        df_null_hot = df.withColumn(f'{attr}_numeric', lit('')).withColumn(f'{attr}_onehot', lit(''))
        df_oneHot_union = df_onehot_str.union(df_null_hot)

        logger.info(f'one_hot_encode on {attr} completed')
        return df_oneHot_union
                
        

