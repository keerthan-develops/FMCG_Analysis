FMCG Analysis

Design and Folder Structure
 
FMCG_ANALYSIS
    data -> Contains the input data (json files from all brands)
    logs -> Contains the log file (assignment.log)
    publish ->  Contains
                    1. Transformed/Output dataset is loaded into this directory.
                    2. Encryption Keys for PII data stored in this directory.
    src     -> Contains the source code, main file and all the dependent py files.
    pipfile.txt
    README.md 
    run_script.sh -> Command for running the spark job


Outcome of this Assignment is listed as follows,

    Create a single object (dataframe) that:                  
    Contains data from all brands                             
    Not every brand has the same columns!                           
    Drop placeSearchOpeningHours                                    
    You can keep sellingPartners as an array                         
    Extract "postal_code" from address                              
    Create new column "province" derived from postal_code       
    Transform geoCoordinates into lat and lon column             
    One-hot-encode the handoverServices
    Pretend houseNumber and streetName are GDPR sensitive.       
    How would you anonymize this data for unauthorized users?
        (optional) Implement the above
    How would you show the real data to authorized users?
        (optional) Implement the above
    Save the end result as a parquet file                      
        (optional)partitioning?                                