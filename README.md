# FMCG_Analysis

Create a single object (dataframe) that:                   Done
Contains data from all brands                              Done
Not every brand has the same columns!                     Done
Drop placeSearchOpeningHours                             Done
You can keep sellingPartners as an array                          Exploded it 
Extract "postal_code" from address                               Done
Create new column "province" derived from postal_code        Done
Transform geoCoordinates into lat and lon column              Done
One-hot-encode the handoverServices
Pretend houseNumber and streetName are GDPR sensitive.
How would you anonymize this data for unauthorized users?
    (optional) Implement the above
How would you show the real data to authorized users?
    (optional) Implement the above
Save the end result as a parquet file                       Done
    (optional)partitioning?                                 Done