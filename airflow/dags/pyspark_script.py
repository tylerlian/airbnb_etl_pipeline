import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--project_id", required=True, type=str)
parser.add_argument("--bq_table_input", required=True, type=str)
parser.add_argument("--bucket", required=True, type=str)
args = parser.parse_args()

BQ_TABLE = f"{args.project_id}.airbnb_data.{args.bq_table_input}"
# BUCKET_URI = f"gs://{args.bucket}"

def main():
    spark = (SparkSession
        .builder
        .appName('transform_airbnb_data')
        .getOrCreate()
    )
    
    df = spark.read.format("bigquery").option("table", BQ_TABLE).load()

    def data_aggregation():
        expr =[F.round(F.mean("price"),2).alias("avg_price"),
        F.round(F.mean("availability_365"),2).alias("avg_availability_365")]
        return(expr)
        
    summary_df = df.groupby([F.col("neighbourhood_group"), F.col("neighbourhood"), F.col("room_type")]).agg(*(data_aggregation()))

    summary_df.write.format("bigquery").option("table", f"{args.project_id}.airbnb_data.airbnb_agg").option("temporaryGcsBucket", args.bucket).mode("overwrite").save()
    
    
if __name__ == "__main__":
    main()
  
  