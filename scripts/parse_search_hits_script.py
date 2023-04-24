import json
import sys
import logging
import boto3
import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, struct
from pyspark.sql import functions as F
from urllib.parse import parse_qs, urlparse

# read input parameters

source_bucket = "shopzilla-search-engine-hits"
file_name = "data.tsv"
s3_prefix = "stage_output"

today = datetime.date.today()
target_file_name = f"{today.strftime('%Y-%m-%d')}_SearchKeywordPerformance.tab"

# Initialize the Gluecontext and Sparkcontext for the Job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.caseSensitive", "true")
job = Job(glueContext)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def get_input_as_df():
    input_file_path = "s3://shopzilla-search-engine-hits/data.tsv"
    # reading input tsv file as dataframe
    input_df = spark.read.csv(input_file_path, sep=r'\t', header=True)
    return input_df

# Parse a URL into 6 components: <scheme>://<netloc>/<path>;<params>?<query>#<fragment>
# Scalable - Possible query terms like "query", ...
# converting the result search terms into case-insensitive
def parse_url(url):
    parsed_url = urlparse(url)
    query = parse_qs(parsed_url.query)
    search_query = query.get('p', query.get('q'))[0]
    return search_query.lower()

def get_file_name(bucket):
    file_names = []
    s3_objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=s3_prefix)
    for item in s3_objects['Contents']:
        file = item['Key']
        file_names.append(file)
    print(f"file_names: {file_names}")
    logging.info(f"output file name: {file_names[0].split('/')[1]}")
    return file_names[0].split("/")[1]

def write_to_tsv(df):
    df.coalesce(1).write.option("delimiter", "\t").mode("overwrite").csv("s3://shopzilla-search-engine-hits/stage_output/", header = 'true')
    
def rename_output_s3():
    # rename file in s3
    file_key = get_file_name(source_bucket)
    s3_client.copy_object(Bucket=f"{source_bucket}", CopySource=f"{source_bucket}/{s3_prefix}/{file_key}", Key=target_file_name)
    

def main():
    input_df = get_input_as_df()

    # filtering input_df with only columns that are required
    filtered_df = input_df.select("ip", "referrer", F.split(
        "product_list", ";").alias("product_list"))

    ip_with_nonnull_rev = filtered_df.select("ip", filtered_df.product_list[3].alias("total_revenue"))\
        .filter(filtered_df.product_list[3] != '')

    # adding new columns to DF
    filtered_df = filtered_df.withColumn("search_engine", F.split(F.regexp_replace(F.split(F.col("referrer"), "\.")[1], "www", ""), "\.")[0]) \
        .withColumn("search_query", udf(parse_url)(F.col("referrer"))) \
        .select("ip", filtered_df.product_list[3].alias("total_revenue"), "search_engine", "search_query")
    # filtering rows only with search engine hits
    filtered_df = filtered_df.filter((filtered_df.search_engine == 'google') | (
        filtered_df.search_engine == 'bing') | (filtered_df.search_engine == 'yahoo'))

    # creating temporary views
    filtered_df.createOrReplaceTempView("filtered_data")
    ip_with_nonnull_rev.createOrReplaceTempView("ip_with_revenue")

    query = """
        SELECT 
            f.search_engine,
            f.search_query,
            sum(i.total_revenue) as Revenue
        FROM 
            filtered_data f
        JOIN ip_with_revenue i 
            on f.ip = i.ip 
        GROUP BY 
            f.search_engine, f.search_query
        ORDER BY
            Revenue desc
    """
    # running sql query
    result_df = spark.sql(query)
    # renaming columns as per target requirement
    result_df = result_df.select(result_df.search_engine.alias(
        "Search Engine Domain"), result_df.search_query.alias("Search Keyword"), "Revenue")
    # writing output df to tsv in s3
    write_to_tsv(result_df)
    # renameing output file to required file naming format
    rename_output_s3()

if __name__ == "__main__":
    main()
