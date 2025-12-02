from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Materialized View from Source to Bronze
@dp.table(
    comment='Sales Bronze Table',
    name='sdp.bronze.sales_order'
)
def bronze_stores():
    return (
        spark.readStream
        .table('sdp.source.sales')
        .withColumn('read_ts', current_timestamp())
    )