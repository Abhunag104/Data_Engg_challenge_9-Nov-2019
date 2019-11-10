import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window


def event_filter(spark, input_path):
    event_df = spark.read.format("json").load(input_path + "/*/*.txt")
    filtered_df = event_df.filter(f.col('event').
                                  isin(['update', 'register', 'deregister']))
    co_ordinates_df = filtered_df.select(f.col('at').alias('time'),
                                         f.col('data.id').alias('id'),
                                         f.struct(f.col('data.location.lat').alias('lat'),
                                                  f.col('data.location.lng').alias('lng'))
                                         .alias('coordinate'))

    return co_ordinates_df


def average_distance(co_ordinates_df):

    # sorting done on basis of time provided and related lat, lng captured
    window = Window.partitionBy(f.col('id')).orderBy(f.col('time'))
    sorted_df = co_ordinates_df.withColumn('lat_lng_list',
                                           f.collect_list("coordinate").over(window))
    sorted_df = sorted_df.groupBy(f.col('id')) \
        .agg(f.max('lat_lng_list').alias('lat_lng_list'))

    from pyspark.sql.functions import udf

    # Udf is used to pass list of lat, lng values and get average distance
    avg_udf = udf(calculate_average)

    result_df = sorted_df.withColumn('avg_dist',
                                     avg_udf(f.column('lat_lng_list')))

    result_df = result_df.select('id', 'avg_dist')

    return result_df


def _write_results(result_df, output_path):

    df = (result_df.repartition(1)
          .write.mode('overwrite')
          .format('csv'))

    df.save(output_path)


# calculation of distance by Latitide,Longitude in km
def calculate_average(lat_lng_list):

    my_list = []
    for i in lat_lng_list:
        my_list.append(tuple(i))

    import geopy.distance
    avg_dist = []
    for dist_pair in range(0, len(my_list) - 1):
        if all(my_list[dist_pair]):  # avoid empty tuple
            co_ordinate_1 = my_list[dist_pair]
            co_ordinate_2 = my_list[dist_pair + 1]

            avg_dist.append(geopy.distance.vincenty(co_ordinate_1,
                                                    co_ordinate_2).km)

    return sum(avg_dist)/len(avg_dist)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Spark job to calculate average distance of sensor data'
    )

    parser.add_argument(
        '--input-path', action='store', type=str,
        dest='input_path', required=True,
        help='input path for the data'
    )

    parser.add_argument(
        '--output-path', action='store', type=str,
        dest='output_path', required=True,
        help='output path for the clean data'
    )

    args = parser.parse_args()
    app_name = 'door2door Average calculation job'
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    event_df = event_filter(spark, input_path=args.input_path)
    average_df = average_distance(co_ordinates_df=event_df)
    _write_results(result_df=average_df, output_path=args.output_path)
