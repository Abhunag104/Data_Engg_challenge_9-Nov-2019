from pyspark.sql import SparkSession, DataFrame ,Row
from spark import average_distance_calculator


INPUT_PATH = "/home/cloudera/Downloads/door2door/input/data/resources/2019/test_data_dir/"


sparkS = SparkSession.builder \
        .appName("test_application") \
        .getOrCreate()


def test_calculate_average():
    input_co_ordinates = []
    input_co_ordinates.append([52.2296756, 21.0122287])
    input_co_ordinates.append([52.406374, 16.9251681])
    result = 279.35290160386563
    assert result == average_distance_calculator.calculate_average(input_co_ordinates)


def test_average_distance():
    event_filter_df = average_distance_calculator.event_filter(sparkS, INPUT_PATH)
    result_df = average_distance_calculator.average_distance(event_filter_df)
    assert type(result_df) == DataFrame
    assert result_df.count() == 1
