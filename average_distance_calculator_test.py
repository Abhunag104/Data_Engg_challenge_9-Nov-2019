from pyspark.sql import SparkSession, DataFrame
from spark import average_distance_calculator
import pyspark.sql.functions as f
from pyspark.sql.window import Window


INPUT_PATH = "/home/cloudera/Downloads/door2door/input/data/resources/2019/test_data_dir/"


spark = SparkSession.builder \
        .appName("test_application") \
        .getOrCreate()


'''def test_average_distance():
    input_co_ordinates = []
    input_co_ordinates.append([52.2296756, 21.0122287])
    input_co_ordinates.append([52.406374, 16.9251681])
    result = 279.35290160386563
    assert result == average_distance_calculator.calculate_average(input_co_ordinates)'''


def test_data_average_calculator():
    event_filter = average_distance_calculator.event_filter(spark, INPUT_PATH)
    result = "id1,279.35290160386563"
    assert result == average_distance_calculator.average_distance(event_filter)

