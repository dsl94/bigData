from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import min, max, avg, mean, stddev, month, dayofweek
from pyspark.sql.types import StructType
import datetime
import argparse

# Odrediti broj i karakteristike odgovarajućih vrednosti atributa/događaja na određenoj
# lokaciji (oblasti) u određenom vremenskom priodu (koji se zadaju kao parametri
# aplikacije), koji zadovoljavaju zadati uslov
def number_of_rides_per_start_station(data: DataFrame, station_id, from_date, to_date):
    print("Total rides from station in given period: " + str(
        data.filter(data.start_station_id == station_id).filter((data.started_at >= from_date) &
                                                                (data.started_at <= to_date)).count()
    ))
    data.unpersist()
    print("Most frequent routes from station in given period")
    data.filter(data.start_station_id == station_id).filter((data.started_at >= from_date) &
                                                            (data.started_at <= to_date)).groupBy(data.end_station_name).count().show()


# Naći minimalne, maksimalne, srednje vrednosti, standardne devijaciju i ostale statističke
# parametre određene(-ih) atributa grupisane po lokaciji/oblasti i vremenu.
def duration_statistic_by_start_station_and_day_of_the_week(data: DataFrame):
    by_start_station = data.groupBy("start_station_name", dayofweek("started_at")).agg(
        min("duration"), max("duration"), stddev("duration"), mean("duration")
    )
    by_start_station.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--station', type=int, help='Id of start station')
    parser.add_argument('--from_date', type=str, help='From date in format 2022-01-01 00:00:00.0+00:00')
    parser.add_argument('--to_date', type=str, help='From date in format 2022-01-01 23:59:59.0+00:00')
    args = parser.parse_args()

    print(args.station)
    print(args.from_date)
    print(args.to_date)
    from_date = datetime.datetime.strptime(args.from_date, "%Y-%m-%dT%H:%M:%S")
    to_date = datetime.datetime.strptime(args.to_date, "%Y-%m-%dT%H:%M:%S")


    dataSchema = StructType() \
        .add("started_at", "timestamp") \
        .add("ended_at", "timestamp") \
        .add("duration", "integer") \
        .add("start_station_id", "integer") \
        .add("start_station_name", "string") \
        .add("start_station_description", "string") \
        .add("start_station_latitude", "decimal") \
        .add("start_station_longitude", "decimal") \
        .add("end_station_id", "integer") \
        .add("end_station_name", "string") \
        .add("end_station_description", "string") \
        .add("end_station_latitude", "decimal") \
        .add("end_station_longitude", "decimal")

    spark = SparkSession.builder.master("spark://spark-master:7077").appName("hdfs_test").getOrCreate()

    data = spark.read.csv("hdfs://namenode:9000/dir/oslo-bikes.csv", schema=dataSchema)
    # Za lokalno pokretanje
    # spark = SparkSession.builder.getOrCreate()
    # data = spark.read.csv("oslo-bikes.csv", schema=dataSchema)
    data.cache()
    number_of_rides_per_start_station(data, args.station, from_date, to_date)
    duration_statistic_by_start_station_and_day_of_the_week(data)
    data.unpersist()