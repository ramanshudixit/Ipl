from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Row, SparkSession, functions, DataFrameWriter

# creating SparkContext
sc = SparkContext.getOrCreate(SparkConf())
sp = SparkSession.builder.getOrCreate()

# reading input file and using only the columns that are needed and creating data frame
data = sc.textFile('input.csv') \
    .filter(lambda x: "IPL_year" not in x) \
    .map(lambda l: Row(year=l.split(",")[1].upper(), info=l.split(",")[2].upper(), teams=l.split(",")[4].upper(),
                       result=l.split(",")[5].upper()))
df = sp.createDataFrame(data)

# removing those matches which are not yet played
df = df.filter(df.result != '')
df = df.filter(df.info.like('%MATCH%'))

# splitting the teams into two columns
split_col = functions.split(df['teams'], ' VS ')
df = df.withColumn('Team1', split_col.getItem(0))
df = df.withColumn('Team2', split_col.getItem(1))

# cleaning the result column
split_col = functions.split(df['result'], ' WON ')
df = df.withColumn('new_result', split_col.getItem(0))
df = df.withColumn('new_result', functions.
                   when(df.new_result.like('%ABANDONED%'), 'TIE').
                   when(df.new_result.like('%RESULT%'), 'TIE').
                   otherwise(df.new_result))
df = df.withColumn('new_result', functions
                   .when(df.new_result.like('%TIED%'), functions.split(df.new_result, '\\(').getItem(1))
                   .otherwise(df.new_result))

# consistency in the names across all columns
df = df.withColumn('Team1', functions.
                   when(df.Team1 == 'DECCAN CHARGERS', 'SUNRISERS HYDERABAD').
                   when(df.Team1 == 'RISING PUNE SUPERGIANT', 'RISING PUNE SUPERGIANTS').
                   otherwise(df.Team1))
df = df.withColumn('Team2', functions.
                   when(df.Team2 == 'DECCAN CHARGERS', 'SUNRISERS HYDERABAD').
                   when(df.Team2 == 'RISING PUNE SUPERGIANT', 'RISING PUNE SUPERGIANTS').
                   otherwise(df.Team2))
df = df.withColumn('new_result', functions.
                   when(df.new_result == 'BANGALORE', 'ROYAL CHALLENGERS BANGALORE').
                   when(df.new_result == 'RISING PUNE SUPERGIANT', 'RISING PUNE SUPERGIANTS').
                   when(df.new_result == 'RAJASTHAN', 'RAJASTHAN ROYALS').
                   when(df.new_result == 'DECCAN CHARGERS', 'SUNRISERS HYDERABAD').
                   when(df.new_result == 'HYDERABAD', 'SUNRISERS HYDERABAD').
                   when(df.new_result == 'DELHI DAREDEVILS', 'DELHI CAPITALS').
                   otherwise(df.new_result))

# dropping the original columns now
df = df.drop('teams') \
    .drop('result') \
    .drop('info')

# final transformations
df1 = df.withColumn('result', functions.
                    when(df.Team1 == df.new_result, 2).
                    when(df.new_result == 'TIE', 1).
                    otherwise(0)) \
    .drop('Team2') \
    .drop('new_result') \
    .withColumnRenamed('Team1', 'team')

df2 = df.withColumn('result', functions.
                    when(df.Team2 == df.new_result, 2).
                    when(df.new_result == 'TIE', 1).
                    otherwise(0)) \
    .drop('Team1') \
    .drop('new_result') \
    .withColumnRenamed('Team2', 'team')

df = df1.union(df2)
df.show()

df.createOrReplaceTempView("ipl")
dt = sp.sql("select year, new_result, count(*) from ipl where year = 2008 group by year, new_result order by year")

final = sp.sql("select year, team,"
               "count(team) as total_matches,"
               "count(case when result = 2 then 1 end) as total_won,"
               "count(case when result = 1 then 1 end) as total_tie,"
               "count(case when result = 0 then 1 end) as total_loss,"
               "sum(result) as total_points from ipl "
               "group by year, team order by year asc, total_points desc, total_won desc")

final.show(truncate=False)