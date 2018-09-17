from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

sc = SparkContext(appName="tvViewers")
sqlContext = SQLContext(sc)

lines = sc.textFile("tv-audience-dataset.csv")
cols = lines.map(lambda l: l.split(","))
obs = cols.map(lambda p: Row(channel_id=p[0], slot=p[1], week=p[2], genre_id = p[3], sub_genre_id = p[4], user_id = p[5], program_id = p[6], event_id = p[7], duration=p[8]))

dataFrame = sqlContext.createDataFrame(obs)

genreLines = sc.textFile("genres_legend.txt")
genreCols = genreLines.map(lambda l: l.split("\t"))
genreObs = genreCols.map(lambda p: Row(genre=p[0], genre_id=p[1], sub_genre=p[2], sub_genre_id=p[3]))

genreDataFrame = sqlContext.createDataFrame(genreObs)

#Cleaning the dataset since data with weeks from 14 to 19 contain errors
#Dataframe in the form of hour slots for the entire week ranging from 1 to 168(24*7)
dataFrame = dataFrame.where(~dataFrame.week.isin(14,15,16,17,18,19))

#Functions to identify days and hours in a week
identify_days = func.udf(lambda s: ((int(s)-1) // 24) + 1, IntegerType())
identify_hours = func.udf(lambda s: ((int(s) - 1) % 24) , IntegerType())

#Dataframe with hours in a specific day
dataFrameDays = dataFrame.select('channel_id','slot','week','genre_id','sub_genre_id','user_id','program_id','event_id','duration',identify_days('slot').alias('day_no'),identify_hours('slot').alias('hour'))

#Return Genre Table
def show_dataframe(df):
	return df.show()
	
#Function to find which hour of a day has the maximum viewers
def pop_day_best_hours(df):
	df = df.groupBy("hour").agg(func.count("user_id")).select(func.col("hour"),func.col("count(user_id)").alias("viewers"))
	ranked =  df.withColumn("rank", func.rank().over(Window.partitionBy("hour").orderBy(func.desc("viewers"))))
	dayHourCount = ranked.filter(ranked["rank"] == 1).select("hour","viewers").orderBy(func.desc("viewers"))
	return dayHourCount.show()

#Best time to host a program during a week
def pop_best_hours(df):
	df = df.groupBy("slot").agg(func.count("user_id")).select(func.col("slot"),func.col("count(user_id)").alias("viewers"))
	ranked =  df.withColumn("rank", func.rank().over(Window.partitionBy("slot").orderBy(func.desc("viewers"))))
	hourCount = ranked.filter(ranked["rank"] == 1).select("slot","viewers").orderBy(func.desc("viewers"))
	return hourCount.show()

#Best channel for a particular time in a day
def pop_channels(df,hour_time):
	df = df.filter(df["hour"] == hour_time).groupBy("hour","channel_id").agg(func.count("user_id")).select(func.col("hour"), func.col("channel_id"),func.col("count(user_id)").alias("viewers"))
	ranked =  df.withColumn("rank", func.rank().over(Window.partitionBy("hour").orderBy(func.desc("viewers"))))
	viewerCount = ranked.select("hour", "channel_id","viewers").orderBy(func.desc("viewers"))
	return viewerCount.show()
	
#Best program in a particular channel
def pop_best_program(df,channel):
	df = df.filter(df["channel_id"] == channel).groupBy("channel_id","program_id").agg(func.count("user_id")).select(func.col("channel_id"), func.col("program_id"),func.col("count(user_id)").alias("viewers"))
	ranked = df.withColumn("rank", func.rank().over(Window.partitionBy("channel_id").orderBy(func.desc("viewers"))))
	viewersProgram = ranked.select("channel_id", "program_id", "viewers").orderBy(func.desc("viewers"))
	return viewersProgram.show()

#Get best sub genres for a particular genre
def pop_best_sub_genre(df,legend,genre):
	gen_id = legend.filter(legend["genre"] == genre).select(func.col("genre_id")).rdd.flatMap(list).first()
	df = df.filter(df["genre_id"] == gen_id).groupBy("genre_id","sub_genre_id").agg(func.count("user_id")).select(func.col("genre_id"), func.col("sub_genre_id"), func.col("count(user_id)").alias("viewers"))
	bestSubGenresId = df.select(func.col("genre_id").alias("df_genre_id"), func.col("sub_genre_id").alias("df_sub_genre_id"), "viewers")
	bestSubGenres = bestSubGenresId.join(legend, (bestSubGenresId.df_genre_id == legend.genre_id) & (bestSubGenresId.df_sub_genre_id == legend.sub_genre_id))
	bestSubGenres = bestSubGenres.select("df_genre_id", "df_sub_genre_id", "viewers", "genre", "sub_genre").orderBy(func.desc("viewers"))
	return bestSubGenres.show()
	
#What is more watched
def pop_genre(df,legend):
	df = df.groupBy("genre_id").agg(func.count("user_id")).select(func.col("genre_id").alias("df_genre_id"), func.col("count(user_id)").alias("viewers"))
	df = df.join(legend.select(func.col("genre_id"), func.col("genre")).distinct(), df.df_genre_id == legend.genre_id)
	genreViewers = df.select("genre_id", "genre", "viewers").orderBy(func.desc("viewers"))
	return genreViewers.show()

#Executing above functions
print(show_dataframe(genreDataFrame))
print(pop_best_sub_genre(dataFrameDays,genreDataFrame,"movie"))
print(pop_day_best_hours(dataFrameDays))
print(pop_best_hours(dataFrame))
print(pop_channels(dataFrameDays,1))
print(pop_best_program(dataFrame,175))
print(pop_genre(dataFrame, genreDataFrame))