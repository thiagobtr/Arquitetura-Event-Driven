
# Leitura Classe
from pyspark.sql import SparkSession

# Carrega Funcoes
from pyspark.sql.functions import to_date,current_date,udf,year


# Carrega Sessão
spark = SparkSession.builder.appName("Teste-TripDataset").getOrCreate()


# Importa arquivo functions.py, com as funçoes para geração das variaveis "long_trip" e "age_range"
spark.sparkContext.addPyFile('teste_engenheiro_de_dados-master/cycle-share-dataset/Functions.py')
from Functions import *


# leitura dos datasets
path = 'teste_engenheiro_de_dados-master/cycle-share-dataset/'

trip_file = path+'trip.csv'
trip_data = spark.read.csv(trip_file, header=True, sep=",").cache()

station_file = path+'station.csv'
station_data = spark.read.csv(station_file, header=True, sep=",").cache()

weather_file = path+'weather.csv'
weather_data = spark.read.csv(weather_file, header=True, sep=",").cache()


# Converte variavel "date" para formato date
weather_data = weather_data.withColumn("date",to_date("date", "MM/dd/yyyy"))


#### - Uma coluna com o nome "long_trip" de valor booleano sendo 'true' para viagens superiores a 10 minutos.

# - Troca valores nulos por 0
# - Converte a coluna "tripduration" para float
# - chama funcao "udf_longtrip" para criar a coluna "long_trip" 

trip_data = trip_data.na.fill({'tripduration':0}).withColumn("tripduration",udf_tofloat('tripduration')).withColumn("long_trip",udf_longtrip('tripduration'))



# Criando table temp

# Obs: "createOrReplaceTempView" -> a view temporaria utilizada nesse script tem um escopo de sessão.
# Para compartilhar a view ente varias sessoes, precisamos utilizar "createGlobalTempView" 

station_data.createOrReplaceTempView("station")
weather_data.createOrReplaceTempView("weather")
trip_data.createOrReplaceTempView("trip")


##### - Lat/long -> Latitude/Longitude da estação de início e de fim.
##### - Condição meteorológica no dia da viagem (coluna events da tabela weather).
# Utilizando "left join" para possiveis registros que nao estao em "weather" e/ou "station"

query = 'select a.*,b.lat as from_station_lat,b.long as from_station_long,c.lat as to_station_lat,c.long as to_station_long, d.events, to_date(birthyear) as age from trip a   left join station b on a.from_station_id = b.station_id  left join station c on a.to_station_id = c.station_id  left join weather d on d.date = to_date(starttime, "MM/dd/yyyy")'


# Executa a query e gera dataframe "trip_dataset"
trip_dataset = spark.sql(query)


#### - Uma coluna com o nome "age_range" sendo o valor de 1 para pessoas 
####    de 0-16 anos, 2 de 17-25, 3 de 26-50 e 4 para 50+.

# Atualiza a coluna "age" para idade 
# troca valores nulos por 0
# executa a funcao para aplicar os valores desejados.

trip_dataset = trip_dataset.withColumn("age",year(current_date())-year('age')).na.fill({'age':0}).withColumn("age_range",udf_agerange('age'))


# Gerando arquivo de saida ".csv", usando o metodo repartition(1) 
trip_dataset.drop("age").repartition(1).write.csv("output",header=True)

