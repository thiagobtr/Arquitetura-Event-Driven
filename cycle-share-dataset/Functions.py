# Carrega Funcoes
from pyspark.sql.functions import udf 
# Tipos
from pyspark.sql.types import FloatType, BooleanType, IntegerType


# Funcao para conversao
def tofloat(variavel):
    return float(variavel)

# Converte para funcao UDF
udf_tofloat=udf(tofloat,FloatType())



# funcao para gerar a coluna "True" para + de 10 min.
def longtrip (variavel):
    # 
    if variavel > 600:
        return bool(True)
    else:
        return bool(False)
    
# Converte para funcao UDF(Nome da funcao, tipo do retorno)
udf_longtrip = udf(longtrip, BooleanType())

# Criacao da funcao para gerar a coluna "age_range"
def agerange (valor):
        
    if valor > 1 and valor <= 16:
        return int(1)
    if valor >= 17 and valor<= 25:
        return int(2)
    if valor >= 26 and valor <= 50:
        return int(3)
    if valor > 50:
        return int(4)
    
# Converte para funcao UDF(Nome da funcao, tipo do retorno)
udf_agerange = udf(agerange, IntegerType())




