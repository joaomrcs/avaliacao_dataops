import json
from bson import ObjectId
from pymongo import MongoClient

# Função personalizada para serializar objetos não padrão do JSON
def json_serial(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError('non-serializable')

# Servidor mongodb e vinculo ao localhost
client = MongoClient('localhost', 27017)
db = client['local']

# Atribuindo as collection's do mongodb à duas variáveis
cars_collection = db['carros']
automakers_collection = db['montadoras']

### 1.4 && 1.5 Criando agregação no MongoDB e agrupando informações ###

# Agregação das collection's 'carros' e 'montadoras'
pipeline = [
    {
        "$lookup": {
            "from": "montadoras",
            "localField": "Montadora",
            "foreignField": "Montadora",
            "as": "MontadoraInfo"
        }
    },
        {
        "$unwind": "$MontadoraInfo"
    },
    {
        "$project": {
            "_id": 1,
            "carro": "$Carro",
            "cor": "$Cor",
            "montadora": "$Montadora",
            "montadoras": "$Montadora",
            "país": "$MontadoraInfo.País"
        }
    },
    {
        "$group": {
            "_id": "$país",
            "Carros": {
                "$push": {
                    "_id": "$_id",
                    "carro": "$Carro",
                    "cor": "$Cor",
                    "montadora": "$Montadora",
                    "montadoras": "$Montadoras"
                }
            }
        }
    }
]

# Realize a agregação na coleção "Carros"
results_agg = list(cars_collection.aggregate(pipeline))

# Itere sobre os resultados e imprima-os
for documento in results_agg:
   print(documento)

# Atribuindo a agg em uma variável
results_agg = cars_collection.aggregate(pipeline)

# Convertendo o resultado em uma lista
list_result = list(results_agg)

# Caminho para o arquivo de saída
path = '/Users/john/Downloads/results.js'

# Gerando o arquivo js com o resultado da agg
with open(path, "w") as arquive_js:
    json.dump(list_result, arquive_js, default=json_serial)

print('Resultado gerado com sucesoo!')