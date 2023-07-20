import pandas as pd
from pymongo import MongoClient

### 1.1 Criação MongoDB Local ###

# Servidor mongodb e vínculo ao localhost
client = MongoClient('localhost', 27017)
db = client['local']

### 1.2 Criação do Pandas Dataframe ###

# Collection carros
cars = {
    'Carro': ['Onix', 'Polo', 'Sandero', 'Fiesta', 'City'],
    'Cor': ['Prata', 'Branco', 'Prata', 'Vermelho', 'Preto'],
    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda']
}

# Criando um df para a coleção: carros
df_cars = pd.DataFrame(cars)

# Collection montadoras
automakers = {
    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda'],
    'País': ['EUA', 'Alemanha', 'Franca', 'EUA', 'Japao']
}

# Criando um df para a coleção: montadoras
df_automakers = pd.DataFrame(automakers)

# Atribuindo as collection's do mongodb à duas variáveis
cars_collection = db['carros']
automakers_collection = db['montadoras']

# Convertendo os df's em dict
data_cars = df_cars.to_dict(orient="records")
data_automakers = df_automakers.to_dict(orient="records")

### 1.3 Salvar Pandas Dataframe no MongoDB ###

# Inserindo os dados nas collection's do mongodb
cars_collection.insert_many(data_cars)
automakers_collection.insert_many(data_automakers)