import pymongo
from pprint import pprint
import json
from tkinter import filedialog

class Connexion:
    @classmethod
    # Se connecter à Atlas
    def connect(cls, database=None):
        cls.user = 'Stephane'
        cls.password = 'isenbrest'
        cls.database = database
        return pymongo.MongoClient(f"mongodb+srv://{cls.user}:{cls.password}@bel-cluster.1cbyc.mongodb.net/{cls.database}?retryWrites=true&w=majority")
    
    @classmethod
    # Se connecter à la base de données
    def open_connexion(cls):
        cls.client = cls.connect()
        cls.publications = cls.client.DBLP.publications

    @classmethod
    # Se déconnecter de la base de données
    def close_connexion(cls):
        cls.client.close()

    @classmethod
    # Compter le nombre de documents de la collection publis
    def get_count(cls):
        cls.open_connexion()
        count = len(list(cls.publications.find({})))
        cls.close_connexion()
        return f"Il y a {count} publications dans la base de données"

    @classmethod
    # Lister tous les articles d'un type
    def get_articles(cls, _type):
        cls.open_connexion()
        articles = list(cls.publications.find({'type': _type}, {'title': 1, '_id': 0}))
        cls.close_connexion()
        return articles

    @classmethod
    # Lister tous les livres d'une année, avant ou à partir d'une année
    def get_articles_year(cls, operator, year, _type):
        cls.open_connexion()
        articles = list(cls.publications.find({'type': _type, 'year': {operator: year}}, {'title': 1, '_id': 0}))
        cls.close_connexion()
        return articles

    @classmethod
    # Lister les publications d'un auteur
    def get_articles_author(cls, author, sort='_id', order=1, count=False):
        cls.open_connexion()
        articles = list(cls.publications.find({'authors': author}, {'title': 1, '_id': 0}).sort(sort, order))
        cls.close_connexion()
        if count == True:
            return f"Il y a {len(articles)} publications pour {author}"
        return articles

    @classmethod
    # Lister tous les auteurs distincts
    def get_all_authors(cls, count=False):
        cls.open_connexion()
        authors = list(cls.publications.find().distinct('authors'))
        cls.close_connexion()
        if count == True:
            return len(authors)
        return authors

    @classmethod
    # Lister le nombre d'articles de chaque type
    def get_number_per_type(cls, operator, year):
        cls.open_connexion()
        result = list(cls.publications.aggregate(
            [{'$match':{'year' : {operator : year}}}, {'$group':{'_id': "$type", 'total' : { '$sum' : 1}}}]))
        cls.close_connexion()
        return result

    @classmethod
    # Compter le nombre de publications par auteur et trier le résultat dans un ordre choisi
    def get_number_articles_per_author(cls, order):
        cls.open_connexion()
        count = list(cls.publications.aggregate(
            [{'$unwind': '$authors'}, {'$group': {'_id': {'author': '$authors'}, 'count': {'$sum': 1 }}}, {'$sort': {'count': order}}]))
        cls.close_connexion()
        return count

    @classmethod
    # Insérer un fichier json dans une collection donnée
    def insert_json(cls, collection):
        path = filedialog.askopenfilename()
        name = path.split('/')[-1]
        
        cls.open_connexion()
        global client
        client = cls.client
        col = None
        exec(f"col = client.DBLP.{collection}", globals())
        with open(path) as file:
            data = json.load(file)
        if isinstance(data, list):
            col.insert_many(data)
        else:
            col.insert_one(data)
        cls.close_connexion()

        return print(f"Le fichier {name} a été inséré dans la colection {collection}")