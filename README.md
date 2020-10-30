# BigData_Evaluation
Evalutaion Formation BigData

On a 3 tables "movies" et "rating" "credits" 


La partie que vous allez développer, sera sous forme de TU (Tests Unitaires), donc dans le partie 'test/scala'
Dans 'test/resources', on a 3 fichier de données (movies, ratings, et credits). C'est la source de donnée qu'on va utiliser dans ce TP.
L'objectif de ce TP est de manipuler ces sources de données.

Dans ce répo, on a un projet Maven, qui contient les packages nécessaires pour le développement

1) - On utilisant (Scala,Spark), on veut lire les fichiers sources (Format CSV) dans dataframes différents
 => Dataframe 1 : 'moviesDF', Dataframe 2 : 'ratingDF', Dataframe 3 : 'creditsDF'
 
Rappel : Avant de commencer toute manipulation avec Spark, faut d'abord initialiser le SparkSession avec les bonne paramètres

2) - Afficher les deux Dataframe, et vérifier qu'ils sont bien formatés


3) - Ensuite Pour le DF : 'moviesDF', on veut filtrer sur la colonne 'original_language', qui contient plusieurs valeurs ('en', 'fr', ..), est créer un nouveu DF (moviesEN).

4) - Faire une jointure (left) entre 'moviesEN' et 'ratingD'F sur la colonne ('id', 'movieId'), le résultat sera : 'moviesWithRatingsDF'
 
5) - Maintenant concernant le DF : 'moviesWithRatingsDF', on veut garder que les colonnes suivantes : ('id', 'original_title', 'genres', 'adult', 'budget', 'overview', 'timestamp') et supprimer le restes

6) - Ensuite on veut Renommer la colonne 'original_title' en -> title dans le Dataframe 'moviesWithRatingsDF'

7) - Crée une fonction pour Afficher les premiers 5 lignes d'un Dataframe et aussi pour visualiser son schéma de données, et utiliser cette fonction pour voir le contenu et le schéma du Dataframe 'moviesWithRatingsDF'

8) - Classer les films sur leur 'rating' en ordre (DESC), et sauvegarder le résultat dans un fichier CSV.

9) - Faire une 2éme jointure entre 'moviesWithRatingsDF' et 'creditsDF' sur la colonne 'id' => le Dataframe de sortie est nommé 'moviesAndCredits' , puis afficher le 'cast' du film avec le titre 'Jumanji', on utilisant deux méthodes différentes : (SparkSQL) et (l'API Dataframe)

10) - Dans le Dataframe 'moviesAndCredits', on a une colonne 'timestamp', on utilisant les 'UDF' de Spark on veut transformer cette colonne
