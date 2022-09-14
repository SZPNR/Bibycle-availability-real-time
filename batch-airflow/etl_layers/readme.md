#extract-gmap > extract-weather > raw-files-processing > processed-files-uploading

#
#extract-gmap et extract-weather
#
#s3-to-raw pour récup les localisations
#voir les adresses de chaque point velib depuis l'API Google et récupérer la météo pour chaque point, envoie sur S3
#calculer la distance entre chaque point d'après gmap
#upload le df sur S3

#a
#raw-files-processing
#
#récupérer le df depuis S3, mettre aux normes, envoie sur S3
#voir s'il va pleuvoir dans l'heure, mettre intensité et proba ? 
#-> https://meteofrance.com/previsions-meteo-france/paris/75000
# https://fr.meteox.com/?time=01:00&fc=1

#
#processed-files-uploading
#
# upload sur snowflake
