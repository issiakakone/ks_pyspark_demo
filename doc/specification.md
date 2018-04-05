
# Test Technique Data Engineer

## Environnement
* Spark 1.6
* Hive
* HDFS

## Enoncé
L’objectif va être de construire une application PySpark permettant de parser un fichier csv afin de produire en sortie une table externe Hive et son script de création.

Celle-ci devra être générique, et se baser sur un fichier de settings dans lequel on pourra préciser sous forme de clés :

* Le nom de la table
* Le schéma (utilisé pour le parsing du fichier et la production de la table externe).
  * Ex : "name": "date_1", "type": "timestamp", "pattern" : "%Y-%m-%d"
* Sa localisation sur HDFS
* Format de fichier en sortie
* Etc.

On aura un fichier de settings par source.

Dans le cadre de cet exercice, on prendra en entrée un fichier in_data.csv situé sur HDFS dans
`/river/raw`.

Celui-ci devra être parsé de la manière suivante :

* Identifiant : doit être identique à l'entrée (String)
* Date : doit être converti au format Timestamp
* Montant_1 : doit être converti au format Double
* Montant_2 : doit être converti au format Double
* Montant_3 : doit être converti au format Double
* Telephone : doit être normalisé en enlevant les points et espaces
* Sum_montant : Montant_1 + Montant_2 + Montant_3
* Div_sum_montant : (Montant_1 + Montant_2) / Montant_3

> * On attendra en sortie une table f_data dans `/river/data/bv_output`

> * Attention : Certaines lignes ne sont pas parsable (ex : Format de date invalide), ces lignes doivent être isolé dans le répertoire `/river/data/bv_exception`

> * Important : On prendra pour hypothèse que le fichier est volumineux, et que la table produite en sortie sera optimisée pour faire des analyses mensuelles


## Output attendu

Une application PySpark déposée sur GitHub déployable en production. Un grand soin devra
donc être apporté sur la qualité du code.