CREATE KEYSPACE ippon_technologies WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE ippon_technologies;

CREATE TABLE effectifs (
	uuid text PRIMARY KEY,
	rentree int,
	rentree_univ text,
	niveau_geo text,
	unite_geo text,
	code_postal text,
	regroupement_code text,
	regroupement text,
	secteur_code text,
	secteur text,
	sexe_code int,
	sexe text,
	nb_inscrits double
	);
