# Projet 08 - Infrastructure cloud de streaming temps reel (Redpanda)

> Formation Data Engineer - OpenClassrooms | Outils : Docker, Redpanda (Kafka-compatible), Python

## Objectif

Modeliser et deployer une infrastructure de streaming de donnees temps reel dans le cloud, basee sur une architecture producteur/consommateur avec Redpanda. Le cas d'usage simule un systeme de gestion de tickets evenementiels.

## Architecture

- Producer : genere et envoie des messages vers Redpanda
- Redpanda : broker de messages temps reel compatible Kafka
- Consumer : consomme et traite les messages en temps reel

## Competences travaillees

- Architecture de streaming producteur/consommateur
- Conteneurisation multi-services avec Docker Compose
- Traitement de donnees en temps reel

## Contenu du repo

- Docker-compose.yml : orchestration des 3 services
- Producer/ : Dockerfile + script Python producteur
- Consumer/ : Dockerfile + script Python consommateur
- Redpanda/ : configuration du broker

## Lancer l'infrastructure

docker-compose up -d

---
Formation Data Engineer - OpenClassrooms
