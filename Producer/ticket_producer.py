import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# ── Configuration ──────────────────────────────────────────────────────────────

import os

BROKER     = os.getenv("BROKER", "localhost:9092")
TOPIC      = os.getenv("TOPIC", "client_tickets")
NB_TICKETS = int(os.getenv("NB_TICKETS", "20"))
DELAI      = 1.0

# ── Données de simulation ──────────────────────────────────────────────────────

TYPES_DEMANDE = [
    "Support technique",
    "Facturation",
    "Remboursement",
    "Information produit",
    "Réclamation",
    "Demande de devis",
]

PRIORITES = ["Faible", "Moyenne", "Haute", "Critique"]

DEMANDES = [
    "Mon équipement ne fonctionne plus depuis ce matin.",
    "Je n'ai pas reçu ma facture du mois dernier.",
    "Je souhaite être remboursé suite à une erreur de commande.",
    "Pouvez-vous m'envoyer la documentation technique du produit X ?",
    "Le technicien n'est pas venu au rendez-vous prévu.",
    "J'ai besoin d'un devis pour 10 unités supplémentaires.",
    "Mon accès au portail client est bloqué.",
    "La livraison est en retard de 5 jours.",
    "Je souhaite mettre à jour mes coordonnées bancaires.",
    "Le produit reçu est endommagé.",
]

# ── Générateur de ticket ───────────────────────────────────────────────────────

def generer_ticket():
    return {
        "ticket_id"     : str(uuid.uuid4()),
        "client_id"     : f"CLIENT-{random.randint(1000, 9999)}",
        "date_creation" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "demande"       : random.choice(DEMANDES),
        "type_demande"  : random.choice(TYPES_DEMANDE),
        "priorite"      : random.choice(PRIORITES),
    }

# ── Callbacks ─────────────────────────────────────────────────────────────────

def on_succes(metadata):
    print(f"  ✅ Envoyé → partition: {metadata.partition} | offset: {metadata.offset}")

def on_erreur(e):
    print(f"  ❌ Erreur → {e}")

# ── Producteur principal ───────────────────────────────────────────────────────

def lancer_producteur():
    print("=" * 60)
    print("  POC InduTechData — Producteur de tickets clients")
    print(f"  Broker : {BROKER}")
    print(f"  Topic  : {TOPIC}")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )
        print(f"\n🟢 Connecté à Redpanda sur {BROKER}\n")
    except Exception as e:
        print(f"\n🔴 Impossible de se connecter : {e}")
        return

    compteur = 0
    try:
        while NB_TICKETS == 0 or compteur < NB_TICKETS:
            ticket = generer_ticket()
            compteur += 1

            print(f"\n📨 Ticket #{compteur}")
            print(f"   ID       : {ticket['ticket_id']}")
            print(f"   Client   : {ticket['client_id']}")
            print(f"   Type     : {ticket['type_demande']}")
            print(f"   Priorité : {ticket['priorite']}")
            print(f"   Demande  : {ticket['demande']}")
            print(f"   Créé le  : {ticket['date_creation']}")

            producer.send(
                TOPIC,
                key=ticket["client_id"],
                value=ticket
            ).add_callback(on_succes).add_errback(on_erreur)

            producer.flush()
            time.sleep(DELAI)

    except KeyboardInterrupt:
        print("\n\n⛔ Arrêt demandé.")

    finally:
        producer.close()
        print(f"\n✅ Fin — {compteur} ticket(s) envoyé(s).")
        print("=" * 60)

# ── Point d'entrée ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    lancer_producteur()