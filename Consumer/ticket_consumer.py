import json
import pandas as pd
from kafka import KafkaConsumer

import os

BROKER = os.getenv("BROKER", "localhost:9092")
TOPIC  = os.getenv("TOPIC", "client_tickets")

# ── Mapping équipes ────────────────────────────────────────────────────────────

def assigner_equipe(type_demande):
    mapping = {
        "Support technique"  : "Equipe Technique",
        "Facturation"        : "Equipe Finance",
        "Remboursement"      : "Equipe Finance",
        "Information produit": "Equipe Commercial",
        "Réclamation"        : "Equipe Qualité",
        "Demande de devis"   : "Equipe Commercial",
    }
    return mapping.get(type_demande, "Equipe Générale")

# ── Lecture Redpanda ───────────────────────────────────────────────────────────

def lire_tickets():
    print("🔄 Lecture des tickets depuis Redpanda...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,
    )
    tickets = [msg.value for msg in consumer]
    consumer.close()
    print(f"✅ {len(tickets)} tickets récupérés.\n")
    return tickets

# ── Analyse ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  POC InduTechData — Analyse des tickets clients")
    print("=" * 60)

    tickets = lire_tickets()
    if not tickets:
        print("❌ Aucun ticket trouvé.")
        return

    # Créer le DataFrame pandas
    df = pd.DataFrame(tickets)

    # Supprimer les doublons
    df = df.drop_duplicates(subset="ticket_id")

    # Ajouter colonne équipe de support
    df["equipe_support"] = df["type_demande"].apply(assigner_equipe)

    print("── Aperçu des données ─────────────────────────────────────")
    print(df[["ticket_id", "client_id", "type_demande",
              "priorite", "equipe_support"]].head(5).to_string(index=False))

    print("\n── Analyse 1 : Tickets par type de demande ────────────────")
    print(df.groupby("type_demande")["ticket_id"]
            .count().reset_index(name="nb_tickets")
            .sort_values("nb_tickets", ascending=False).to_string(index=False))

    print("\n── Analyse 2 : Tickets par priorité ───────────────────────")
    print(df.groupby("priorite")["ticket_id"]
            .count().reset_index(name="nb_tickets")
            .sort_values("nb_tickets", ascending=False).to_string(index=False))

    print("\n── Analyse 3 : Tickets par équipe de support ──────────────")
    print(df.groupby("equipe_support")["ticket_id"]
            .count().reset_index(name="nb_tickets")
            .sort_values("nb_tickets", ascending=False).to_string(index=False))

    print("\n── Analyse 4 : Tickets critiques ───────────────────────────")
    critiques = df[df["priorite"] == "Critique"]
    print(critiques[["ticket_id", "client_id",
                      "type_demande", "equipe_support"]].to_string(index=False))

    print("\n── Résumé global ───────────────────────────────────────────")
    print(f"   Total tickets     : {len(df)}")
    print(f"   Tickets critiques : {len(critiques)}")
    print(f"   Tickets normaux   : {len(df) - len(critiques)}")
    print("=" * 60)

if __name__ == "__main__":
    main()