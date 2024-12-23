import pandas as pd
from igraph import Graph

# 1. CSV-Dateien einlesen
edges_file_path = "data/VoiceActorOneMode.csv"
nodes_file_path = "data/VoiceActor_Nodes.csv"

# Kanten-Daten einlesen
edges_data = pd.read_csv(edges_file_path)

# Knoten-Daten einlesen
nodes_data = pd.read_csv(nodes_file_path, delimiter=';')

# IDs und Namen aus der Knoten-Datei extrahieren
id_to_name = dict(zip(nodes_data["id"], nodes_data["name"]))  # Mapping von ID zu Name

# 2. IDs normalisieren (wie zuvor)
unique_ids = pd.concat([edges_data["Source"], edges_data["Target"]]).unique()
id_mapping = {original_id: idx for idx, original_id in enumerate(unique_ids)}

# IDs ersetzen
edges_data["Source"] = edges_data["Source"].map(id_mapping)
edges_data["Target"] = edges_data["Target"].map(id_mapping)

# Auch die Namen normalisieren
normalized_id_to_name = {id_mapping[original_id]: id_to_name[original_id] for original_id in unique_ids}

# 3. Graph in igraph erstellen
edges = edges_data[["Source", "Target"]].values.tolist()
g = Graph()
g.add_vertices(len(unique_ids))
g.add_edges(edges)

if "Weight" in edges_data.columns:
    g.es["weight"] = edges_data["Weight"]
print("Anzahl der Knoten:", g.vcount())
print("Anzahl der Kanten:", g.ecount())

#Berechnung der Jaccard-Scores
jaccard_scores = g.similarity_jaccard(mode="ALL")

results = []
for source_idx in range(len(jaccard_scores)):
    for target_idx in range(len(jaccard_scores[source_idx])):
        if source_idx != target_idx and not g.are_adjacent(source_idx, target_idx):
            results.append({
                "Source": source_idx,
                "Target": target_idx,
                "Jaccard-Score": jaccard_scores[source_idx][target_idx]
            })

results = sorted(results, key=lambda x: x["Jaccard-Score"], reverse=True)

printed_pairs = set()
print("grösse von results:", len(results))
print("Top 10 potenzielle Verbindungen mit Namen:")
for r in results:
    source_name = normalized_id_to_name[r["Source"]]
    target_name = normalized_id_to_name[r["Target"]]
    pair = tuple(sorted([source_name, target_name]))
    if pair not in printed_pairs:
        printed_pairs.add(pair)
        print(f"Potenzielle Verbindung: {source_name} ↔ {target_name} mit Score {r['Jaccard-Score']:.4f}")
        if len(printed_pairs) >= 10:
            break

#TODO: Versuchen two-mode Graphen zu erstellen und Jaccard-Score zu berechnen