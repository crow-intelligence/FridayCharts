import pickle
import time
from itertools import combinations

import pandas as pd
import spacy
import wikipediaapi

nlp = spacy.load("en_core_web_lg")

wiki_wiki = wikipediaapi.Wikipedia(
    user_agent="Semiconductor KG (zoltan.varju@crowintelligence.org)",
    language="en",
    extract_format=wikipediaapi.ExtractFormat.WIKI,
)


def extract_organizations(text):
    """Extract organization names from text using spaCy NER"""
    doc = nlp(text)
    organizations = []

    for ent in doc.ents:
        if ent.label_ == "ORG":
            org_name = ent.text.strip()
            organizations.append(org_name)

    return organizations


df = pd.read_csv("data/semiconductor_seed.csv")

nodes = set()
edges = []

for _, row in df.iterrows():
    org_name = row["Name"]
    print(f"Processing {org_name}")
    page_py = wiki_wiki.page(org_name)
    org_text = page_py.text

    organizations = set(extract_organizations(org_text))
    nodes.update(organizations)
    edges.extend(list(combinations(organizations, 2)))

    print(len(nodes), len(edges))
    # being nice to the API :D
    time.sleep(1)


nodes = sorted(nodes)
sorted_edges = sorted([tuple(sorted(t)) for t in edges])

with open("data/semiconductor_kg.pickle", "wb") as f:
    pickle.dump({"nodes": nodes, "edges": sorted_edges}, f)
