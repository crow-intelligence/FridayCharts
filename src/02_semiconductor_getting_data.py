import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import time
from typing import Dict, List, Optional, Any, Union, Set, Tuple


def read_seed_list(file_path: str) -> Optional[pd.DataFrame]:
    """Read the seed list CSV file and return a DataFrame.

    Args:
        file_path: Path to the CSV file

    Returns:
        DataFrame containing the seed list or None if an error occurs
    """
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully read {len(df)} organizations from {file_path}")
        return df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None


def query_dbpedia_for_organization(org_name: str) -> Optional[Dict[str, Any]]:
    """Query DBpedia for information about an organization.

    Args:
        org_name: Name of the organization to query

    Returns:
        Dictionary containing the query results or None if an error occurs
    """
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")

    query = f"""
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX dbp: <http://dbpedia.org/property/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT DISTINCT ?organization ?label ?abstract ?headquarters ?foundingDate 
                    ?industry ?numberOfEmployees ?location ?country
    WHERE {{
      ?organization rdfs:label ?label .
      FILTER(CONTAINS(LCASE(?label), LCASE("{org_name}")) && LANG(?label) = "en") .

      OPTIONAL {{ ?organization dbo:abstract ?abstract . FILTER(LANG(?abstract) = "en") }}
      OPTIONAL {{ ?organization dbo:headquarters ?headquarters }}
      OPTIONAL {{ ?organization dbo:foundingDate ?foundingDate }}
      OPTIONAL {{ ?organization dbo:industry ?industry }}
      OPTIONAL {{ ?organization dbo:numberOfEmployees ?numberOfEmployees }}
      OPTIONAL {{ ?organization dbo:location ?location }}
      OPTIONAL {{ ?organization dbo:country ?country }}
    }}
    LIMIT 5
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    try:
        results = sparql.query().convert()
        print(f"Retrieved data for {org_name}")
        return results
    except Exception as e:
        print(f"Error querying DBpedia for {org_name}: {e}")
        return None


def process_results(
    results: Optional[Dict[str, Any]], org_name: str
) -> Optional[Dict[str, Union[str, List[str]]]]:
    """Process the DBpedia query results for an organization.

    Args:
        results: Results from the DBpedia query
        org_name: Name of the organization

    Returns:
        Dictionary containing processed organization data or None if no results found
    """
    if (
        not results
        or "results" not in results
        or "bindings" not in results["results"]
        or len(results["results"]["bindings"]) == 0
    ):
        print(f"No results found for {org_name}")
        return None

    bindings = results["results"]["bindings"]
    print(f"\n--- Results for {org_name} ---")

    org_data = {
        "name": org_name,
        "dbpedia_uri": [],
        "abstract": [],
        "headquarters": [],
        "founding_date": [],
        "industry": [],
        "employees": [],
        "locations": [],
        "countries": [],
    }

    for binding in bindings:
        if "organization" in binding:
            org_data["dbpedia_uri"].append(binding["organization"]["value"])
        if "abstract" in binding:
            org_data["abstract"].append(binding["abstract"]["value"])
        if "headquarters" in binding:
            org_data["headquarters"].append(binding["headquarters"]["value"])
        if "foundingDate" in binding:
            org_data["founding_date"].append(binding["foundingDate"]["value"])
        if "industry" in binding:
            org_data["industry"].append(binding["industry"]["value"])
        if "numberOfEmployees" in binding:
            org_data["employees"].append(binding["numberOfEmployees"]["value"])
        if "location" in binding:
            org_data["locations"].append(binding["location"]["value"])
        if "country" in binding:
            org_data["countries"].append(binding["country"]["value"])

    print(f"DBpedia URIs found: {len(org_data['dbpedia_uri'])}")
    for uri in org_data["dbpedia_uri"]:
        print(f"  - {uri}")

    if org_data["abstract"]:
        print(f"Abstract: {org_data['abstract'][0][:200]}...")

    if org_data["headquarters"]:
        print(f"Headquarters: {', '.join(org_data['headquarters'])}")

    if org_data["locations"]:
        print(f"Locations: {', '.join(org_data['locations'])}")

    if org_data["countries"]:
        print(f"Countries: {', '.join(org_data['countries'])}")

    return org_data


def query_related_organizations(dbpedia_uri: str) -> Optional[Dict[str, Any]]:
    """Query DBpedia for organizations related to the given URI.

    Args:
        dbpedia_uri: URI of the organization to find relations for

    Returns:
        Dictionary containing the query results or None if an error occurs
    """
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")

    query = f"""
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX dbp: <http://dbpedia.org/property/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?relation ?relationType ?relatedOrg ?relatedOrgLabel
    WHERE {{
      # Direct relationships where our organization is the subject
      {{
        <{dbpedia_uri}> ?relationType ?relatedOrg .
        FILTER(?relationType IN (
          dbo:subsidiary, dbo:owningCompany, dbo:parentCompany,
          dbp:parent, dbp:owner, dbp:subsidiary, dbp:subsidiaries,
          dbo:product, dbo:keyPerson, dbo:partner, dbp:partners,
          dbo:merger, dbo:acquisition, dbp:acquisitions
        ))
        BIND("outgoing" AS ?relation)
      }}
      UNION
      # Reverse relationships where our organization is the object
      {{
        ?relatedOrg ?relationType <{dbpedia_uri}> .
        FILTER(?relationType IN (
          dbo:subsidiary, dbo:owningCompany, dbo:parentCompany,
          dbp:parent, dbp:owner, dbp:subsidiary, dbp:subsidiaries,
          dbo:product, dbo:keyPerson, dbo:partner, dbp:partners,
          dbo:merger, dbo:acquisition, dbp:acquisitions
        ))
        BIND("incoming" AS ?relation)
      }}

      # Get the label of the related organization
      ?relatedOrg rdfs:label ?relatedOrgLabel .
      FILTER(LANG(?relatedOrgLabel) = "en")

      # Ensure the related entity is an organization
      ?relatedOrg a ?type .
      FILTER(?type IN (
        dbo:Company, dbo:Organisation, dbo:Organization, 
        <http://schema.org/Organization>, <http://schema.org/Corporation>
      ))
    }}
    LIMIT 50
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    try:
        results = sparql.query().convert()
        return results
    except Exception as e:
        print(f"Error querying related organizations for {dbpedia_uri}: {e}")
        return None


def process_related_organizations(
    results: Optional[Dict[str, Any]], org_uri: str
) -> List[Dict[str, str]]:
    """Process the results of the related organizations query.

    Args:
        results: Results from the DBpedia query for related organizations
        org_uri: URI of the organization we queried for

    Returns:
        List of dictionaries containing information about related organizations
    """
    related_orgs = []

    if not results or "results" not in results or "bindings" not in results["results"]:
        print(f"No related organizations found for {org_uri}")
        return related_orgs

    bindings = results["results"]["bindings"]
    print(f"Found {len(bindings)} relationships for {org_uri}")

    for binding in bindings:
        if "relatedOrg" in binding and "relatedOrgLabel" in binding:
            related_org = {
                "uri": binding["relatedOrg"]["value"],
                "label": binding["relatedOrgLabel"]["value"],
                "relation_direction": binding["relation"]["value"]
                if "relation" in binding
                else "unknown",
                "relation_type": binding["relationType"]["value"]
                if "relationType" in binding
                else "unknown",
            }
            related_orgs.append(related_org)
            print(
                f"  - {related_org['label']} ({related_org['relation_direction']} {related_org['relation_type']})"
            )

    return related_orgs


def recursive_org_exploration(
    seed_organizations: List[str], max_depth: int = 3
) -> Tuple[List[Dict[str, Any]], Dict[str, Set[str]]]:
    """Recursively explore organizations and their relationships.

    Args:
        seed_organizations: List of organization names to start exploration from
        max_depth: Maximum depth of recursion (default is 3)

    Returns:
        Tuple containing:
        - List of organization data dictionaries
        - Dictionary mapping organization URIs to sets of related organization URIs
    """
    all_org_data = []
    explored_uris = set()
    relationships = {}

    queue = [(org, 0) for org in seed_organizations]

    while queue:
        org_name, depth = queue.pop(0)

        if depth > max_depth:
            continue

        print(f"\n{'=' * 20}\nProcessing {org_name} (depth {depth})\n{'=' * 20}")

        results = query_dbpedia_for_organization(org_name)
        org_data = process_results(results, org_name)

        if not org_data or not org_data["dbpedia_uri"]:
            print(f"Skipping {org_name} - no DBpedia data found")
            continue

        all_org_data.append(org_data)

        for uri in org_data["dbpedia_uri"]:
            if uri in explored_uris:
                print(f"Already explored {uri}, skipping")
                continue

            explored_uris.add(uri)

            if depth < max_depth:
                related_results = query_related_organizations(uri)
                related_orgs = process_related_organizations(related_results, uri)

                relationships[uri] = set()

                for related_org in related_orgs:
                    related_uri = related_org["uri"]
                    related_label = related_org["label"]

                    relationships[uri].add(related_uri)

                    if related_uri not in explored_uris:
                        queue.append((related_label, depth + 1))

            time.sleep(1)

    return all_org_data, relationships


def save_results_to_csv(
    all_org_data: List[Dict[str, Any]],
    relationships: Dict[str, Set[str]],
    output_dir: str = "data/",
):
    """Save the collected data to CSV files with deduplication.

    Args:
        all_org_data: List of organization data dictionaries
        relationships: Dictionary mapping organization URIs to sets of related URIs
        output_dir: Directory to save the CSV files
    """
    orgs_rows = []
    seen_uris = set()

    for org in all_org_data:
        if not org["dbpedia_uri"]:
            continue

        for uri_idx, uri in enumerate(org["dbpedia_uri"]):
            if uri in seen_uris:
                continue

            seen_uris.add(uri)

            abstract = (
                org["abstract"][uri_idx]
                if uri_idx < len(org["abstract"])
                else (org["abstract"][0] if org["abstract"] else None)
            )
            headquarters = (
                org["headquarters"][uri_idx]
                if uri_idx < len(org["headquarters"])
                else (org["headquarters"][0] if org["headquarters"] else None)
            )
            founding_date = (
                org["founding_date"][uri_idx]
                if uri_idx < len(org["founding_date"])
                else (org["founding_date"][0] if org["founding_date"] else None)
            )
            employees = (
                org["employees"][uri_idx]
                if uri_idx < len(org["employees"])
                else (org["employees"][0] if org["employees"] else None)
            )

            row = {
                "name": org["name"],
                "uri": uri,
                "abstract": abstract,
                "headquarters": headquarters,
                "founding_date": founding_date,
                "employees": employees,
                "locations": "|".join(org["locations"]) if org["locations"] else None,
                "countries": "|".join(org["countries"]) if org["countries"] else None,
            }
            orgs_rows.append(row)

    orgs_df = pd.DataFrame(orgs_rows)

    orgs_df = orgs_df.drop_duplicates(subset=["uri"])

    orgs_csv_path = f"{output_dir}semiconductor_organizations.csv"
    orgs_df.to_csv(orgs_csv_path, index=False)
    print(f"Saved {len(orgs_df)} unique organization records to {orgs_csv_path}")

    rel_rows = []
    seen_relationships = set()

    for source_uri, target_uris in relationships.items():
        for target_uri in target_uris:
            rel_id = f"{source_uri}||{target_uri}"

            if rel_id in seen_relationships:
                continue

            seen_relationships.add(rel_id)

            rel_rows.append({"source_uri": source_uri, "target_uri": target_uri})

    rel_df = pd.DataFrame(rel_rows)
    rel_csv_path = f"{output_dir}semiconductor_relationships.csv"
    rel_df.to_csv(rel_csv_path, index=False)
    print(f"Saved {len(rel_df)} unique relationship records to {rel_csv_path}")


file_path = "data/semiconductor_seed.csv"

seed_df = read_seed_list(file_path)

if seed_df is not None:
    organizations = seed_df["Name"].tolist()

    all_org_data, relationships = recursive_org_exploration(organizations, max_depth=3)
    save_results_to_csv(all_org_data, relationships)

    # For initial testing
    # test_orgs = organizations[:3]  # Just the first 3 orgs
    # print(f"Testing with: {test_orgs}")

    # all_org_data, relationships = recursive_org_exploration(test_orgs, max_depth=1)
    # save_results_to_csv(all_org_data, relationships)
