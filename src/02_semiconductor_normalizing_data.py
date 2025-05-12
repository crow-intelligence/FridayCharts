import json
import pickle
import time

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed
from tqdm import tqdm

semiconductor_raw_kg = pickle.load(open("data/semiconductor_kg.pickle", "rb"))

LM_STUDIO_API_URL = "http://localhost:1234/v1/chat/completions"

SINGLE_REQUEST_TIMEOUT = 20


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def normalize_company_name(company_name):
    """
    Normalize a single company name using DeepSeek-R1 via LM Studio

    Args:
        company_name (str): The company name to normalize

    Returns:
        str: The normalized company name
    """
    headers = {"Content-Type": "application/json"}

    payload = {
        "messages": [
            {
                "role": "system",
                "content": "You are a company name normalization assistant specialized in the semiconductor industry. Return only the normalized name without explanations.",
            },
            {
                "role": "user",
                "content": f"""Normalize this semiconductor industry company name: "{company_name}"

Instructions for normalization:
1. Remove legal entity suffixes (Inc, Corp, Ltd, GmbH, etc.)
2. Expand common abbreviations (Intl → International, Semi → Semiconductor, etc.)
3. Remove periods, commas, and parentheses
4. Preserve hyphens in proper names
5. Return only the canonical company name, nothing else

Example:
Input: "Taiwan Semiconductor Manufacturing Co., Ltd. (TSMC)"
Output: Taiwan Semiconductor Manufacturing""",
            },
        ],
        "temperature": 0.1,
        "max_tokens": 50,
    }

    try:
        response = requests.post(
            LM_STUDIO_API_URL,
            headers=headers,
            data=json.dumps(payload),
            timeout=SINGLE_REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        result = response.json()
        normalized_name = result["choices"][0]["message"]["content"].strip()

        normalized_name = normalized_name.replace('"', "").replace("'", "").strip()

        return normalized_name

    except requests.exceptions.RequestException as e:
        print(f"Error calling LM Studio API for '{company_name}': {e}")
        raise
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"Error parsing LM Studio response for '{company_name}': {e}")
        raise


def batch_normalize_company_names(company_names, batch_size=3):
    """
    Process a list of company names in smaller batches with better error handling

    Args:
        company_names (list): List of company names to normalize
        batch_size (int): Number of names to process in each batch (smaller is more reliable)

    Returns:
        dict: Mapping from original names to normalized names
    """
    results = {}

    for i in tqdm(range(0, len(company_names), batch_size)):
        batch = company_names[i : i + batch_size]

        for name in batch:
            if name in results:
                continue

            try:
                if i > 0 or batch.index(name) > 0:
                    time.sleep(1)

                normalized_name = normalize_company_name(name)
                results[name] = normalized_name

            except Exception as e:
                print(f"Error processing '{name}': {e}")
                results[name] = name
                time.sleep(3)

    return results


def normalize_with_smaller_batches_and_fallbacks(company_names):
    """
    A more robust approach that processes very small batches
    and has multiple fallback mechanisms

    Args:
        company_names (list): List of company names to normalize

    Returns:
        dict: Mapping from original names to normalized names
    """
    results = {}
    failed_names = []

    print("First pass - processing companies...")
    for i in tqdm(range(0, len(company_names), 1)):
        name = company_names[i]

        try:
            if i > 0:
                time.sleep(1.5)

            normalized_name = normalize_company_name(name)
            results[name] = normalized_name

        except Exception as e:
            print(f"First pass: Failed to process '{name}': {e}")
            failed_names.append(name)
            time.sleep(2)

    if failed_names:
        print(f"\nSecond pass - retrying {len(failed_names)} failed companies...")
        for name in tqdm(failed_names):
            try:
                time.sleep(3)
                normalized_name = normalize_company_name(name)
                results[name] = normalized_name
            except Exception as e:
                print(f"Second pass: Failed to process '{name}': {e}")
                results[name] = simple_company_name_cleanup(name)

    return results


def simple_company_name_cleanup(company_name):
    """
    A simple rule-based fallback for company name normalization
    when the LLM method fails

    Args:
        company_name (str): The company name to normalize

    Returns:
        str: The normalized company name
    """
    import re

    name = company_name.lower()
    legal_suffixes = [
        r"\s+inc\.?$",
        r"\s+incorporated$",
        r"\s+corp\.?$",
        r"\s+corporation$",
        r"\s+ltd\.?$",
        r"\s+limited$",
        r"\s+llc$",
        r"\s+l\.l\.c\.?$",
        r"\s+gmbh$",
        r"\s+co\.?$",
        r"\s+company$",
        r"\s+group$",
        r"\s+holdings?$",
    ]

    for suffix in legal_suffixes:
        name = re.sub(suffix, "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s*\([^)]*\)", "", name)
    name = re.sub(r"[^\w\s-]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name.title()


def normalize_from_csv(input_csv, output_csv, name_column="Name"):
    """
    Normalize company names from a CSV file using the most robust method

    Args:
        input_csv (str): Path to input CSV file
        output_csv (str): Path to output CSV file
        name_column (str): Name of the column containing company names
    """
    df = pd.read_csv(input_csv)

    if name_column not in df.columns:
        print(
            f"Error: Column '{name_column}' not found in CSV. Available columns: {df.columns.tolist()}"
        )
        return

    company_names = df[name_column].tolist()
    print(f"Found {len(company_names)} company names to normalize")

    normalized_names = normalize_with_smaller_batches_and_fallbacks(company_names)

    df["Normalized_Name"] = df[name_column].map(normalized_names)

    df.to_csv(output_csv, index=False)
    print(f"Normalized names saved to {output_csv}")

    print("\nSample of normalized names:")
    sample = df[[name_column, "Normalized_Name"]].head(5)
    print(sample.to_string())


if __name__ == "__main__":
    normalized_dict = batch_normalize_company_names(semiconductor_raw_kg["nodes"])

    with open("normalized_dict.json", "w") as outfile:
        json.dump(normalized_dict, outfile)

    print("\nBatch normalization results:")
    for original, normalized in normalized_dict.items():
        print(f"Original: {original}")
        print(f"Normalized: {normalized}")
        print("-" * 40)
