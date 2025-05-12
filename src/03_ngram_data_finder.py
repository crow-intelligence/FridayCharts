import os

import pandas as pd

df_file = "data/ngram_helpers/terms.csv"
input_path = "data/ngram_helpers"
metafiles = [f for f in os.listdir(input_path) if f.endswith(".txt")]


def find_file_for_term(term, files_with_first_terms):
    """
    Given a term and a list of (filename, first_term) tuples sorted by first_term,
    returns the filename where the term would be found.

    Parameters:
    - term: The search term (string).
    - files_with_first_terms: List of tuples (filename, first_term), sorted by first_term.

    Returns:
    - The filename (string) containing the term.
    """
    result = None
    for filename, first_term in files_with_first_terms:
        if first_term <= term:
            result = filename
        else:
            break
    return result


df = pd.read_csv(os.path.join(input_path, df_file))
for _, row in df.iterrows():
    term = row["term"]
