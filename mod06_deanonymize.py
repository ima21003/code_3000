import pandas as pd

def load_data(anonymized_path, auxiliary_path):
    """
    Load anonymized and auxiliary datasets.
    """
    anon = pd.read_csv(anonymized_path)
    aux = pd.read_csv(auxiliary_path)
    return anon, aux


def link_records(anon_df, aux_df):
    """
    Attempt to link anonymized records to auxiliary records
    using exact matching on quasi-identifiers.

    Returns a DataFrame with columns:
      anon_id, matched_name
    containing ONLY uniquely matched records.
    """
    #Find common columns 
    common_cols = list(set(anon_df.columns).intersection(set(aux_df.columns)))
    #remove identifier columns if present
    for col in ['anon_id', 'name']:
        if col in common_cols:
            common_cols.remove(col)
    #merge
    merged = pd.merge(anon_df, aux_df, on=common_cols)

    # Keep only unique matches per anonymized record
    counts = merged.groupby('anon_id').size()
    unique_ids = counts[counts == 1].index
    unique_matches = merged[merged['anon_id'].isin(unique_ids)]

    result = unique_matches[['anon_id', 'name']].rename(columns={'name': 'matched_name'})
    return result


def deanonymization_rate(matches_df, anon_df):
    """
    Compute the fraction of anonymized records
    that were uniquely re-identified.
    """
    return len(matches_df) / len(anon_df) #number of matched records/total anonymized records
