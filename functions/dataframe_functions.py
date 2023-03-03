
# Vertically concatenate (i.e., append) a list of dataframes
def vertically_concatenate_dfs(list_of_dfs):

    import pandas as pd

    # For every df provided
    for df in list_of_dfs:
        # If it's a dataframe
        if isinstance(df, pd.DataFrame):
            # Create a new df if this will be the base df
            if 'df_concatenated' not in locals():
                df_concatenated = df
            # Otherwise vertically concatenate
            else:
                df_concatenated = pd.concat([df_concatenated, df])

    return df_concatenated

# Reset index of dataframe (use after deleting rows)
def reset_df_index(df):
    import pandas as pd
    df.reset_index(drop=True, inplace=True)
    return df

# Dataframe to tuples
def df_to_tuples(df, return_named_tuples=False):

    if return_named_tuples:
        return list(df.itertuples(index=False))
    else:
        return list(df.itertuples(index=False, name=None))