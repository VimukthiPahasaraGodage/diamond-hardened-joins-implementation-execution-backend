le_decomposition_join_function = r"""
def le_decomposition_join(LE):
    if len(LE_stacks_2[LE]) != 2:
        raise ValueError('There should be exactly two Expand operations and two Lookup operations in the LE_stack_2')

    # There will be exactly two lookups and two expands for the L&E Decomposition(Note this is a limited implementation)
    first = LE_stacks_2[LE][0]
    second = LE_stacks_2[LE][1]

    first_l, first_r = children[first['nid']]
    second_l, second_r = children[second['nid']]
    df_2 = dfs[second_l]
    df_3 = dfs[second_r]
    ht_2_key = []
    ht_3_key = []
    if op_type[first_r] == 'Lookup':
        df_1 = dfs[first_l]
        for i in range(len(first['left_on'])):
            if 0 <= first['right_on'][i] < df_2.shape[1]:
                ht_2_key.append({'df_1': first['left_on'][i], 'df_2': first['right_on'][i]})
            elif df_2.shape[1] <= first['right_on'][i] < (df_2.shape[1] + df_3.shape[1]):
                ht_3_key.append({'df_1': first['left_on'][i], 'df_3': first['right_on'][i] - df_2.shape[1]})
            else:
                raise ValueError("Join index out of bound")
        df_1_left = True
    elif op_type[first_l] == 'Lookup':
        df_1 = dfs[first_r]
        for i in range(len(first['right_on'])):
            if 0 <= first['left_on'][i] < df_2.shape[1]:
                ht_2_key.append({'df_1': first['right_on'][i], 'df_2': first['left_on'][i]})
            elif df_2.shape[1] <= first['left_on'][i] < (df_2.shape[1] + df_3.shape[1]):
                ht_3_key.append({'df_1': first['right_on'][i], 'df_3': first['left_on'][i] - df_2.shape[1]})
            else:
                raise ValueError('Join index out of bound')
        df_1_left = False
    else:
        raise ValueError('The nodes for Lookup and Expand are not in the standard')

    for i in range(len(second['left_on'])):
        ht_3_key.append({'df_2': second['left_on'][i], 'df_3': second['right_on'][i]})

    ht_2 = defaultdict(list)
    hash_key_col_indices_ht_2 = []
    probe_key_col_indices_ht_2 = []
    for item in ht_2_key:
        hash_key_col_indices_ht_2.append(item['df_2'])
        probe_key_col_indices_ht_2.append(item['df_1'])
    for index, row in df_2.iterrows():
        key = tuple(df_2.loc[index, hash_key_col_indices_ht_2])
        ht_2[key].append(row)

    ht_3 = defaultdict(list)
    hash_key_col_indices_ht_3 = []
    probe_key_col_indices_ht_3_df_1 = []
    probe_key_col_indices_ht_3_df_2 = []
    for item in ht_3_key:
        hash_key_col_indices_ht_3.append(item['df_3'])
        if 'df_1' in item:
            probe_key_col_indices_ht_3_df_1.append(item['df_1'])
        elif 'df_2' in item:
            probe_key_col_indices_ht_3_df_2.append(item['df_2'])
    for index, row in df_3.iterrows():
        key = tuple(df_3.loc[index, hash_key_col_indices_ht_3])
        ht_3[key].append(row)

    join_results = []
    for r1_index, r1 in df_1.iterrows():
        ht2_key = []
        for index in probe_key_col_indices_ht_2:
            ht2_key.append(r1.iloc[index])
        ht2_key = tuple(ht2_key)
        for r2 in ht_2.get(ht2_key, []):
            ht3_key = []
            for index in probe_key_col_indices_ht_3_df_1:
                ht3_key.append(r1.iloc[index])
            for index in probe_key_col_indices_ht_3_df_2:
                ht3_key.append(r2.iloc[index])
            ht3_key = tuple(ht3_key)
            for r3 in ht_3.get(ht3_key, []):
                if df_1_left:
                    join_results.append(r1.tolist() + r2.tolist() + r3.tolist())
                else:
                    join_results.append(r2.tolist() + r3.tolist() + r1.tolist())
    df = pd.DataFrame(join_results, columns=range(df_1.shape[1] + df_2.shape[1] + df_3.shape[1]))
    return df
"""