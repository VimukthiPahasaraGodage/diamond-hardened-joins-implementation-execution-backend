multi_threaded_le_decomposition_join_function = r"""
def le_decomposition_join(
    LE,
    num_threads_join=None,
    num_threads_ht2=None,
    num_threads_ht3=None
):

    # ------------------------------------------------------------
    # 1) SET DEFAULT THREAD COUNTS
    # ------------------------------------------------------------
    cpu = os.cpu_count() or 1

    if num_threads_join is None:
        num_threads_join = cpu
    if num_threads_ht2 is None:
        num_threads_ht2 = cpu
    if num_threads_ht3 is None:
        num_threads_ht3 = cpu

    # ------------------------------------------------------------
    # 2) VALIDATE & EXTRACT THE TWO LOOKUP/EXPAND OPS
    # ------------------------------------------------------------
    if len(LE_stacks_2[LE]) != 2:
        raise ValueError('There should be exactly two Expand operations and two Lookup operations in the LE_stack_2')

    first = LE_stacks_2[LE][0]
    second = LE_stacks_2[LE][1]

    first_l, first_r = children[first['nid']]
    second_l, second_r = children[second['nid']]

    df_2 = dfs[second_l]
    df_3 = dfs[second_r]

    # ------------------------------------------------------------
    # 3) DETERMINE ht_2_key, ht_3_key, AND WHICH SIDE IS df_1
    # ------------------------------------------------------------
    ht_2_key = []
    ht_3_key = []

    if op_type[first_r] == 'Lookup':
        df_1 = dfs[first_l]
        for i in range(len(first['left_on'])):
            right_idx = first['right_on'][i]
            if 0 <= right_idx < df_2.shape[1]:
                ht_2_key.append({'df_1': first['left_on'][i], 'df_2': right_idx})
            elif df_2.shape[1] <= right_idx < (df_2.shape[1] + df_3.shape[1]):
                ht_3_key.append({
                    'df_1': first['left_on'][i],
                    'df_3': right_idx - df_2.shape[1]
                })
            else:
                raise ValueError("Join index out of bound for df_2/df_3")
        df_1_left = True

    elif op_type[first_l] == 'Lookup':
        df_1 = dfs[first_r]
        for i in range(len(first['right_on'])):
            left_idx = first['left_on'][i]
            if 0 <= left_idx < df_2.shape[1]:
                ht_2_key.append({'df_1': first['right_on'][i], 'df_2': left_idx})
            elif df_2.shape[1] <= left_idx < (df_2.shape[1] + df_3.shape[1]):
                ht_3_key.append({
                    'df_1': first['right_on'][i],
                    'df_3': left_idx - df_2.shape[1]
                })
            else:
                raise ValueError('Join index out of bound for df_2/df_3')
        df_1_left = False

    else:
        raise ValueError('The nodes for Lookup and Expand are not in the expected configuration')

    # Always add the second lookup (df_2 → df_3) into ht_3_key
    for i in range(len(second['left_on'])):
        ht_3_key.append({
            'df_2': second['left_on'][i],
            'df_3': second['right_on'][i]
        })

    # ------------------------------------------------------------
    # 4) PARTIAL HASH‐TABLE BUILDERS (FIXED ht_3)
    # ------------------------------------------------------------
    def build_partial_ht2(row_indices):
        local_ht2 = defaultdict(list)
        hash_cols = [item['df_2'] for item in ht_2_key]
        if not hash_cols:
            return local_ht2

        for idx in row_indices:
            row = df_2.iloc[idx]
            key = tuple(row[col] for col in hash_cols)
            local_ht2[key].append(row)
        return local_ht2

    def build_partial_ht3(row_indices):
        local_ht3 = defaultdict(list)
        # === FIXED: include every item['df_3'], not just those without 'df_2' ===
        hash_cols_3 = [item['df_3'] for item in ht_3_key]

        if not hash_cols_3:
            return local_ht3

        for idx in row_indices:
            row = df_3.iloc[idx]
            key = tuple(row[col] for col in hash_cols_3)
            local_ht3[key].append(row)
        return local_ht3

    # ------------------------------------------------------------
    # 5) PARTITION df_2 & df_3 INDICES INTO CHUNKS
    # ------------------------------------------------------------
    def partition_indices(total_rows, num_parts):
        base_size = total_rows // num_parts
        remainder = total_rows % num_parts
        partitions = []
        start = 0
        for i in range(num_parts):
            size = base_size + (1 if i < remainder else 0)
            end = start + size
            if start >= total_rows:
                break
            partitions.append(list(range(start, min(end, total_rows))))
            start = end
        return partitions

    idx_partitions_2 = partition_indices(df_2.shape[0], num_threads_ht2)
    idx_partitions_3 = partition_indices(df_3.shape[0], num_threads_ht3)

    # ------------------------------------------------------------
    # 6) BUILD ALL PARTIAL ht_2 & ht_3 IN PARALLEL, THEN MERGE
    # ------------------------------------------------------------
    global_ht2 = defaultdict(list)
    global_ht3 = defaultdict(list)

    # Build ht_2 in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads_ht2) as executor:
        futures_ht2 = [
            executor.submit(build_partial_ht2, part_indices)
            for part_indices in idx_partitions_2
        ]
        for fut in concurrent.futures.as_completed(futures_ht2):
            partial = fut.result()
            for key, rows in partial.items():
                global_ht2[key].extend(rows)

    # Build ht_3 in parallel (now hashing on all df_3 columns from ht_3_key)
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads_ht3) as executor:
        futures_ht3 = [
            executor.submit(build_partial_ht3, part_indices)
            for part_indices in idx_partitions_3
        ]
        for fut in concurrent.futures.as_completed(futures_ht3):
            partial = fut.result()
            for key, rows in partial.items():
                global_ht3[key].extend(rows)

    ht_2 = global_ht2
    ht_3 = global_ht3

    # ------------------------------------------------------------
    # 7) PREPARE PROBE‐KEY COLUMN LISTS
    # ------------------------------------------------------------
    probe_ht2_cols = [item['df_1'] for item in ht_2_key]
    probe_ht3_cols_df1 = [item['df_1'] for item in ht_3_key if 'df_1' in item]
    probe_ht3_cols_df2 = [item['df_2'] for item in ht_3_key if 'df_2' in item]

    # ------------------------------------------------------------
    # 8) DEFINE A WORKER FOR THE FINAL JOIN‐PROBE PHASE
    # ------------------------------------------------------------
    def join_worker(start_idx, end_idx):
        local_results = []
        for r1_idx in range(start_idx, end_idx):
            r1 = df_1.iloc[r1_idx]

            # 8a) Build the key to probe ht_2
            key_ht2 = tuple(r1[col] for col in probe_ht2_cols)
            matching_rows_2 = ht_2.get(key_ht2, [])

            # 8b) For each matched row in df_2, probe ht_3
            for r2 in matching_rows_2:
                # Build the ht_3 key using r1 & r2
                key_parts = []
                for col in probe_ht3_cols_df1:
                    key_parts.append(r1[col])
                for col in probe_ht3_cols_df2:
                    key_parts.append(r2[col])
                key_ht3 = tuple(key_parts)

                for r3 in ht_3.get(key_ht3, []):
                    if df_1_left:
                        local_results.append(r1.tolist() + r2.tolist() + r3.tolist())
                    else:
                        local_results.append(r2.tolist() + r3.tolist() + r1.tolist())
        return local_results

    # ------------------------------------------------------------
    # 9) SPLIT df_1 INTO CHUNKS AND RUN join_worker IN PARALLEL
    # ------------------------------------------------------------
    total_rows_1 = df_1.shape[0]
    chunk_size_1 = math.ceil(total_rows_1 / num_threads_join)
    boundaries_1 = [
        (i * chunk_size_1, min((i + 1) * chunk_size_1, total_rows_1))
        for i in range(num_threads_join)
        if i * chunk_size_1 < total_rows_1
    ]

    join_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads_join) as executor:
        futures_join = [
            executor.submit(join_worker, start, end)
            for (start, end) in boundaries_1
        ]
        for fut in concurrent.futures.as_completed(futures_join):
            join_results.extend(fut.result())

    # ------------------------------------------------------------
    # 10) ASSEMBLE FINAL DATAFRAME
    # ------------------------------------------------------------
    total_cols = df_1.shape[1] + df_2.shape[1] + df_3.shape[1]
    result_df = pd.DataFrame(join_results, columns=list(range(total_cols)))
    return result_df
"""

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