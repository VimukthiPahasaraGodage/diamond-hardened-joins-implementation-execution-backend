multithreaded_hash_join_function = r"""
def hash_join(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        left_on: List[int],
        right_on: List[int],
        max_workers: int = 8
) -> pd.DataFrame:
    # Determine build and probe sides
    if len(left_df) <= len(right_df):
        build_df, build_keys, probe_df, probe_keys = left_df, left_on, right_df, right_on
        swap = False
    else:
        build_df, build_keys, probe_df, probe_keys = right_df, right_on, left_df, left_on
        swap = True

    # Build hash table
    hash_table: Dict[Tuple, List[int]] = defaultdict(list)
    for idx, row in build_df.iterrows():
        key = tuple(row.iloc[build_keys].tolist())
        hash_table[key].append(idx)

    # Partition probe_df into chunks
    indices = probe_df.index.tolist()
    chunk_size = max(1, len(indices) // max_workers)
    chunks = [indices[i:i + chunk_size] for i in range(0, len(indices), chunk_size)]

    # Worker to probe a chunk
    def probe_chunk(chunk_idx_list: List[int]) -> List[Tuple[int, int]]:
        matches: List[Tuple[int, int]] = []  # list of (build_idx, probe_idx)
        for p_idx in chunk_idx_list:
            prob_row = probe_df.loc[p_idx]
            key = tuple(prob_row.iloc[probe_keys].tolist())
            for b_idx in hash_table.get(key, []):
                # depending on swap, b_idx/probe_idx correspond to left/right
                if swap:
                    matches.append((p_idx, b_idx))
                else:
                    matches.append((b_idx, p_idx))
        return matches

    # Parallel probe
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(probe_chunk, chunk) for chunk in chunks]
        all_matches: List[Tuple[int, int]] = []
        for fut in futures:
            all_matches.extend(fut.result())

    if not all_matches:
        # empty join
        return pd.DataFrame(columns=list(left_df.columns) + list(right_df.columns))

    # Extract joined rows
    left_idxs, right_idxs = zip(*all_matches)
    left_part = left_df.loc[list(left_idxs)].reset_index(drop=True)
    right_part = right_df.loc[list(right_idxs)].reset_index(drop=True)
    joined = pd.concat([left_part.reset_index(drop=True), right_part.reset_index(drop=True)], axis=1)
    return joined
"""

partitioned_hash_join_function = r"""
def hash_join(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        left_on: List[str],
        right_on: List[str]
) -> pd.DataFrame:
    # 1. Determine build vs. probe side
    if len(left_df) <= len(right_df):
        build_df, build_keys, probe_df, probe_keys = left_df, left_on, right_df, right_on
        left_is_build = True
    else:
        build_df, build_keys, probe_df, probe_keys = right_df, right_on, left_df, left_on
        left_is_build = False
        
    # 2. Compute partition count based on NUMBER OF ROWS
    TARGET_SIZE = 50000
    num_rows = probe_df.shape[0]
    P = max(1, (num_rows + TARGET_SIZE - 1) // TARGET_SIZE)
    max_workers = min(P, os.cpu_count() or P)

    # 2. Compute partition ID for each row
    def compute_partitions(df: pd.DataFrame, keys: List[str]) -> pd.Series:
        # vectorized tuple-of-keys column
        tuples = list(zip(*(df[k] for k in keys)))
        # hash & mod P
        return pd.Series([hash(t) % P for t in tuples], index=df.index)

    build_parts = compute_partitions(build_df, build_keys)
    probe_parts = compute_partitions(probe_df, probe_keys)

    # 3. Group row-indices by partition
    build_groups = {i: build_parts[build_parts == i].index.tolist() for i in range(P)}
    probe_groups = {i: probe_parts[probe_parts == i].index.tolist() for i in range(P)}

    # 4. Worker: join a single partition
    def join_partition(pid: int) -> List[Tuple[int, int]]:
        b_idxs = build_groups[pid]
        p_idxs = probe_groups[pid]
        if not b_idxs or not p_idxs:
            return []

        # build hash table for this partition
        ht = defaultdict(list)
        for b in b_idxs:
            key = tuple(build_df.loc[b, build_keys])
            ht[key].append(b)

        matches = []
        for p in p_idxs:
            key = tuple(probe_df.loc[p, probe_keys])
            for b in ht.get(key, []):
                # record build/probe index pair
                matches.append((b, p))
        return matches

    # 5. Parallel join across partitions
    all_matches: List[Tuple[int, int]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures_ = [exe.submit(join_partition, pid) for pid in range(P)]
        for f_ in futures_:
            all_matches.extend(f_.result())

    # 6. Build final DataFrame
    if not all_matches:
        # no matches → empty result with appropriate columns
        return pd.DataFrame(columns=list(left_df.columns) + list(right_df.columns))

    build_idxs, probe_idxs = zip(*all_matches)
    build_slice = build_df.loc[list(build_idxs)].reset_index(drop=True)
    probe_slice = probe_df.loc[list(probe_idxs)].reset_index(drop=True)

    # Reorder columns so left always comes first
    if left_is_build:
        joined = pd.concat([build_slice, probe_slice], axis=1)
    else:
        joined = pd.concat([probe_slice, build_slice], axis=1)

    return joined
"""
