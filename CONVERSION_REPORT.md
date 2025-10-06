# Azure Synapse to Fabric PySpark Conversion

## PHASE 1 — AUDIT

### 1. Data Flow Name
**df_l2_text_values**

### 2. Global Settings & Parameters
**Parameters:**
- `schema`: string, default 'gold'
- `table`: string, default 'l2_data_dictionary_text_values_v2'
- `folderName`: string, default '3rd-party/l2/scratch/text_values/'
- `insertDate`: timestamp, default currentTimestamp()

**Global Settings:**
- Schema drift: Allowed on all sources (`allowSchemaDrift: true`)
- Schema validation: Disabled on all sources (`validateSchema: false`)
- Row limits: None specified
- Null handling: Default PySpark behavior
- Broadcast hints: None explicitly defined
- Partitioning: None specified
- Optimization flags: `skipDuplicateMapInputs/Outputs: true` on sinks

### 3. Flowlet Inventory
**No flowlets referenced in this data flow.**

### 4. Parent Components (Topological Order)

| Order | Type | OriginalName | SanitizedMethodName | Inputs | Outputs | Key Properties |
|-------|------|--------------|-------------------|---------|---------|----------------|
| 1 | Source | jsonFiles | df_json_files | None | name, sort_order, value | JSON format, wildcard path, allowSchemaDrift |
| 2 | Source | oldRecords | df_old_records | None | name, value, sort_order, dt_created | Synapse table, READ_UNCOMMITTED, staged |
| 3 | Source | newRecords | df_new_records | None | name, sort_order, value | JSON format, identical to jsonFiles |
| 4 | Aggregate | distinctDataPointNames | df_distinct_data_point_names | jsonFiles | name, values | groupBy(name), collectUnique(value) |
| 5 | DerivedColumn | addLookupColumn | df_add_lookup_column | oldRecords, cachedNames | All oldRecords + lookup | Cache lookup: cachedNames#lookup(name).name |
| 6 | Filter | findDeletes | df_find_deletes | addLookupColumn | Filtered records | Predicate: not(isNull(lookup)) |
| 7 | AlterRow | setDelete | df_set_delete | findDeletes | Delete-marked records | deleteIf(true()) |
| 8 | Sort | sortNewRecords | df_sort_new_records | newRecords | Sorted records | asc(name), asc(sort_order) |
| 9 | DerivedColumn | addCreatedDate | df_add_created_date | sortNewRecords | All inputs + dt_created | dt_created = $insertDate |
| 10 | Select | rearrangeColumns | df_rearrange_columns | addCreatedDate | name, value, sort_order, dt_created | mapColumn selection |
| 11 | AlterRow | setInserts | df_set_inserts | rearrangeColumns | Insert-marked records | insertIf(true()) |
| 12 | Sink | cachedNames | df_cached_names | distinctDataPointNames | Cache storage | Cache store, keys: ['name'], saveOrder: 1 |
| 13 | Sink | deleteRecords | df_delete_records | setDelete | Table deletions | Synapse table, deletable: true, saveOrder: 2 |
| 14 | Sink | insertRecords | df_insert_records | setInserts | Table insertions | Synapse table, insertable: true, saveOrder: 3 |

### 5. Flowlet Internal Components
**No flowlets in this data flow.**

### 6. Flowlet Invocation Points
**No flowlet invocations in this data flow.**

### 7. Execution Graph (Expanded)
```
jsonFiles ──┬── distinctDataPointNames ── cachedNames [sink:1]
            │
            └── [same as newRecords]

oldRecords ── addLookupColumn ── findDeletes ── setDelete ── deleteRecords [sink:2]
                │
                └── (cache lookup to cachedNames)

newRecords ── sortNewRecords ── addCreatedDate ── rearrangeColumns ── setInserts ── insertRecords [sink:3]
```

### 8. Sinks Summary
**cachedNames:**
- Type: Cache, Format: inline, Mode: Cache storage
- Target: Memory cache, Keys: ['name'], Partitioning: None
- Options: validateSchema: false, output: false, saveOrder: 1

**deleteRecords:**
- Type: Table, Format: Synapse Analytics, Mode: Delete operation
- Target: {schema}.{table} (parameterized), Keys: ['name']
- Options: staged: true, allowCopyCommand: true, saveOrder: 2
- Merge semantics: Delete records where name matches

**insertRecords:**
- Type: Table, Format: Synapse Analytics, Mode: Insert operation
- Target: {schema}.{table} (parameterized), Partitioning: None
- Options: staged: true, allowCopyCommand: true, saveOrder: 3
- Merge semantics: Append new records

### 9. Notes / Warnings
**Unsupported constructs:**
- Cache lookup: `cachedNames#lookup(name).name` - approximated with temp view and join
- AlterRow operations: `deleteIf()` and `insertIf()` - converted to Delta merge and append
- Synapse-specific features: `allowCopyCommand`, `staged`, isolation levels

**Unresolved references:**
- Storage account name: `prodhspsynapsefs@<storage_account>` - requires configuration
- Linked services: All references need Fabric workspace configuration

**Schema ambiguities:**
- Duplicate sources: jsonFiles and newRecords load identical data
- Data type inconsistency: sort_order (integer vs short) between sources

## PHASE 2 — CODE GENERATION
