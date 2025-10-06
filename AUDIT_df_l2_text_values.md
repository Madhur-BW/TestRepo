# PHASE 1 — AUDIT

## 1. Data Flow Name
**df_l2_text_values**

## 2. Global Settings & Parameters
- **Parameters:**
  - `schema`: string, default 'gold'
  - `table`: string, default 'l2_data_dictionary_text_values_v2'
  - `folderName`: string, default '3rd-party/l2/scratch/text_values/'
  - `insertDate`: timestamp, default currentTimestamp()

- **Global Settings:**
  - Schema drift: Allowed on all sources (`allowSchemaDrift: true`)
  - Schema validation: Disabled on all sources (`validateSchema: false`)
  - Null handling: Default PySpark behavior
  - Broadcast hints: None explicitly defined
  - Partitioning: None specified
  - Optimization flags: `skipDuplicateMapInputs/Outputs: true` on sinks

## 3. Flowlet Inventory
**No flowlets referenced in this data flow.**

## 4. Parent Components (Topological Order)

### Order 1-3: Sources
1. **Type:** Source  
   **OriginalName:** jsonFiles  
   **SanitizedMethodName:** df_json_files  
   **Inputs:** None (source)  
   **Outputs:** name, sort_order, value  
   **Key Properties:** JSON format, wildcard path with parameter, documentPerLine, allowSchemaDrift

2. **Type:** Source  
   **OriginalName:** oldRecords  
   **SanitizedMethodName:** df_old_records  
   **Inputs:** None (source)  
   **Outputs:** name, value, sort_order, dt_created  
   **Key Properties:** Synapse Analytics table, READ_UNCOMMITTED isolation, staged: true

3. **Type:** Source  
   **OriginalName:** newRecords  
   **SanitizedMethodName:** df_new_records  
   **Inputs:** None (source)  
   **Outputs:** name, sort_order, value  
   **Key Properties:** JSON format, identical to jsonFiles

### Order 4-11: Transformations
4. **Type:** Aggregate  
   **OriginalName:** distinctDataPointNames  
   **SanitizedMethodName:** df_distinct_data_point_names  
   **Inputs:** jsonFiles  
   **Outputs:** name, values (collected)  
   **Key Properties:** Group by name, collectUnique(value)

5. **Type:** DerivedColumn  
   **OriginalName:** addLookupColumn  
   **SanitizedMethodName:** df_add_lookup_column  
   **Inputs:** oldRecords, cachedNames (cache lookup)  
   **Outputs:** All oldRecords columns + lookup  
   **Key Properties:** Cache lookup: cachedNames#lookup(name).name

6. **Type:** Filter  
   **OriginalName:** findDeletes  
   **SanitizedMethodName:** df_find_deletes  
   **Inputs:** addLookupColumn  
   **Outputs:** Filtered records where lookup is not null  
   **Key Properties:** Predicate: not(isNull(lookup))

7. **Type:** AlterRow  
   **OriginalName:** setDelete  
   **SanitizedMethodName:** df_set_delete  
   **Inputs:** findDeletes  
   **Outputs:** Records marked for deletion  
   **Key Properties:** deleteIf(true()) - unconditional delete

8. **Type:** Sort  
   **OriginalName:** sortNewRecords  
   **SanitizedMethodName:** df_sort_new_records  
   **Inputs:** newRecords  
   **Outputs:** Sorted records  
   **Key Properties:** Sort by name (asc), sort_order (asc), nulls last

9. **Type:** DerivedColumn  
   **OriginalName:** addCreatedDate  
   **SanitizedMethodName:** df_add_created_date  
   **Inputs:** sortNewRecords  
   **Outputs:** All input columns + dt_created  
   **Key Properties:** dt_created = $insertDate parameter

10. **Type:** Select  
    **OriginalName:** rearrangeColumns  
    **SanitizedMethodName:** df_rearrange_columns  
    **Inputs:** addCreatedDate  
    **Outputs:** name, value, sort_order, dt_created  
    **Key Properties:** Column mapping, skip duplicates

11. **Type:** AlterRow  
    **OriginalName:** setInserts  
    **SanitizedMethodName:** df_set_inserts  
    **Inputs:** rearrangeColumns  
    **Outputs:** Records marked for insertion  
    **Key Properties:** insertIf(true()) - unconditional insert

### Order 12-14: Sinks
12. **Type:** Sink  
    **OriginalName:** cachedNames  
    **SanitizedMethodName:** df_cached_names  
    **Inputs:** distinctDataPointNames  
    **Outputs:** Cache storage  
    **Key Properties:** Cache store, inline format, keys: ['name'], saveOrder: 1

13. **Type:** Sink  
    **OriginalName:** deleteRecords  
    **SanitizedMethodName:** df_delete_records  
    **Inputs:** setDelete  
    **Outputs:** Table with deletions applied  
    **Key Properties:** Synapse table, deletable: true, keys: ['name'], saveOrder: 2

14. **Type:** Sink  
    **OriginalName:** insertRecords  
    **SanitizedMethodName:** df_insert_records  
    **Inputs:** setInserts  
    **Outputs:** Table with insertions applied  
    **Key Properties:** Synapse table, insertable: true, saveOrder: 3

## 5. Flowlet Internal Components
**No flowlets in this data flow.**

## 6. Flowlet Invocation Points
**No flowlet invocations in this data flow.**

## 7. Execution Graph (Expanded)
```
jsonFiles ──┬── distinctDataPointNames ── cachedNames [sink:1]
            │
            └── [same as newRecords]

oldRecords ── addLookupColumn ── findDeletes ── setDelete ── deleteRecords [sink:2]
                │
                └── (cache lookup to cachedNames)

newRecords ── sortNewRecords ── addCreatedDate ── rearrangeColumns ── setInserts ── insertRecords [sink:3]
```

**Execution Order by saveOrder:**
1. cachedNames (cache for lookups)
2. deleteRecords (remove outdated records)
3. insertRecords (add new records)

## 8. Sinks Summary

### cachedNames
- **Type:** Cache  
- **Format:** Inline  
- **Mode:** Cache storage  
- **Target:** Memory cache  
- **Keys:** ['name']  
- **Partitioning:** None  
- **Options:** validateSchema: false, output: false  
- **saveOrder:** 1

### deleteRecords
- **Type:** Table  
- **Format:** Synapse Analytics  
- **Mode:** Delete operation  
- **Target:** {schema}.{table} (parameterized)  
- **Keys:** ['name']  
- **Partitioning:** None  
- **Options:** staged: true, allowCopyCommand: true  
- **saveOrder:** 2  
- **Merge Semantics:** Delete records where name matches

### insertRecords
- **Type:** Table  
- **Format:** Synapse Analytics  
- **Mode:** Insert operation  
- **Target:** {schema}.{table} (parameterized)  
- **Partitioning:** None  
- **Options:** staged: true, allowCopyCommand: true  
- **saveOrder:** 3  
- **Merge Semantics:** Append new records

## 9. Notes / Warnings

### Schema Ambiguities
- **jsonFiles vs newRecords:** Both sources read from the same path with identical schemas, potentially redundant
- **sort_order data type:** Inconsistent between sources (integer vs short)

### Unsupported Constructs
- **Cache lookup:** `cachedNames#lookup(name).name` - Fabric doesn't have direct cache lookup equivalent, approximated with temp view and join
- **AlterRow operations:** `deleteIf()` and `insertIf()` - Converted to Delta merge operations and append mode
- **Synapse-specific features:** `allowCopyCommand`, `staged`, isolation levels - Not directly applicable to Fabric

### Unresolved References
- **Storage account name:** `prodhspsynapsefs@<storage_account>` - Placeholder requires actual storage account configuration
- **Linked services:** All linked service references need to be configured in Fabric workspace

### Performance Considerations
- **Duplicate source loading:** jsonFiles and newRecords load the same data - could be optimized
- **Cache replacement:** Temp view used instead of native cache may impact performance for large datasets
- **Delete operations:** Using Delta merge instead of direct delete operations

### Parameter Dependencies
- All parameters have defaults but folderName concatenation in wildcard paths requires proper escaping
- insertDate parameter using currentTimestamp() - behavior may differ slightly between Synapse and Fabric

### Data Type Conversions
- **Timestamp handling:** Synapse timestamp vs Spark timestamp semantics
- **Short vs Integer:** sort_order type inconsistency between old and new records may cause join issues