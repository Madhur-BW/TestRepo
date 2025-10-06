# %run "./Fabric_nb_flowlet_<flowlet_name>" # Use this pattern if flowlets are needed

from pyspark.sql.functions import col, lit, expr, when, collect_list, asc, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable

class DfL2TextValues:
    """
    Fabric-compatible PySpark conversion of Azure Synapse Mapping Data Flow: df_l2_text_values
    
    Purpose: Process text values from JSON files, compare with existing records,
    and perform delete/insert operations for data synchronization.
    """
    
    def __init__(self):
        # Parameters from original data flow
        self.schema = 'gold'
        self.table = 'l2_data_dictionary_text_values_v2'
        self.folder_name = '3rd-party/l2/scratch/text_values/'
        self.insert_date = current_timestamp()
        
        # DataFrames for component outputs
        self.df_json_files = None
        self.df_old_records = None
        self.df_new_records = None
        self.df_distinct_data_point_names = None
        self.df_add_lookup_column = None
        self.df_find_deletes = None
        self.df_set_delete = None
        self.df_sort_new_records = None
        self.df_add_created_date = None
        self.df_rearrange_columns = None
        self.df_set_inserts = None
    
    def df_json_files(self):
        """Source: jsonFiles - Load JSON files from data lake"""
        # Original: jsonFiles
        path = f"abfss://prodhspsynapsefs@<storage_account>.dfs.core.windows.net/{self.folder_name}*.json"
        
        self.df_json_files = (spark.read
                             .format("json")
                             .option("multiline", "false")
                             .load(path)
                             .select(
                                 col("name").cast("string"),
                                 col("sort_order").cast("integer"),
                                 col("value").cast("string")
                             ))
        return self.df_json_files
    
    def df_old_records(self):
        """Source: oldRecords - Load existing records from Synapse table"""
        # Original: oldRecords
        table_name = f"{self.schema}.{self.table}"
        
        self.df_old_records = (spark.table(table_name)
                              .select(
                                  col("name").cast("string"),
                                  col("value").cast("string"),
                                  col("sort_order").cast("short"),
                                  col("dt_created").cast("timestamp")
                              ))
        return self.df_old_records
    
    def df_new_records(self):
        """Source: newRecords - Load new records from JSON files (same as jsonFiles)"""
        # Original: newRecords
        path = f"abfss://prodhspsynapsefs@<storage_account>.dfs.core.windows.net/{self.folder_name}*.json"
        
        self.df_new_records = (spark.read
                              .format("json")
                              .option("multiline", "false")
                              .load(path)
                              .select(
                                  col("name").cast("string"),
                                  col("sort_order").cast("integer"),
                                  col("value").cast("string")
                              ))
        return self.df_new_records
    
    def df_distinct_data_point_names(self):
        """Transformation: distinctDataPointNames - Aggregate unique values by name"""
        # Original: distinctDataPointNames
        # jsonFiles aggregate(groupBy(name), values = collectUnique(value))
        
        if self.df_json_files is None:
            self.df_json_files()
            
        self.df_distinct_data_point_names = (self.df_json_files
                                           .groupBy("name")
                                           .agg(collect_list("value").alias("values")))
        return self.df_distinct_data_point_names
    
    def df_add_lookup_column(self):
        """Transformation: addLookupColumn - Add lookup column using cached names"""
        # Original: addLookupColumn
        # oldRecords derive(lookup = cachedNames#lookup(name).name)
        
        if self.df_old_records is None:
            self.df_old_records()
        if self.df_distinct_data_point_names is None:
            self.df_distinct_data_point_names()
            
        # NOTE: Approximating cache lookup with left join on distinct names
        self.df_add_lookup_column = (self.df_old_records
                                   .join(self.df_distinct_data_point_names.select("name").alias("lookup_name"),
                                         self.df_old_records.name == col("lookup_name.name"),
                                         "left")
                                   .withColumn("lookup", col("lookup_name.name"))
                                   .drop("lookup_name"))
        return self.df_add_lookup_column
    
    def df_find_deletes(self):
        """Transformation: findDeletes - Filter records that need to be deleted"""
        # Original: findDeletes
        # addLookupColumn filter(not(isNull(lookup)))
        
        if self.df_add_lookup_column is None:
            self.df_add_lookup_column()
            
        self.df_find_deletes = self.df_add_lookup_column.filter(col("lookup").isNotNull())
        return self.df_find_deletes
    
    def df_set_delete(self):
        """Transformation: setDelete - Mark records for deletion"""
        # Original: setDelete
        # findDeletes alterRow(deleteIf(true()))
        
        if self.df_find_deletes is None:
            self.df_find_deletes()
            
        # NOTE: AlterRow with deleteIf(true()) - all records in this stream are marked for deletion
        self.df_set_delete = self.df_find_deletes.withColumn("_delete_flag", lit(True))
        return self.df_set_delete
    
    def df_sort_new_records(self):
        """Transformation: sortNewRecords - Sort new records by name and sort_order"""
        # Original: sortNewRecords
        # newRecords sort(asc(name, true), asc(sort_order, true))
        
        if self.df_new_records is None:
            self.df_new_records()
            
        self.df_sort_new_records = (self.df_new_records
                                  .orderBy(asc("name"), asc("sort_order")))
        return self.df_sort_new_records
    
    def df_add_created_date(self):
        """Transformation: addCreatedDate - Add creation timestamp"""
        # Original: addCreatedDate
        # sortNewRecords derive(dt_created = $insertDate)
        
        if self.df_sort_new_records is None:
            self.df_sort_new_records()
            
        self.df_add_created_date = (self.df_sort_new_records
                                  .withColumn("dt_created", self.insert_date))
        return self.df_add_created_date
    
    def df_rearrange_columns(self):
        """Transformation: rearrangeColumns - Select and arrange final columns"""
        # Original: rearrangeColumns
        # addCreatedDate select(mapColumn(name, value, sort_order, dt_created))
        
        if self.df_add_created_date is None:
            self.df_add_created_date()
            
        self.df_rearrange_columns = (self.df_add_created_date
                                   .select("name", "value", "sort_order", "dt_created"))
        return self.df_rearrange_columns
    
    def df_set_inserts(self):
        """Transformation: setInserts - Mark records for insertion"""
        # Original: setInserts
        # rearrangeColumns alterRow(insertIf(true()))
        
        if self.df_rearrange_columns is None:
            self.df_rearrange_columns()
            
        # NOTE: AlterRow with insertIf(true()) - all records in this stream are marked for insertion
        self.df_set_inserts = self.df_rearrange_columns.withColumn("_insert_flag", lit(True))
        return self.df_set_inserts
    
    def df_cached_names(self):
        """Sink: cachedNames - Cache distinct names for lookup"""
        # Original: cachedNames
        # distinctDataPointNames sink(..., store: 'cache', format: 'inline')
        
        if self.df_distinct_data_point_names is None:
            self.df_distinct_data_point_names()
            
        # NOTE: Fabric doesn't have direct cache sink equivalent, using temp view
        self.df_distinct_data_point_names.createOrReplaceTempView("cached_names")
        # NOTE: Cache storage approximated with temp view for lookup operations
    
    def df_delete_records(self):
        """Sink: deleteRecords - Delete records from target table"""
        # Original: deleteRecords
        # setDelete sink(..., deletable: true, keys:['name'])
        
        if self.df_set_delete is None:
            self.df_set_delete()
            
        table_name = f"{self.schema}.{self.table}"
        
        # NOTE: Using Delta merge for delete operations
        if spark.catalog.tableExists(table_name):
            delta_table = DeltaTable.forName(spark, table_name)
            
            # Perform delete operation using merge
            (delta_table.alias("target")
             .merge(self.df_set_delete.alias("source"), "target.name = source.name")
             .whenMatchedDelete()
             .execute())
        # WARNING: If table doesn't exist, delete operation will be skipped
    
    def df_insert_records(self):
        """Sink: insertRecords - Insert new records to target table"""
        # Original: insertRecords
        # setInserts sink(..., insertable: true)
        
        if self.df_set_inserts is None:
            self.df_set_inserts()
            
        table_name = f"{self.schema}.{self.table}"
        
        # Remove internal flags before insertion
        df_clean = self.df_set_inserts.drop("_insert_flag")
        
        # Insert records to Delta table
        (df_clean.write
         .mode("append")
         .format("delta")
         .saveAsTable(table_name))
    
    def run(self):
        """Execute the complete data flow pipeline"""
        try:
            # Phase 1: Load sources
            self.df_json_files()
            self.df_old_records()
            self.df_new_records()
            
            # Phase 2: Process transformations
            self.df_distinct_data_point_names()
            self.df_add_lookup_column()
            self.df_find_deletes()
            self.df_set_delete()
            self.df_sort_new_records()
            self.df_add_created_date()
            self.df_rearrange_columns()
            self.df_set_inserts()
            
            # Phase 3: Execute sinks in order
            self.df_cached_names()      # saveOrder: 1
            self.df_delete_records()    # saveOrder: 2
            self.df_insert_records()    # saveOrder: 3
            
            print(f"Data flow completed successfully for table: {self.schema}.{self.table}")
            
        except Exception as e:
            print(f"Error in data flow execution: {str(e)}")
            raise e

# Execute the data flow
etljob = DfL2TextValues()
try:
    etljob.run()
except Exception as e:
    raise e