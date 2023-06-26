# Databricks notebook source
def merge_delta_data(src_df, tgt_catalog, tgt_schema, tgt_tbl, merge_condition):    
    
    spark.sql(f"""MERGE INTO {tgt_catalog}.{tgt_schema}.{tgt_tbl} tgt
                 USING {src_df} src
                 ON {merge_condition}
                 WHEN MATCHED THEN
                   UPDATE SET *
                 WHEN NOT MATCHED THEN
                   INSERT * """)

