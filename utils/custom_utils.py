from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col,
    trim,
    row_number,
    current_timestamp
)
from delta.tables import DeltaTable


class Transformations:

    def dedup(self, df: DataFrame, dedup_cols: List[str], cdc: str) -> DataFrame:
        """
        Deduplicate dataframe keeping latest record based on CDC column.
        Args:
            df(DataFrame): Spark DataFrame that has duplicate records
            dedup_cols(List): List of columns to deduplicate on
            cdc(string): CDC column
        Returns:
            df(DataFrame): Deduplicated dataframe 
        """

        window_spec = Window.partitionBy(*dedup_cols).orderBy(col(cdc).desc())
        df = (
            df.withColumn("dedup_key", row_number().over(window_spec))
            .filter(col("dedup_key") == 1)
            .drop("dedup_key")
        )
        return df
            

    def trim_columns(self, df: DataFrame) -> DataFrame:
        """
        This function trims the string columns of the dataframe
        Args:
            df(DataFrame): Untrimmed spark dataframe
        Returns:
            df(DataFrame): Dataframe with trimmed columns
        """

        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, trim(col(field.name)))
        return df
    

    def remove_null(self, df: DataFrame, primary_col: str) -> DataFrame:
        """
        This function removes the null values from the dataframe based on the primary_key
        Args:
            df(DataFrame): Spark dataframe with null values in the primary key
            primary_col(string): Primary key column
        Returns:
            df(DataFrame): Spark dataframe with no nulls in the primary key
        """

        df = df.filter(col(primary_col).isNotNull())
        return df
    
    
    def add_ingestion_timestamp(self, df: DataFrame) -> DataFrame:
        
        """
        This function adds a current_timestamp column to the dataframe
        Args:
            df(DataFrame): Spark dataframe with no current_time column
        Returns:
            df(DataFrame): Spark dataframe with current_timestamp column
        """

        df = df.withColumn("ingestion_ts", current_timestamp())
        return df


    def upsert(self, spark, df: DataFrame, key_cols: List[str], table: str, cdc: str):
        """
        This function performs upsert(SCD type 1) operation on the table based on the key columns
        Args:
            spark(SparkSession): Spark session
            df(DataFrame): Spark dataframe with data to be upserted(Source)
            key_cols(List): List of key columns to perform upsert on
            table(string): Destination table name
            cdc(string): CDC column
        Returns:
            None
        """

        if not key_cols:
            raise ValueError("Key columns cannot be empty")

        target_table = f"lakehouse.silver.{table}"

        delta_table = DeltaTable.forName(spark, target_table)

        merge_condition = (
            " AND ".join([f"src.{col} = dest.{col}" for col in key_cols]) # "separator".join(iterable)
        )

        update_condition = f"""
            src.{cdc} IS NOT NULL AND
            (
                dest.{cdc} IS NULL OR
                src.{cdc} > dest.{cdc}
                )
        """
        
        # Applying merge condition
        (
            delta_table.alias("dest")
            .merge(
                df.alias("src"),
                merge_condition
            )
            .whenMatchedUpdateAll(condition = update_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )