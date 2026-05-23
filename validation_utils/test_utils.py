from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import(
    col,
    row_number,
    trim
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType


class Validations:
    
    def null_check(self, df: DataFrame, primary_col: str) -> None:
        """
        This function checks if there are any null values in the primary key column.
        If any null values are found, it raises a ValueError.
        Args:
            df(DataFrame): Spark DataFrame which might contain nulls in the primary key column
            primary_col(str): Primary key column name
        Returns:
            None
        """
        null_count = df.filter(col(primary_col).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Null values found in primary key column '{primary_col}'.")
        else:
            print(f"No null values found in primary key column '{primary_col}'.")


    def duplicate_check(self, df: DataFrame, dedup_cols: List[str], cdc: str) -> None:
        """
        This function checks if there are any duplicate records in the DataFrame based on the specified columns.
        If any duplicates are found, it raises a Exception.
        Args:
            df(DataFrame): Spark DataFrame which might contain duplicates
            dedup_cols(List[str]): List of columns to check for duplicates
            cdc(str): Change data capture column name
        Returns:
            None
        """
        window_spec = Window.partitionBy(*dedup_cols).orderBy(col(cdc).desc())
        df_with_rownum = (
            df.withColumn("row_num",
            row_number().over(window_spec))
        )
        duplicate_count = df_with_rownum.filter(col("row_num") > 1).count()
        if duplicate_count > 0:
            raise Exception(f"Duplicate records found based on columns: { ', '.join(dedup_cols)}.")
        else:
            print(f"No duplicate records found based on columns: { ', '.join(dedup_cols)}.")


    def whitespace_check(self, df: DataFrame) -> None:
        """
        This function checks if there are any leading or trailing whitespaces in the DataFrame.
        If any whitespaces are found, it raises a Exception.
        Args:
            df(DataFrame): Spark DataFrame which might contain whitespaces
        Returns:
            None
        """

        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]
        for column_name in string_cols:
            mismatch_count = (
                df.filter(col(column_name) != trim(col(column_name)))
                .count()
            )
            if mismatch_count > 0:
                raise Exception(
                    f"Leading or trailing whitespaces found in column '{column_name}'."
                )
            else:
                print(
                    f"No leading or trailing whitespaces found in column '{column_name}'.")
                


                
        