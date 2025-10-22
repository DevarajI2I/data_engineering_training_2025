"""
Module: TableMismatchChecker

Description:
    This module defines the `TableMismatchChecker` class, which helps find out
    the count mistmatch between the given tables

Author:
    

Date:
    2025-02-18
"""
import os
import logging
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

class TableMismatchChecker:
    """
    TableMismatchChecker helps out to find row count of the tables.
    """
    def create_gold_tables_db_engine(self):
        """
        Prepares the connection engine of the DB wich has admin table data
        
        Returns:
        -------
        admin_gold_Postgres_engine
            Postgress connection engine object

        admin_gold_trino_engine
            Trino connection engine object
        """
        admin_gold_postgres_user = os.getenv("gold_postgres_user")
        admin_gold_postgres_host = os.getenv("gold_postgres_host")
        admin_gold_postgres_port = os.getenv("gold_postgres_port")
        admin_gold_postgres_db_name = os.getenv("gold_postgres_db_name")
        admin_gold_postgres_password = quote_plus(os.getenv("gold_postgres_password"))
        admin_gold_postgress_url = f"postgresql://{admin_gold_postgres_user}:{admin_gold_postgres_password}@{admin_gold_postgres_host}:{admin_gold_postgres_port}/{admin_gold_postgres_db_name}"
        
        admin_gold_trino_user = os.getenv("gold_trino_user")
        admin_gold_trino_password = os.getenv("gold_trino_password")
        admin_gold_trino_host = os.getenv("gold_trino_host")
        admin_gold_trino_port = os.getenv("gold_trino_port")
        admin_gold_trino_db_name = os.getenv("gold_trino_db_name")
        admin_gold_trino_schema = os.getenv("gold_trino_schema")
        admin_gold_trino_http_schema = os.getenv("gold_trino_http_schema")
        admin_gold_trino_url = f"trino://{admin_gold_trino_user}:{admin_gold_trino_password}@{admin_gold_trino_host}:{admin_gold_trino_port}/{admin_gold_trino_db_name}?schema={admin_gold_trino_schema}&http_scheme={admin_gold_trino_http_schema}"
        
        admin_gold_Postgres_engine = create_engine(admin_gold_postgress_url)
        admin_gold_trino_engine = create_engine(admin_gold_trino_url)
        return admin_gold_Postgres_engine,admin_gold_trino_engine


    def create_bronze_tables_db_engine(self):
        """
        Prepares the connection engine of the DB wich has bronze table data
        
        Returns:
        -------
        admin_gold_Postgres_engine
            Postgress connection engine object

        admin_gold_trino_engine
            Trino connection engine object
        """
        bronze_postgres_user = os.getenv("bronze_postgres_user")
        bronze_postgres_host = os.getenv("bronze_postgres_host")
        bronze_postgres_port = os.getenv("bronze_postgres_port")
        bronze_postgres_db_name = os.getenv("bronze_postgres_db_name")
        bronze_postgres_password = quote_plus(os.getenv("bronze_postgres_password"))
        bronze_postgress_url = f"postgresql://{bronze_postgres_user}:{bronze_postgres_password}@{bronze_postgres_host}:{bronze_postgres_port}/{bronze_postgres_db_name}"
        
        bronze_trino_user = os.getenv("gold_trino_user")
        bronze_trino_password = os.getenv("gold_trino_password")
        bronze_trino_host = os.getenv("gold_trino_host")
        bronze_trino_port = os.getenv("gold_trino_port")
        bronze_trino_db_name = os.getenv("gold_trino_db_name")
        bronze_trino_http_schema = os.getenv("gold_trino_http_schema")
        bronze_trino_schema = os.getenv("bronze_trino_schema")
        bronze_trino_url = f"trino://{bronze_trino_user}:{bronze_trino_password}@{bronze_trino_host}:{bronze_trino_port}/{bronze_trino_db_name}?schema={bronze_trino_schema}&http_scheme={bronze_trino_http_schema}"

        bronze_trino_engine = create_engine(bronze_trino_url)
        bronze_postgress_engine = create_engine(bronze_postgress_url)
        return bronze_postgress_engine,bronze_trino_engine

    def get_table_count(self, engine, schema, table):
        """
        Get the count of the given tables
        
        Returns:
        -------
            Row count of the table
        """
        query = f"SELECT COUNT(*) AS row_count FROM {schema}.{table}"
        try:
            df = pd.read_sql(query, engine)
            return df.iloc[0, 0]
        except Exception as e:
            logging.error(f"Error fetching count for {schema}.{table}: {e}")
        
    def get_fhir_table_count(self, engine, table, column):
        """
        Get the count of the given tables
        
        Returns:
        -------
            Row count of the table
        """
        query = f"select count(*), {column} from {table} group by {column}"
        try:
            df = pd.read_sql(query, engine)
            return df.iloc[0, 0]
        except Exception as e:
            logging.error(f"Error fetching count for {table}.{column}: {e}")
        
    def check_gold_tables_count(self):
        """
        Prepares the admin table mismatch counts
        
        Returns:
        -------
        gold_mismatch_results
            Return the list of table names with their respective counts
        """
        table_mapping = {}
        gold_mismatch_results = []
        gold_Postgres_engine,gold_redshift_engine  = TableMismatchChecker.create_gold_tables_db_engine(self)

        for key, value in os.environ.items():
            if key.startswith("ADMIN_GOLD_"):  # Filter only relevant mappings
                source, destination = value.split(":")
                source_schema, source_table = source.split(".")
                destination_schema, destinaon_table = destination.split(".")
                table_mapping[(source_schema, source_table)] = (destination_schema, destinaon_table)

        # Compare counts and store mismatches
        for (source_schema, source_table), (destination_schema, destinaon_table) in table_mapping.items():
            source_count = TableMismatchChecker.get_table_count(self, gold_Postgres_engine, source_schema, source_table)
            detination_count =TableMismatchChecker.get_table_count(self, gold_redshift_engine, destination_schema, destinaon_table)

            if source_count != detination_count:
                gold_mismatch_results.append((source_table, source_count, detination_count))
        return gold_mismatch_results

    def check_bronze_tables_count(self):
        """
        Prepares the bronze table mismatch counts
        
        Returns:
        -------
        bronze_mismatch_tables
            Return the list of table names with their respective counts
        """
        table_mapping = []
        bronze_mismatch_results = []
        source_table = os.getenv("FHIR_HFJ_RES_VER_TABLE")
        source_column = os.getenv("FHIR_HFJ_RES_VER_COLUMN")
        bronze_postgres_engine,bronze_trino_engine  = TableMismatchChecker.create_bronze_tables_db_engine(self)
        source_count = TableMismatchChecker.get_fhir_table_count(self, bronze_postgres_engine, source_table, source_column)

        for key, value in os.environ.items():
            if key.startswith("BRONZE_"):  # Filter only relevant mappings
                destination_schema, destination_table = value.split(".")
                table_mapping.append((destination_schema, destination_table))

        # Compare counts and store mismatches
        for destination_schema, destination_table in table_mapping:
            destination_count = TableMismatchChecker.get_table_count(self, bronze_trino_engine, destination_schema, destination_table)

            if source_count != destination_count:
                bronze_mismatch_results.append((destination_table, source_count, destination_count))
        return bronze_mismatch_results

        
    def check_tables_count(self):
        """
        Prepares mismatch counts of the given tables
        
        Returns:
        -------
        gold_mismatch_results and bronze_mismatch_tables
            Return the list of table names with their respective counts
        """
        gold_mismatch_tables = TableMismatchChecker.check_gold_tables_count(self)
        bronze_mismatch_tables = TableMismatchChecker.check_bronze_tables_count(self)
        return gold_mismatch_tables,bronze_mismatch_tables