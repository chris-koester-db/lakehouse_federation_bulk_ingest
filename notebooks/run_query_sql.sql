-- Databricks notebook source
-- Placeholder notebook for DBSQL compute. Has not been fully tested.
declare or replace qry_str string;
set var qry_str =
    'insert into `' || :tgt_catalog || '`. `' || :tgt_schema || '`. `' || :tgt_table || '`'
    'select * from `' || :src_catalog || '`. `' || :src_schema || '`. `' || :src_table || '`'
    'where ' || :where_clause;

execute immediate qry_str;
