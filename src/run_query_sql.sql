-- Databricks notebook source
-- Placeholder SQL notebook - this is not currently functional
-- Runs any query passed as a parameter
declare or replace variable qry string;
set var qry = :qry;

execute immediate qry;
