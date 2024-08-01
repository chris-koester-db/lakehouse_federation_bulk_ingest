-- Databricks notebook source
-- Runs any query passed as a parameter
declare or replace variable qry string;
set var qry = :qry;

execute immediate qry;
