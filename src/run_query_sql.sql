-- Databricks notebook source
declare or replace variable qry string;
set var qry = :qry;

execute immediate qry;
