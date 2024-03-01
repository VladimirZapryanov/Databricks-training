# Databricks notebook source
order_items = spark.read.json('/FileStore/public/retail_db_json/order_items')
