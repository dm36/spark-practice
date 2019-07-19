#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///home/dmadhok/spark_course/customer-orders.csv")

def parseLine(line):
    fields = line.split(',')
    customerID = fields[0]
    dollarAmount = float(fields[2])
    return (customerID, dollarAmount)

customerDollars = input.map(parseLine)
customerDollars = customerDollars.reduceByKey(lambda x, y: x + y)

# customerDollars.collect() returns a list from the rdd- running sorted sorts it
sorted(customerDollars.collect())

