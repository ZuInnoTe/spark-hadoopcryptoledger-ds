# spark-hadoopcryptoledger-ds
[![Build Status](https://travis-ci.org/ZuInnoTe/spark-hadoopcryptoledger-ds.svg?branch=master)](https://travis-ci.org/ZuInnoTe/spark-hadoopcryptoledger-ds)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/dc05d48352034c5a8608ff71b629ce9f)](https://www.codacy.com/app/jornfranke/spark-hadoopcryptoledger-ds?utm_source=github.com&utm_medium=referral&utm_content=ZuInnoTe/spark-hadoopcryptoledger-ds&utm_campaign=badger)

A [Spark datasource](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) for the [HadoopCryptoLedger](https://github.com/ZuInnoTe/hadoopcryptoledger/wiki) library. This Spark datasource assumes at least Spark > 1.5 or Spark >= 2.0.
Currently this datasource supports the following formats of the HadoopCryptoLedger library:
* Bitcoin Blockchain
  * Bitcoin Block Datasource format: org.zuinnote.spark.bitcoin.block
  * Bitcoin Transaction Datasource format: org.zuinnote.spark.bitcoin.transaction
  * Bitcoin TransactionElement Datasource format: org.zuinnote.spark.bitcoin.transactionelement

This datasource is available on [Spark-packages.org](https://spark-packages.org/package/ZuInnoTe/spark-hadoopcryptoledger-ds) and on [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Chadoopcryptoledger).

Find here the status from the Continuous Integration service: https://travis-ci.org/ZuInnoTe/spark-hadoopcryptoledger-ds/

# Release Notes
Find the latest release information [here](https://github.com/ZuInnoTe/spark-hadoopcryptoledger-ds/releases)

# Options
The following options are mapped to the following options of the HadoopCryptoLedger library ([Explanation](https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Hadoop-File-Format#configure)):
* "magic" is mapped to "hadoopcryptoledger.bitcoinblockinputformat.filter.magic"
* "maxblockSize" is mapped to "hadoopcryptoledger.bitcoinblockinputformat.maxblocksize"
* "useDirectBuffer" is mapped to "hadoopcryptoledeger.bitcoinblockinputformat.usedirectbuffer"
* "isSplitable" is mapped to "hadoopcryptoledeger.bitcoinblockinputformat.issplitable"


# Dependency
## Scala 2.10

groupId: com.github.zuinnote

artifactId: spark-hadoopcryptoledger-ds_2.10

version: 1.0.7

## Scala 2.11
 
groupId: com.github.zuinnote

artifactId: spark-hadoopcryptoledger-ds_2.11

version: 1.0.7


# Develop
The following sections describe some example code. 
## Scala
 This example loads Bitcoin Blockchain data from the folder "/home/user/bitcoin/input" using the BitcoinBlock representation (format).
 ```
val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("org.zuinnote.spark.bitcoin.block")
    .option("magic", "F9BEB4D9")
    .load("/home/user/bitcoin/input")
```
 The HadoopCryptoLedger library provides an example for scala using the data source library: https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Use-HadoopCrytoLedger-library-as-Spark-DataSource
## Java
 This example loads Bitcoin Blockchain data from the folder "/home/user/bitcoin/input" using the BitcoinBlock representation (format).
 ```
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);
DataFrame df = sqlContext.read()
    .format("org.zuinnote.spark.bitcoin.block")
    .option("magic", "F9BEB4D9")
    .load("/home/user/bitcoin/input");
```
## R
 This example loads Bitcoin Blockchain data from the folder "/home/user/bitcoin/input" using the BitcoinBlock representation (format).
```
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.github.zuinnote:spark-hadoopcrytoledger-ds_2.10:1.0.7" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)

df <- read.df(sqlContext, "/home/user/bitcoin/input", source = "org.zuinnote.spark.bitcoin.block", magic = "F9BEB4D9")
 ```
## Python
This example loads Bitcoin Blockchain data from the folder "/home/user/bitcoin/input" using the BitcoinBlock representation (format).
```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('org.zuinnote.spark.bitcoin.block').options(magic='F9BEB4D9').load('/home/user/bitcoin/input')
```
## SQL
The following statement creates a table that contains Bitcoin Blockchain data in the folder /home/user/bitcoin/input
```
CREATE TABLE BitcoinBlockchain
USING  org.zuinnote.spark.bitcoin.block
OPTIONS (path "/home/user/bitcoin/input", magic "F9BEB4D9")
```

# Schemas
## Format: org.zuinnote.spark.bitcoin.block
```
root
 |-- blockSize: integer (nullable = false)
 |-- magicNo: binary (nullable = false)
 |-- version: integer (nullable = false)
 |-- time: integer (nullable = false)
 |-- bits: binary (nullable = false)
 |-- nonce: integer (nullable = false)
 |-- transactionCounter: long (nullable = false)
 |-- hashPrevBlock: binary (nullable = false)
 |-- hashMerkleRoot: binary (nullable = false)
 |-- transactions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- version: integer (nullable = false)
 |    |    |-- marker: byte (nullable = false)
 |    |    |-- flag: byte (nullable = false)
 |    |    |-- inCounter: binary (nullable = false)
 |    |    |-- outCounter: binary (nullable = false)
 |    |    |-- listOfInputs: array (nullable = false)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- prevTransactionHash: binary (nullable = false)
 |    |    |    |    |-- previousTxOutIndex: long (nullable = false)
 |    |    |    |    |-- txInScriptLength: binary (nullable = false)
 |    |    |    |    |-- txInScript: binary (nullable = false)
 |    |    |    |    |-- seqNo: long (nullable = false)
 |    |    |-- listOfOutputs: array (nullable = false)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- value: long (nullable = false)
 |    |    |    |    |-- txOutScriptLength: binary (nullable = false)
 |    |    |    |    |-- txOutScript: binary (nullable = false)
 |    |    |-- listOfScriptWitnessItem: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = false)
 |    |    |    |    |-- stackItemCounter: binary (nullable = false)
 |    |    |    |    |-- scriptWitnessList: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = false)
 |    |    |    |    |    |    |-- witnessScriptLength: binary (nullable = false)
 |    |    |    |    |    |    |-- witnessScript: binary (nullable = false)
 |    |    |-- lockTime: integer (nullable = false)
```
## Format: org.zuinnote.spark.bitcoin.transaction
```
root
 |-- currentTransactionHash: binary (nullable = false)
 |-- version: integer (nullable = false)
 |-- marker: byte (nullable = false)
 |-- flag: byte (nullable = false)
 |-- inCounter: binary (nullable = false)
 |-- outCounter: binary (nullable = false)
 |-- listOfInputs: array (nullable = false)
 |    |-- element: struct (containsNull = true)
 |    |    |-- prevTransactionHash: binary (nullable = false)
 |    |    |-- previousTxOutIndex: long (nullable = false)
 |    |    |-- txInScriptLength: binary (nullable = false)
 |    |    |-- txInScript: binary (nullable = false)
 |    |    |-- seqNo: long (nullable = false)
 |-- listOfOutputs: array (nullable = false)
 |    |-- element: struct (containsNull = true)
 |    |    |-- value: long (nullable = false)
 |    |    |-- txOutScriptLength: binary (nullable = false)
 |    |    |-- txOutScript: binary (nullable = false)
 |-- listOfScriptWitnessItem: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- stackItemCounter: binary (nullable = false)
 |    |    |-- scriptWitnessList: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = false)
 |    |    |    |    |-- witnessScriptLength: binary (nullable = false)
 |    |    |    |    |-- witnessScript: binary (nullable = false)
 |-- lockTime: integer (nullable = false)
                                                                                                                                                       
```
