# spark-hadoopcryptoledger-ds
[![Build Status](https://travis-ci.org/ZuInnoTe/spark-hadoopcryptoledger-ds.svg?branch=master)](https://travis-ci.org/ZuInnoTe/spark-hadoopcryptoledger-ds)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/dc05d48352034c5a8608ff71b629ce9f)](https://www.codacy.com/app/jornfranke/spark-hadoopcryptoledger-ds?utm_source=github.com&utm_medium=referral&utm_content=ZuInnoTe/spark-hadoopcryptoledger-ds&utm_campaign=badger)

A [Spark datasource](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) for the [HadoopCryptoLedger](https://github.com/ZuInnoTe/hadoopcryptoledger/wiki) library.

This Spark datasource assumes 2.2 > Spark >= 2.0 (see note below). If you're using Spark > 1.5, please use version 1.1.2, which is the last version to support Spark < 2.0.

Currently this datasource supports the following formats of the HadoopCryptoLedger library (see schemas at the end of the page):
* Bitcoin and Altcoin Blockchain
  * Bitcoin Block Datasource format: org.zuinnote.spark.bitcoin.block
  * Bitcoin Transaction Datasource format: org.zuinnote.spark.bitcoin.transaction
* Ethereum and Altcoin Blockchain
  * Ethereum Block Datasource format: org.zuinnote.spark.ethereum.block
  
This datasource is available on [Spark-packages.org](https://spark-packages.org/package/ZuInnoTe/spark-hadoopcryptoledger-ds) and on [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Chadoopcryptoledger).

Find here the status from the Continuous Integration service: https://travis-ci.org/ZuInnoTe/spark-hadoopcryptoledger-ds/

# Release Notes
Find the latest release information [here](https://github.com/ZuInnoTe/spark-hadoopcryptoledger-ds/releases)

# Options
The following options are mapped to the following options of the HadoopCryptoLedger library ([Explanation](https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Hadoop-File-Format#configure)):
* Bitcoin and Altcoins
  * "magic" is mapped to "hadoopcryptoledger.bitcoinblockinputformat.filter.magic"
  * "maxblockSize" is mapped to "hadoopcryptoledger.bitcoinblockinputformat.maxblocksize"
  * "useDirectBuffer" is mapped to "hadoopcryptoledeger.bitcoinblockinputformat.usedirectbuffer"
  * "isSplittable" is mapped to "hadoopcryptoledeger.bitcoinblockinputformat.issplitable"
  * "readAuxPOW" is mapped to "hadoopcryptoledeger.bitcoinblockinputformat.readauxpow"
  * "enrich" adds the transaction hash to each transaction in the Bitcoin block
* Ethereum and Altcoins
  * "maxblockSize" is mapped to "hadoopcryptoledger.ethereumlockinputformat.maxblocksize"
  * "useDirectBuffer" is mapped to "hadoopcryptoledeger.ethereumblockinputformat.usedirectbuffer"
  * "enrich" in case of true it additional data is calculated for transactions: sendAddress and hash. Default: false. Note: you must include the bouncycastle dependency to use this.

# Setup
```
libraryDependencies += "com.github.zuinnote" %% "spark-hadoopcryptoledger-ds" % "1.2.0"
```

The library is published for Scala 2.10 and 2.11. It will be published for 2.12 once Spark supports it.

## Information Spark 2.2 and outdated Bouncy Castle library
As [omervk and liorregev point out](https://github.com/ZuInnoTe/spark-hadoopcryptoledger-ds/issues/9), Spark 2.2 uses jets3t 0.9.3, which depends on an outdated version of Bouncy Castle.
Unfortunately, this outdated version does not support the cryptographic operations needed for enrichment of Ethereum data (SendAddress and TransactionHash).

You have the following alternatives:
1. Shade the latest BC library in your application (hadoopcryptoledger has it only as a provided dependency)
2. Remove the outdated bouncy castle libraries from the Spark libraries (and do not use jets3t); or
3. Wait for an updated jets3t version, which then need to be included into an updated Spark version

Other Spark versions do not currently show any issues.

# Develop
The following sections describe some example code. 
## Scala
### Bitcoin and Altcoins
 This example loads Bitcoin Blockchain data from the folder "/user/bitcoin/input" using the BitcoinBlock representation (format).
 ```
val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("org.zuinnote.spark.bitcoin.block")
    .option("magic", "F9BEB4D9")
    .load("/user/bitcoin/input")

// Or if you'd like a Dataset version (Spark 2.0+)...

import org.zuinnote.spark.bitcoin.model._
import df.sparkSession.implicits._

// Also available: EnrichedBitcoinBlock, BitcoinBlockWithAuxPOW, EnrichedBitcoinBlockWithAuxPOW
val ds: Dataset[BitcoinBlock] = df.as[BitcoinBlock]
```
 The HadoopCryptoLedger library provides an example for scala using the data source library: https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Use-HadoopCrytoLedger-library-as-Spark-DataSource
### Ethereum and Altcoins
 This example loads Ethereum Blockchain data from the folder "/user/ethereum/input" using the EthereumBlock representation (format).
 ```
val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("org.zuinnote.spark.ethereum.block")
    .option("enrich", "false")
    .load("/user/ethereum/input")

// Or if you'd like a Dataset version (Spark 2.0+)...

import org.zuinnote.spark.ethereum.model._
import df.sparkSession.implicits._

// Also available: EnrichedEthereumBlock
val ds: Dataset[EthereumBlock] = df.as[EthereumBlock]
```
 The HadoopCryptoLedger library provides an example for scala using the data source library: ledger/wiki/Use-HadoopCrytoLedger-library-as-Spark-datasource-to-read-Ethereum-data
## Java
### Bitcoin and Altcoins
 This example loads Bitcoin Blockchain data from the folder "/user/bitcoin/input" using the BitcoinBlock representation (format).
 ```
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);
DataFrame df = sqlContext.read()
    .format("org.zuinnote.spark.bitcoin.block")
    .option("magic", "F9BEB4D9")
    .load("/user/bitcoin/input");
```
### Ethereum and Altcoins
 This example loads Ethereum Blockchain data from the folder "/user/ethereum/input" using the EthereumBlock representation (format).
 ```
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);
DataFrame df = sqlContext.read()
    .format("org.zuinnote.spark.ethereum.block")
    .option("enrich", "false")
    .load("/user/ethereum/input");
```
## R
### Bitcoin and Altcoins
 This example loads Bitcoin Blockchain data from the folder "/user/bitcoin/input" using the BitcoinBlock representation (format).
```
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.github.zuinnote:spark-hadoopcrytoledger-ds_2.11:1.2.0" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)

df <- read.df(sqlContext, "/user/bitcoin/input", source = "org.zuinnote.spark.bitcoin.block", magic = "F9BEB4D9")
 ```
### Ethereum and Altcoins
 This example loads Ethereum Blockchain data from the folder "/user/ethereum/input" using the EthereumBlock representation (format).
```
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.github.zuinnote:spark-hadoopcrytoledger-ds_2.11:1.2.0" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)

df <- read.df(sqlContext, "/user/ethereum/input", source = "org.zuinnote.spark.ethereum.block", enrich = "false")
 ```
## Python
### Bitcoin and Altcoins
This example loads Bitcoin Blockchain data from the folder "/user/bitcoin/input" using the BitcoinBlock representation (format).
```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('org.zuinnote.spark.bitcoin.block').options(magic='F9BEB4D9').load('/user/bitcoin/input')
```
### Ethereum and Altcoins
This example loads Ethereum Blockchain data from the folder "/user/ethereum/input" using the EthereumBlock representation (format).
```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('org.zuinnote.spark.ethereum.block').options(enrich='false').load('/user/ethereum/input')
```
## SQL
### Bitcoin and Altcoins
The following statement creates a table that contains Bitcoin Blockchain data in the folder /user/bitcoin/input
```
CREATE TABLE BitcoinBlockchain
USING  org.zuinnote.spark.bitcoin.block
OPTIONS (path "/user/bitcoin/input", magic "F9BEB4D9")
```
### Ethereum and Altcoins
The following statement creates a table that contains Ethereum Blockchain data in the folder /user/ethereum/input
```
CREATE TABLE EthereumBlockchain
USING  org.zuinnote.spark.ethereum.block
OPTIONS (path "/user/ethereum/input", enrich "false")
```
# Schemas
## Format: org.zuinnote.spark.bitcoin.block (readAuxPOW=false,enrich=true)
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
 |    |    |-- currentTransactionHash: binary (nullable = false)
```
## Format: org.zuinnote.spark.bitcoin.block (readAuxPOW=false,enrich=false)
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
## Format: org.zuinnote.spark.bitcoin.block (readAuxPOW=true,enrich=true)
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
 |    |    |-- currentTransactionHash: binary (nullable = false)
 |-- auxPOW: struct (nullable = true)
 |    |-- version: integer (nullable = false)
 |    |-- coinbaseTransaction: struct (nullable = false)
 |    |    |-- version: integer (nullable = false)
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
 |    |    |-- lockTime: integer (nullable = false)
 |    |-- parentBlockHeaderHash: binary (nullable = false)
 |    |-- coinbaseBranch: struct (nullable = false)
 |    |    |-- numberOfLinks: binary (nullable = false)
 |    |    |-- links: array (nullable = false)
 |    |    |    |-- element: binary (containsNull = true)
 |    |    |-- branchSideBitmask: binary (nullable = false)
 |    |-- auxBlockChainBranch: struct (nullable = false)
 |    |    |-- numberOfLinks: binary (nullable = false)
 |    |    |-- links: array (nullable = false)
 |    |    |    |-- element: binary (containsNull = true)
 |    |    |-- branchSideBitmask: binary (nullable = false)
 |    |-- parentBlockHeader: struct (nullable = false)
 |    |    |-- version: integer (nullable = false)
 |    |    |-- previousBlockHash: binary (nullable = false)
 |    |    |-- merkleRoot: binary (nullable = false)
 |    |    |-- time: integer (nullable = false)
 |    |    |-- bits: binary (nullable = false)
 |    |    |-- nonce: integer (nullable = false)
```
## Format: org.zuinnote.spark.bitcoin.block (readAuxPOW=true,enrich=false)
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
 |-- auxPOW: struct (nullable = true)
 |    |-- version: integer (nullable = false)
 |    |-- coinbaseTransaction: struct (nullable = false)
 |    |    |-- version: integer (nullable = false)
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
 |    |    |-- lockTime: integer (nullable = false)
 |    |-- parentBlockHeaderHash: binary (nullable = false)
 |    |-- coinbaseBranch: struct (nullable = false)
 |    |    |-- numberOfLinks: binary (nullable = false)
 |    |    |-- links: array (nullable = false)
 |    |    |    |-- element: binary (containsNull = true)
 |    |    |-- branchSideBitmask: binary (nullable = false)
 |    |-- auxBlockChainBranch: struct (nullable = false)
 |    |    |-- numberOfLinks: binary (nullable = false)
 |    |    |-- links: array (nullable = false)
 |    |    |    |-- element: binary (containsNull = true)
 |    |    |-- branchSideBitmask: binary (nullable = false)
 |    |-- parentBlockHeader: struct (nullable = false)
 |    |    |-- version: integer (nullable = false)
 |    |    |-- previousBlockHash: binary (nullable = false)
 |    |    |-- merkleRoot: binary (nullable = false)
 |    |    |-- time: integer (nullable = false)
 |    |    |-- bits: binary (nullable = false)
 |    |    |-- nonce: integer (nullable = false)
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
## Format: org.zuinnote.spark.ethereum.block (enrich=true)
```
root
 |-- ethereumBlockHeader: struct (nullable = false)
 |    |-- parentHash: binary (nullable = false)
 |    |-- uncleHash: binary (nullable = false)
 |    |-- coinBase: binary (nullable = false)
 |    |-- stateRoot: binary (nullable = false)
 |    |-- txTrieRoot: binary (nullable = false)
 |    |-- receiptTrieRoot: binary (nullable = false)
 |    |-- logsBloom: binary (nullable = false)
 |    |-- difficulty: binary (nullable = false)
 |    |-- timestamp: long (nullable = false)
 |    |-- number: long (nullable = false)
 |    |-- gasLimit: long (nullable = false)
 |    |-- gasUsed: long (nullable = false)
 |    |-- mixHash: binary (nullable = false)
 |    |-- extraData: binary (nullable = false)
 |    |-- nonce: binary (nullable = false)
 |-- ethereumTransactions: array (nullable = false)
 |    |-- element: struct (containsNull = true)
 |    |    |-- nonce: binary (nullable = false)
 |    |    |-- value: long (nullable = false)
 |    |    |-- receiveAddress: binary (nullable = false)
 |    |    |-- gasPrice: long (nullable = false)
 |    |    |-- gasLimit: long (nullable = false)
 |    |    |-- data: binary (nullable = false)
 |    |    |-- sig_v: binary (nullable = false)
 |    |    |-- sig_r: binary (nullable = false)
 |    |    |-- sig_s: binary (nullable = false)
 |    |    |-- sendAddress: binary (nullable = false)
 |    |    |-- hash: binary (nullable = false)
 |-- uncleHeaders: array (nullable = false)
 |    |-- element: struct (containsNull = true)
 |    |    |-- parentHash: binary (nullable = false)
 |    |    |-- uncleHash: binary (nullable = false)
 |    |    |-- coinBase: binary (nullable = false)
 |    |    |-- stateRoot: binary (nullable = false)
 |    |    |-- txTrieRoot: binary (nullable = false)
 |    |    |-- receiptTrieRoot: binary (nullable = false)
 |    |    |-- logsBloom: binary (nullable = false)
 |    |    |-- difficulty: binary (nullable = false)
 |    |    |-- timestamp: long (nullable = false)
 |    |    |-- number: long (nullable = false)
 |    |    |-- gasLimit: long (nullable = false)
 |    |    |-- gasUsed: long (nullable = false)
 |    |    |-- mixHash: binary (nullable = false)
 |    |    |-- extraData: binary (nullable = false)
 |    |    |-- nonce: binary (nullable = false)
                                                                                                                                                       
```

## Format: org.zuinnote.spark.ethereum.block (enrich=false)
```
root
 |-- ethereumBlockHeader: struct (nullable = false)
 |    |-- parentHash: binary (nullable = false)
 |    |-- uncleHash: binary (nullable = false)
 |    |-- coinBase: binary (nullable = false)
 |    |-- stateRoot: binary (nullable = false)
 |    |-- txTrieRoot: binary (nullable = false)
 |    |-- receiptTrieRoot: binary (nullable = false)
 |    |-- logsBloom: binary (nullable = false)
 |    |-- difficulty: binary (nullable = false)
 |    |-- timestamp: long (nullable = false)
 |    |-- number: long (nullable = false)
 |    |-- gasLimit: long (nullable = false)
 |    |-- gasUsed: long (nullable = false)
 |    |-- mixHash: binary (nullable = false)
 |    |-- extraData: binary (nullable = false)
 |    |-- nonce: binary (nullable = false)
 |-- ethereumTransactions: array (nullable = false)
 |    |-- element: struct (containsNull = true)
 |    |    |-- nonce: binary (nullable = false)
 |    |    |-- value: long (nullable = false)
 |    |    |-- receiveAddress: binary (nullable = false)
 |    |    |-- gasPrice: long (nullable = false)
 |    |    |-- gasLimit: long (nullable = false)
 |    |    |-- data: binary (nullable = false)
 |    |    |-- sig_v: binary (nullable = false)
 |    |    |-- sig_r: binary (nullable = false)
 |    |    |-- sig_s: binary (nullable = false)
 |-- uncleHeaders: array (nullable = false)
 |    |-- element: struct (containsNull = true)
 |    |    |-- parentHash: binary (nullable = false)
 |    |    |-- uncleHash: binary (nullable = false)
 |    |    |-- coinBase: binary (nullable = false)
 |    |    |-- stateRoot: binary (nullable = false)
 |    |    |-- txTrieRoot: binary (nullable = false)
 |    |    |-- receiptTrieRoot: binary (nullable = false)
 |    |    |-- logsBloom: binary (nullable = false)
 |    |    |-- difficulty: binary (nullable = false)
 |    |    |-- timestamp: long (nullable = false)
 |    |    |-- number: long (nullable = false)
 |    |    |-- gasLimit: long (nullable = false)
 |    |    |-- gasUsed: long (nullable = false)
 |    |    |-- mixHash: binary (nullable = false)
 |    |    |-- extraData: binary (nullable = false)
 |    |    |-- nonce: binary (nullable = false)
                                                                                                                                                       
```
