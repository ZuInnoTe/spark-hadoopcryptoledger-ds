# spark-hadoopcryptoledger-ds
[![Build Status](https://travis-ci.org/ZuInnoTe/spark-hadoopcryptoledger-ds.svg?branch=master)](https://travis-ci.org/ZuInnoTe/spark-hadoopcryptoledger-ds)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/dc05d48352034c5a8608ff71b629ce9f)](https://www.codacy.com/app/jornfranke/spark-hadoopcryptoledger-ds?utm_source=github.com&utm_medium=referral&utm_content=ZuInnoTe/spark-hadoopcryptoledger-ds&utm_campaign=badger)

A [Spark datasource](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) for the [HadoopCryptoLedger](https://github.com/ZuInnoTe/hadoopcryptoledger/wiki) library.

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
  * "chainId" this is needed if you enrich to correctly calculate the SendAddress. Examples are 1 Frontier (Mainnet), 2 Morden, 3 Ropsten, but other chainIds exist for Altcoins based on Ethereum. Default 1.

# Setup
```
libraryDependencies += "com.github.zuinnote" %% "spark-hadoopcryptoledger-ds" % "1.3.0"
```

The library is currently published for 2.11 and 2.12. Scala 2.12 requires that you use at least Spark 2.4.0 

If you use Ethereum and the enrich functionality then you need the Bouncycastle dependency:
```
libraryDependencies += "org.bouncycastle" %% "sbcprov-ext-jdk15on" % "1.64"
```
## Information Spark 2.0 and 2.1 BigInteger too large
Spark 2.0 and Spark 2.1 have a bug (see https://issues.apache.org/jira/browse/SPARK-20341) that does not allow Big Integers with a precision large than 19. You may face those in the Ethereum blockchain. We recommend to use Spark 2.3 latest.

## Information Spark 2.2 and outdated Bouncy Castle library
As [omervk and liorregev point out](https://github.com/ZuInnoTe/spark-hadoopcryptoledger-ds/issues/9), Spark 2.2 uses jets3t 0.9.3, which depends on an outdated version of Bouncy Castle.
Unfortunately, this outdated version does not support the cryptographic operations needed for enrichment of Ethereum data (SendAddress and TransactionHash).

You have the following alternatives:
1. Shade the latest BC library in your application (hadoopcryptoledger has it only as a provided dependency)
2. Remove the outdated bouncy castle libraries from the Spark libraries (and do not use jets3t); or
3. Wait for an updated jets3t version, which then need to be included into an updated Spark version

Other Spark versions do not currently show any issues (Spark 1.6, 2.0, 2.1 or 2.3 are not affected).

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

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.github.zuinnote:spark-hadoopcrytoledger-ds_2.11:1.3.0" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)

df <- read.df(sqlContext, "/user/bitcoin/input", source = "org.zuinnote.spark.bitcoin.block", magic = "F9BEB4D9")
 ```
### Ethereum and Altcoins
 This example loads Ethereum Blockchain data from the folder "/user/ethereum/input" using the EthereumBlock representation (format).
```
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.github.zuinnote:spark-hadoopcrytoledger-ds_2.11:1.3.0" "sparkr-shell"')
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
 |-- blockSize: long (nullable = false)
 |-- magicNo: binary (nullable = false)
 |-- version: long (nullable = false)
 |-- time: long (nullable = false)
 |-- bits: binary (nullable = false)
 |-- nonce: long (nullable = false)
 |-- transactionCounter: long (nullable = false)
 |-- hashPrevBlock: binary (nullable = false)
 |-- hashMerkleRoot: binary (nullable = false)
 |-- transactions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- version: long (nullable = false)
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
 |    |    |    |    |-- value: decimal(38,0) (nullable = false)
 |    |    |    |    |-- txOutScriptLength: binary (nullable = false)
 |    |    |    |    |-- txOutScript: binary (nullable = false)
 |    |    |-- listOfScriptWitnessItem: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = false)
 |    |    |    |    |-- stackItemCounter: binary (nullable = false)
 |    |    |    |    |-- scriptWitnessList: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = false)
 |    |    |    |    |    |    |-- witnessScriptLength: binary (nullable = false)
 |    |    |    |    |    |    |-- witnessScript: binary (nullable = false)
 |    |    |-- lockTime: long (nullable = false)
 |    |    |-- currentTransactionHash: binary (nullable = false)
```
## Format: org.zuinnote.spark.bitcoin.block (readAuxPOW=false,enrich=false)
```
root
 |-- blockSize: long (nullable = false)
 |-- magicNo: binary (nullable = false)
 |-- version: long (nullable = false)
 |-- time: long (nullable = false)
 |-- bits: binary (nullable = false)
 |-- nonce: long (nullable = false)
 |-- transactionCounter: long (nullable = false)
 |-- hashPrevBlock: binary (nullable = false)
 |-- hashMerkleRoot: binary (nullable = false)
 |-- transactions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- version: long (nullable = false)
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
 |    |    |    |    |-- value: decimal(38,0) (nullable = false)
 |    |    |    |    |-- txOutScriptLength: binary (nullable = false)
 |    |    |    |    |-- txOutScript: binary (nullable = false)
 |    |    |-- listOfScriptWitnessItem: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = false)
 |    |    |    |    |-- stackItemCounter: binary (nullable = false)
 |    |    |    |    |-- scriptWitnessList: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = false)
 |    |    |    |    |    |    |-- witnessScriptLength: binary (nullable = false)
 |    |    |    |    |    |    |-- witnessScript: binary (nullable = false)
 |    |    |-- lockTime: long (nullable = false)
 
```
## Format: org.zuinnote.spark.bitcoin.block (readAuxPOW=true,enrich=true)
```
root
 |-- blockSize: long (nullable = false)
 |-- magicNo: binary (nullable = false)
 |-- version: long (nullable = false)
 |-- time: long (nullable = false)
 |-- bits: binary (nullable = false)
 |-- nonce: integer (nullable = false)
 |-- transactionCounter: long (nullable = false)
 |-- hashPrevBlock: binary (nullable = false)
 |-- hashMerkleRoot: binary (nullable = false)
 |-- transactions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- version: long (nullable = false)
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
 |    |    |    |    |-- value: decimal(38,0) (nullable = false)
 |    |    |    |    |-- txOutScriptLength: binary (nullable = false)
 |    |    |    |    |-- txOutScript: binary (nullable = false)
 |    |    |-- listOfScriptWitnessItem: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = false)
 |    |    |    |    |-- stackItemCounter: binary (nullable = false)
 |    |    |    |    |-- scriptWitnessList: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = false)
 |    |    |    |    |    |    |-- witnessScriptLength: binary (nullable = false)
 |    |    |    |    |    |    |-- witnessScript: binary (nullable = false)
 |    |    |-- lockTime: long (nullable = false)
 |    |    |-- currentTransactionHash: binary (nullable = false)
 |-- auxPOW: struct (nullable = true)
 |    |-- version: long (nullable = false)
 |    |-- coinbaseTransaction: struct (nullable = false)
 |    |    |-- version: long (nullable = false)
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
 |    |    |    |    |-- value: decimal(38,0) (nullable = false)
 |    |    |    |    |-- txOutScriptLength: binary (nullable = false)
 |    |    |    |    |-- txOutScript: binary (nullable = false)
 |    |    |-- lockTime: long (nullable = false)
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
 |    |    |-- version: long (nullable = false)
 |    |    |-- previousBlockHash: binary (nullable = false)
 |    |    |-- merkleRoot: binary (nullable = false)
 |    |    |-- time: long (nullable = false)
 |    |    |-- bits: binary (nullable = false)
 |    |    |-- nonce: long (nullable = false)
```
## Format: org.zuinnote.spark.bitcoin.block (readAuxPOW=true,enrich=false)
```
root
 |-- blockSize: long (nullable = false)
 |-- magicNo: binary (nullable = true)
 |-- version: long (nullable = false)
 |-- time: long (nullable = false)
 |-- bits: binary (nullable = true)
 |-- nonce: long (nullable = false)
 |-- transactionCounter: long (nullable = false)
 |-- hashPrevBlock: binary (nullable = true)
 |-- hashMerkleRoot: binary (nullable = true)
 |-- transactions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- version: long (nullable = false)
 |    |    |-- marker: byte (nullable = false)
 |    |    |-- flag: byte (nullable = false)
 |    |    |-- inCounter: binary (nullable = true)
 |    |    |-- outCounter: binary (nullable = true)
 |    |    |-- listOfInputs: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- prevTransactionHash: binary (nullable = true)
 |    |    |    |    |-- previousTxOutIndex: long (nullable = false)
 |    |    |    |    |-- txInScriptLength: binary (nullable = true)
 |    |    |    |    |-- txInScript: binary (nullable = true)
 |    |    |    |    |-- seqNo: long (nullable = false)
 |    |    |-- listOfOutputs: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- value: decimal(38,0) (nullable = true)
 |    |    |    |    |-- txOutScriptLength: binary (nullable = true)
 |    |    |    |    |-- txOutScript: binary (nullable = true)
 |    |    |-- listOfScriptWitnessItem: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- stackItemCounter: binary (nullable = true)
 |    |    |    |    |-- scriptWitnessList: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- witnessScriptLength: binary (nullable = true)
 |    |    |    |    |    |    |-- witnessScript: binary (nullable = true)
 |    |    |-- lockTime: long (nullable = false)
 |-- auxPOW: struct (nullable = true)
 |    |-- version: long (nullable = false)
 |    |-- coinbaseTransaction: struct (nullable = true)
 |    |    |-- version: long (nullable = false)
 |    |    |-- inCounter: binary (nullable = true)
 |    |    |-- outCounter: binary (nullable = true)
 |    |    |-- listOfInputs: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- prevTransactionHash: binary (nullable = true)
 |    |    |    |    |-- previousTxOutIndex: long (nullable = false)
 |    |    |    |    |-- txInScriptLength: binary (nullable = true)
 |    |    |    |    |-- txInScript: binary (nullable = true)
 |    |    |    |    |-- seqNo: long (nullable = false)
 |    |    |-- listOfOutputs: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- value: decimal(38,0) (nullable = true)
 |    |    |    |    |-- txOutScriptLength: binary (nullable = true)
 |    |    |    |    |-- txOutScript: binary (nullable = true)
 |    |    |-- lockTime: long (nullable = false)
 |    |-- parentBlockHeaderHash: binary (nullable = true)
 |    |-- coinbaseBranch: struct (nullable = true)
 |    |    |-- numberOfLinks: binary (nullable = true)
 |    |    |-- links: array (nullable = true)
 |    |    |    |-- element: binary (containsNull = true)
 |    |    |-- branchSideBitmask: binary (nullable = true)
 |    |-- auxBlockChainBranch: struct (nullable = true)
 |    |    |-- numberOfLinks: binary (nullable = true)
 |    |    |-- links: array (nullable = true)
 |    |    |    |-- element: binary (containsNull = true)
 |    |    |-- branchSideBitmask: binary (nullable = true)
 |    |-- parentBlockHeader: struct (nullable = true)
 |    |    |-- version: long (nullable = false)
 |    |    |-- previousBlockHash: binary (nullable = true)
 |    |    |-- merkleRoot: binary (nullable = true)
 |    |    |-- time: long (nullable = false)
 |    |    |-- bits: binary (nullable = true)
 |    |    |-- nonce: long (nullable = false)
```
## Format: org.zuinnote.spark.bitcoin.transaction
```
root
 |-- currentTransactionHash: binary (nullable = true)
 |-- version: long (nullable = false)
 |-- marker: byte (nullable = false)
 |-- flag: byte (nullable = false)
 |-- inCounter: binary (nullable = true)
 |-- outCounter: binary (nullable = true)
 |-- listOfInputs: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- prevTransactionHash: binary (nullable = true)
 |    |    |-- previousTxOutIndex: long (nullable = false)
 |    |    |-- txInScriptLength: binary (nullable = true)
 |    |    |-- txInScript: binary (nullable = true)
 |    |    |-- seqNo: long (nullable = false)
 |-- listOfOutputs: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- value: decimal(38,0) (nullable = true)
 |    |    |-- txOutScriptLength: binary (nullable = true)
 |    |    |-- txOutScript: binary (nullable = true)
 |-- listOfScriptWitnessItem: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- stackItemCounter: binary (nullable = true)
 |    |    |-- scriptWitnessList: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- witnessScriptLength: binary (nullable = true)
 |    |    |    |    |-- witnessScript: binary (nullable = true)
 |-- lockTime: long (nullable = false)
                                                                                                                                                       
```
## Format: org.zuinnote.spark.ethereum.block (enrich=true)
```
root
 |-- ethereumBlockHeader: struct (nullable = true)
 |    |-- parentHash: binary (nullable = true)
 |    |-- uncleHash: binary (nullable = true)
 |    |-- coinBase: binary (nullable = true)
 |    |-- stateRoot: binary (nullable = true)
 |    |-- txTrieRoot: binary (nullable = true)
 |    |-- receiptTrieRoot: binary (nullable = true)
 |    |-- logsBloom: binary (nullable = true)
 |    |-- difficulty: binary (nullable = true)
 |    |-- timestamp: long (nullable = false)
 |    |-- number: decimal(38,0) (nullable = true)
 |    |-- numberRaw: binary (nullable = true)
 |    |-- gasLimit: decimal(38,0) (nullable = true)
 |    |-- gasLimitRaw: binary (nullable = true)
 |    |-- gasUsed: decimal(38,0) (nullable = true)
 |    |-- gasUsedRaw: binary (nullable = true)
 |    |-- mixHash: binary (nullable = true)
 |    |-- extraData: binary (nullable = true)
 |    |-- nonce: binary (nullable = true)
 |-- ethereumTransactions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- nonce: binary (nullable = true)
 |    |    |-- value: decimal(38,0) (nullable = true)
 |    |    |-- valueRaw: binary (nullable = true)
 |    |    |-- receiveAddress: binary (nullable = true)
 |    |    |-- gasPrice: decimal(38,0) (nullable = true)
 |    |    |-- gasPriceRaw: binary (nullable = true)
 |    |    |-- gasLimit: decimal(38,0) (nullable = true)
 |    |    |-- gasLimitRaw: binary (nullable = true)
 |    |    |-- data: binary (nullable = true)
 |    |    |-- sig_v: binary (nullable = true)
 |    |    |-- sig_r: binary (nullable = true)
 |    |    |-- sig_s: binary (nullable = true)
 |    |    |-- sendAddress: binary (nullable = true)
 |    |    |-- hash: binary (nullable = true)
 |-- uncleHeaders: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- parentHash: binary (nullable = true)
 |    |    |-- uncleHash: binary (nullable = true)
 |    |    |-- coinBase: binary (nullable = true)
 |    |    |-- stateRoot: binary (nullable = true)
 |    |    |-- txTrieRoot: binary (nullable = true)
 |    |    |-- receiptTrieRoot: binary (nullable = true)
 |    |    |-- logsBloom: binary (nullable = true)
 |    |    |-- difficulty: binary (nullable = true)
 |    |    |-- timestamp: long (nullable = false)
 |    |    |-- number: decimal(38,0) (nullable = true)
 |    |    |-- numberRaw: binary (nullable = true)
 |    |    |-- gasLimit: decimal(38,0) (nullable = true)
 |    |    |-- gasLimitRaw: binary (nullable = true)
 |    |    |-- gasUsed: decimal(38,0) (nullable = true)
 |    |    |-- gasUsedRaw: binary (nullable = true)
 |    |    |-- mixHash: binary (nullable = true)
 |    |    |-- extraData: binary (nullable = true)
 |    |    |-- nonce: binary (nullable = true)                                                                                                          
```

## Format: org.zuinnote.spark.ethereum.block (enrich=false)
```
root
 |-- ethereumBlockHeader: struct (nullable = true)
 |    |-- parentHash: binary (nullable = true)
 |    |-- uncleHash: binary (nullable = true)
 |    |-- coinBase: binary (nullable = true)
 |    |-- stateRoot: binary (nullable = true)
 |    |-- txTrieRoot: binary (nullable = true)
 |    |-- receiptTrieRoot: binary (nullable = true)
 |    |-- logsBloom: binary (nullable = true)
 |    |-- difficulty: binary (nullable = true)
 |    |-- timestamp: long (nullable = false)
 |    |-- number: decimal(38,0) (nullable = true)
 |    |-- numberRaw: binary (nullable = true)
 |    |-- gasLimit: decimal(38,0) (nullable = true)
 |    |-- gasLimitRaw: binary (nullable = true)
 |    |-- gasUsed: decimal(38,0) (nullable = true)
 |    |-- gasUsedRaw: binary (nullable = true)
 |    |-- mixHash: binary (nullable = true)
 |    |-- extraData: binary (nullable = true)
 |    |-- nonce: binary (nullable = true)
 |-- ethereumTransactions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- nonce: binary (nullable = true)
 |    |    |-- value: decimal(38,0) (nullable = true)
 |    |    |-- valueRaw: binary (nullable = true)
 |    |    |-- receiveAddress: binary (nullable = true)
 |    |    |-- gasPrice: decimal(38,0) (nullable = true)
 |    |    |-- gasPriceRaw: binary (nullable = true)
 |    |    |-- gasLimit: decimal(38,0) (nullable = true)
 |    |    |-- gasLimitRaw: binary (nullable = true)
 |    |    |-- data: binary (nullable = true)
 |    |    |-- sig_v: binary (nullable = true)
 |    |    |-- sig_r: binary (nullable = true)
 |    |    |-- sig_s: binary (nullable = true)
 |-- uncleHeaders: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- parentHash: binary (nullable = true)
 |    |    |-- uncleHash: binary (nullable = true)
 |    |    |-- coinBase: binary (nullable = true)
 |    |    |-- stateRoot: binary (nullable = true)
 |    |    |-- txTrieRoot: binary (nullable = true)
 |    |    |-- receiptTrieRoot: binary (nullable = true)
 |    |    |-- logsBloom: binary (nullable = true)
 |    |    |-- difficulty: binary (nullable = true)
 |    |    |-- timestamp: long (nullable = false)
 |    |    |-- number: decimal(38,0) (nullable = true)
 |    |    |-- numberRaw: binary (nullable = true)
 |    |    |-- gasLimit: decimal(38,0) (nullable = true)
 |    |    |-- gasLimitRaw: binary (nullable = true)
 |    |    |-- gasUsed: decimal(38,0) (nullable = true)
 |    |    |-- gasUsedRaw: binary (nullable = true)
 |    |    |-- mixHash: binary (nullable = true)
 |    |    |-- extraData: binary (nullable = true)
 |    |    |-- nonce: binary (nullable = true)                                                                                                                          
```
