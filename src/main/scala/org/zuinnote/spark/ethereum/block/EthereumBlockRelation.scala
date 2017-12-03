/**
  * Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/
package org.zuinnote.spark.ethereum.block

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf._
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, _}
import org.zuinnote.hadoop.ethereum.format.common
import org.zuinnote.hadoop.ethereum.format.mapreduce._
import org.zuinnote.spark.ethereum.util.EthereumBlockFile
import org.zuinnote.spark.ethereum.model._

/**
  * Author: Jörn Franke <zuinnote@gmail.com>
  *
  */
/**
  * Defines the schema of a EthereumBlock for Spark SQL
  *
  */
case class EthereumBlockRelation(location: String,
                                 maxBlockSize: Integer = AbstractEthereumRecordReader.DEFAULT_MAXSIZE_ETHEREUMBLOCK,
                                 useDirectBuffer: Boolean = AbstractEthereumRecordReader.DEFAULT_USEDIRECTBUFFER,
                                 enrich: Boolean = false)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  private lazy val LOG = LogFactory.getLog(EthereumBlockRelation.getClass)

  override def schema: StructType = {
    val structEthereum = StructType(
      Seq(
        StructField(
          "ethereumBlockHeader",
          StructType(Seq(
            StructField("parentHash", BinaryType, nullable = false),
            StructField("uncleHash", BinaryType, nullable = false),
            StructField("coinBase", BinaryType, nullable = false),
            StructField("stateRoot", BinaryType, nullable = false),
            StructField("txTrieRoot", BinaryType, nullable = false),
            StructField("receiptTrieRoot", BinaryType, nullable = false),
            StructField("logsBloom", BinaryType, nullable = false),
            StructField("difficulty", BinaryType, nullable = false),
            StructField("timestamp", LongType, nullable = false),
            StructField("number", LongType, nullable = false),
            StructField("gasLimit", LongType, nullable = false),
            StructField("gasUsed", LongType, nullable = false),
            StructField("mixHash", BinaryType, nullable = false),
            StructField("extraData", BinaryType, nullable = false),
            StructField("nonce", BinaryType, nullable = false)
          )),
          nullable = false
        ),
        StructField(
          "ethereumTransactions",
          ArrayType(StructType(Seq(
            StructField("nonce", BinaryType, nullable = false),
            StructField("value", LongType, nullable = false),
            StructField("receiveAddress", BinaryType, nullable = false),
            StructField("gasPrice", LongType, nullable = false),
            StructField("gasLimit", LongType, nullable = false),
            StructField("data", BinaryType, nullable = false),
            StructField("sig_v", BinaryType, nullable = false),
            StructField("sig_r", BinaryType, nullable = false),
            StructField("sig_s", BinaryType, nullable = false)
          ))),
          nullable = false
        ),
        StructField(
          "uncleHeaders",
          ArrayType(StructType(Seq(
            StructField("parentHash", BinaryType, nullable = false),
            StructField("uncleHash", BinaryType, nullable = false),
            StructField("coinBase", BinaryType, nullable = false),
            StructField("stateRoot", BinaryType, nullable = false),
            StructField("txTrieRoot", BinaryType, nullable = false),
            StructField("receiptTrieRoot", BinaryType, nullable = false),
            StructField("logsBloom", BinaryType, nullable = false),
            StructField("difficulty", BinaryType, nullable = false),
            StructField("timestamp", LongType, nullable = false),
            StructField("number", LongType, nullable = false),
            StructField("gasLimit", LongType, nullable = false),
            StructField("gasUsed", LongType, nullable = false),
            StructField("mixHash", BinaryType, nullable = false),
            StructField("extraData", BinaryType, nullable = false),
            StructField("nonce", BinaryType, nullable = false)
          ))),
          nullable = false
        )
      ))

    val structEthereumEnrich = StructType(
      Seq(
        StructField(
          "ethereumBlockHeader",
          StructType(Seq(
            StructField("parentHash", BinaryType, nullable = false),
            StructField("uncleHash", BinaryType, nullable = false),
            StructField("coinBase", BinaryType, nullable = false),
            StructField("stateRoot", BinaryType, nullable = false),
            StructField("txTrieRoot", BinaryType, nullable = false),
            StructField("receiptTrieRoot", BinaryType, nullable = false),
            StructField("logsBloom", BinaryType, nullable = false),
            StructField("difficulty", BinaryType, nullable = false),
            StructField("timestamp", LongType, nullable = false),
            StructField("number", LongType, nullable = false),
            StructField("gasLimit", LongType, nullable = false),
            StructField("gasUsed", LongType, nullable = false),
            StructField("mixHash", BinaryType, nullable = false),
            StructField("extraData", BinaryType, nullable = false),
            StructField("nonce", BinaryType, nullable = false)
          )),
          nullable = false
        ),
        StructField(
          "ethereumTransactions",
          ArrayType(StructType(Seq(
            StructField("nonce", BinaryType, nullable = false),
            StructField("value", LongType, nullable = false),
            StructField("receiveAddress", BinaryType, nullable = false),
            StructField("gasPrice", LongType, nullable = false),
            StructField("gasLimit", LongType, nullable = false),
            StructField("data", BinaryType, nullable = false),
            StructField("sig_v", BinaryType, nullable = false),
            StructField("sig_r", BinaryType, nullable = false),
            StructField("sig_s", BinaryType, nullable = false),
            StructField("sendAddress", BinaryType, nullable = false),
            StructField("hash", BinaryType, nullable = false)
          ))),
          nullable = false
        ),
        StructField(
          "uncleHeaders",
          ArrayType(StructType(Seq(
            StructField("parentHash", BinaryType, nullable = false),
            StructField("uncleHash", BinaryType, nullable = false),
            StructField("coinBase", BinaryType, nullable = false),
            StructField("stateRoot", BinaryType, nullable = false),
            StructField("txTrieRoot", BinaryType, nullable = false),
            StructField("receiptTrieRoot", BinaryType, nullable = false),
            StructField("logsBloom", BinaryType, nullable = false),
            StructField("difficulty", BinaryType, nullable = false),
            StructField("timestamp", LongType, nullable = false),
            StructField("number", LongType, nullable = false),
            StructField("gasLimit", LongType, nullable = false),
            StructField("gasUsed", LongType, nullable = false),
            StructField("mixHash", BinaryType, nullable = false),
            StructField("extraData", BinaryType, nullable = false),
            StructField("nonce", BinaryType, nullable = false)
          ))),
          nullable = false
        )
      ))

    if (enrich) {
      structEthereumEnrich
    } else {
      structEthereum
    }
  }

  /**
    * Used by Spark to fetch Ethereum blocks according to the schema specified above from files.
    *
    *
    * returns EthereumBlocks as rows
    **/
  override def buildScan: RDD[Row] = {
    val ethereumBlockRDD: RDD[(BytesWritable, common.EthereumBlock)] = readRawBlockRDD()

    if (enrich) {
      ethereumBlockRDD
        .map { case (_, block) => block.asScalaEnriched }
        .map(Row.fromTuple)
    } else {
      ethereumBlockRDD
        .map { case (_, block) => block.asScala }
        .map(Row.fromTuple)
    }
  }

  private def readRawBlockRDD(): RDD[(BytesWritable, common.EthereumBlock)] = {
    // create hadoopConf
    val hadoopConf = new Configuration()
    hadoopConf.set(AbstractEthereumRecordReader.CONF_MAXBLOCKSIZE, String.valueOf(maxBlockSize))
    hadoopConf.set(AbstractEthereumRecordReader.CONF_USEDIRECTBUFFER, String.valueOf(useDirectBuffer))
    // read Ethereum Block
    EthereumBlockFile.load(sqlContext, location, hadoopConf)
  }
}
