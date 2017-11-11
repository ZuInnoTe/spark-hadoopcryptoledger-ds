/**
* Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/
package org.zuinnote.spark.ethereum.block

import scala.collection.JavaConversions._


import org.apache.spark.sql.sources.{BaseRelation,TableScan}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf._
import org.apache.hadoop.mapred._



import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log


import org.zuinnote.hadoop.ethereum.format.common._
import org.zuinnote.hadoop.ethereum.format.mapreduce._
import org.zuinnote.spark.ethereum.util.EthereumBlockFile

/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Defines the schema of a EthereumBlock for Spark SQL
*
*/

case class EthereumBlockRelation(location: String,maxBlockSize: Integer = AbstractEthereumRecordReader.DEFAULT_MAXSIZE_ETHEREUMBLOCK,useDirectBuffer: Boolean = AbstractEthereumRecordReader.DEFAULT_USEDIRECTBUFFER)
(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan
       with Serializable {
  val LOG = LogFactory.getLog(EthereumBlockRelation.getClass);

  override def schema: StructType = {
    val structEthereum = StructType(Seq(
      StructField("ethereumBlockHeader",StructType(Seq(
                StructField("parentHash",BinaryType,false),
                StructField("uncleHash",BinaryType,false),
                StructField("coinBase",BinaryType,false),
                StructField("stateRoot",BinaryType,false),
                StructField("txTrieRoot",BinaryType,false),
                StructField("receiptTrieRoot",BinaryType,false),
                StructField("logsBloom",BinaryType,false),
                StructField("difficulty",BinaryType,false),
                StructField("timestamp",LongType,false),
                StructField("number",LongType,false),
                StructField("gasLimit",LongType,false),
                StructField("gasUsed",LongType,false),
                StructField("mixHash",BinaryType,false),
                StructField("extraData",BinaryType,false),
                StructField("nonce",BinaryType,false)
      )),false),
      StructField("ethereumTransactions",ArrayType(StructType(Seq(
                StructField("nonce",BinaryType,false),
                StructField("value",LongType,false),
                StructField("receiveAddress",BinaryType,false),
                StructField("gasPrice",LongType,false),
                StructField("gasLimit",LongType,false),
                StructField("data",BinaryType,false),
                StructField("sig_v",BinaryType,false),
                StructField("sig_r",BinaryType,false),
                StructField("sig_s",BinaryType,false)
      ))),false),
        StructField("uncleHeaders",ArrayType(StructType(Seq(
                StructField("parentHash",BinaryType,false),
                StructField("uncleHash",BinaryType,false),
                StructField("coinBase",BinaryType,false),
                StructField("stateRoot",BinaryType,false),
                StructField("txTrieRoot",BinaryType,false),
                StructField("receiptTrieRoot",BinaryType,false),
                StructField("logsBloom",BinaryType,false),
                StructField("difficulty",BinaryType,false),
                StructField("timestamp",LongType,false),
                StructField("number",LongType,false),
                StructField("gasLimit",LongType,false),
                StructField("gasUsed",LongType,false),
                StructField("mixHash",BinaryType,false),
                StructField("extraData",BinaryType,false),
                StructField("nonce",BinaryType,false)
        ))),false)))

    return structEthereum
    }



    /**
     * Used by Spark to fetch Ethereum blocks according to the schema specified above from files.
     *
     *
     * returns EthereumBlocks as rows
    **/
    override def buildScan: RDD[Row] = {
	// create hadoopConf
	val hadoopConf = new Configuration()
 	hadoopConf.set(AbstractEthereumRecordReader.CONF_MAXBLOCKSIZE,String.valueOf(maxBlockSize))
 	hadoopConf.set(AbstractEthereumRecordReader.CONF_USEDIRECTBUFFER,String.valueOf(useDirectBuffer))
        // read BitcoinBlock
	val ethereumBlockRDD = EthereumBlockFile.load(sqlContext, location, hadoopConf)
        // map to schema
	val schemaFields = schema.fields
	val rowArray = new Array[Any](schemaFields.length)
        ethereumBlockRDD.flatMap(hadoopKeyValueTuple => {
		// map the EthereumBlock data structure to a Spark SQL schema
    val ethereumBlockStructArray = new Array[Any](3)
    val ethereumBlockHeaderStructArray = new Array[Any](15)
    ethereumBlockHeaderStructArray(0) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getParentHash
    ethereumBlockHeaderStructArray(1) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getUncleHash
    ethereumBlockHeaderStructArray(2) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getCoinBase
    ethereumBlockHeaderStructArray(3) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getStateRoot
    ethereumBlockHeaderStructArray(4) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getTxTrieRoot
    ethereumBlockHeaderStructArray(5) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getReceiptTrieRoot
    ethereumBlockHeaderStructArray(6) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getLogsBloom
    ethereumBlockHeaderStructArray(7) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getDifficulty
    ethereumBlockHeaderStructArray(8) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getTimestamp
    ethereumBlockHeaderStructArray(9) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getNumber
    ethereumBlockHeaderStructArray(10) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getGasLimit
    ethereumBlockHeaderStructArray(11) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getGasUsed
    ethereumBlockHeaderStructArray(12) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getMixHash
    ethereumBlockHeaderStructArray(13) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getExtraData
    ethereumBlockHeaderStructArray(14) = hadoopKeyValueTuple._2.getEthereumBlockHeader.getNonce
    rowArray(0)= Row.fromSeq(ethereumBlockHeaderStructArray)
    val ethereumTransactionArray=new Array[Any](hadoopKeyValueTuple._2.getEthereumTransactions().size())
    var i=0
    for (currentTransaction <- hadoopKeyValueTuple._2.getEthereumTransactions()) {
        val ethereumTransactionStructArray = new Array[Any](9)
        ethereumTransactionStructArray(0) = currentTransaction.getNonce
        ethereumTransactionStructArray(1) = currentTransaction.getValue
        ethereumTransactionStructArray(2) = currentTransaction.getReceiveAddress
        ethereumTransactionStructArray(3) = currentTransaction.getGasPrice
        ethereumTransactionStructArray(4) = currentTransaction.getGasLimit
        ethereumTransactionStructArray(5) = currentTransaction.getData
        ethereumTransactionStructArray(6) = currentTransaction.getSig_v
        ethereumTransactionStructArray(7) = currentTransaction.getSig_r
        ethereumTransactionStructArray(8) = currentTransaction.getSig_s
        ethereumTransactionArray(i)=Row.fromSeq(ethereumTransactionStructArray)
        i += 1
    }
    rowArray(1) = ethereumTransactionArray
    val uncleHeadersArray = new Array[Any](hadoopKeyValueTuple._2.getUncleHeaders().size())
    i=0
    for (currentUncleHeader <- hadoopKeyValueTuple._2.getUncleHeaders()) {
      val ethereumUncleHeaderStructArray = new Array[Any](15)
      ethereumUncleHeaderStructArray(0) = currentUncleHeader.getParentHash
      ethereumUncleHeaderStructArray(1) = currentUncleHeader.getUncleHash
      ethereumUncleHeaderStructArray(2) = currentUncleHeader.getCoinBase
      ethereumUncleHeaderStructArray(3) = currentUncleHeader.getStateRoot
      ethereumUncleHeaderStructArray(4) = currentUncleHeader.getTxTrieRoot
      ethereumUncleHeaderStructArray(5) = currentUncleHeader.getReceiptTrieRoot
      ethereumUncleHeaderStructArray(6) = currentUncleHeader.getLogsBloom
      ethereumUncleHeaderStructArray(7) = currentUncleHeader.getDifficulty
      ethereumUncleHeaderStructArray(8) = currentUncleHeader.getTimestamp
      ethereumUncleHeaderStructArray(9) = currentUncleHeader.getNumber
      ethereumUncleHeaderStructArray(10) = currentUncleHeader.getGasLimit
      ethereumUncleHeaderStructArray(11) = currentUncleHeader.getGasUsed
      ethereumUncleHeaderStructArray(12) = currentUncleHeader.getMixHash
      ethereumUncleHeaderStructArray(13) = currentUncleHeader.getExtraData
      ethereumUncleHeaderStructArray(14) = currentUncleHeader.getNonce
      uncleHeadersArray(i)= Row.fromSeq(ethereumUncleHeaderStructArray)
      i += 1
    }
    rowArray(2) = uncleHeadersArray
	 	// add row representing one Ethereum Block
          	Some(Row.fromSeq(rowArray))
		}
        )

     }


}
