/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.spark.bitcoin.transactionelement

import scala.collection.JavaConversions._

import org.apache.spark.sql.sources.{BaseRelation,TableScan}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.ArrayType
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

import org.zuinnote.hadoop.bitcoin.format.common._
import org.zuinnote.hadoop.bitcoin.format.mapreduce._   
   
import org.zuinnote.spark.bitcoin.util.BitcoinTransactionElementFile

/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Defines the schema of a BitcoinTransactionElement for Spark SQL
*
*/

case class BitcoinTransactionElementRelation(location: String,maxBlockSize: Integer = AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK,magic: String = AbstractBitcoinRecordReader.DEFAULT_MAGIC,useDirectBuffer: Boolean = AbstractBitcoinRecordReader.DEFAULT_USEDIRECTBUFFER,isSplitable: Boolean = AbstractBitcoinFileInputFormat.DEFAULT_ISSPLITABLE)
(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan
       with Serializable {
  val LOG = LogFactory.getLog(BitcoinTransactionElementRelation.getClass);

  override def schema: StructType = {
 
      return StructType(Seq(StructField("blockHash", BinaryType, false), 
                            StructField("transactionIdxInBlock", IntegerType, false),
				StructField("transactionHash", BinaryType, false),
				StructField("type", IntegerType, false),
				StructField("indexInTransaction",LongType,false),
				StructField("amount", LongType,false),	
				StructField("script", BinaryType, false)))
    }


    /**
     * Used by Spark to fetch Bitcoin blocks according to the schema specified above from files.
     *
     *
     * returns BitcoinBlocks as rows
    **/
    override def buildScan: RDD[Row] = {
	// create hadoopConf
	val hadoopConf = new Configuration()
 	hadoopConf.set(AbstractBitcoinRecordReader.CONF_MAXBLOCKSIZE,String.valueOf(maxBlockSize))
 	hadoopConf.set(AbstractBitcoinRecordReader.CONF_FILTERMAGIC,magic)
 	hadoopConf.set(AbstractBitcoinRecordReader.CONF_USEDIRECTBUFFER,String.valueOf(useDirectBuffer))
 	hadoopConf.set(AbstractBitcoinFileInputFormat.CONF_ISSPLITABLE, String.valueOf(isSplitable))
        // read BitcoinBlock
	val bitcoinTransactionElementRDD = BitcoinTransactionElementFile.load(sqlContext, location, hadoopConf)
        // map to schema
	val schemaFields = schema.fields
	val rowArray = new Array[Any](schemaFields.length)
        bitcoinTransactionElementRDD.flatMap(hadoopKeyValueTuple => {
		// map the BitcoinBlock data structure to a Spark SQL schema
		rowArray(0) = Row.fromSeq(hadoopKeyValueTuple._2.getBlockHash)
		rowArray(1) = hadoopKeyValueTuple._2.getTransactionIdxInBlock
		rowArray(2) = Row.fromSeq(hadoopKeyValueTuple._2.getTransactionHash)
		rowArray(3) = hadoopKeyValueTuple._2.getType
		rowArray(4) = hadoopKeyValueTuple._2.getIndexInTransaction
		rowArray(5) = hadoopKeyValueTuple._2.getAmount
		rowArray(6) = Row.fromSeq(hadoopKeyValueTuple._2.getScript)		
          	Some(Row.fromSeq(rowArray))
		}
        )

     }
  

}
