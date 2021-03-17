/**
  * Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.spark.bitcoin.transaction

import org.apache.hadoop.conf._
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row, SQLContext}
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransactionWritable
import org.zuinnote.hadoop.bitcoin.format.mapreduce._
import org.zuinnote.spark.bitcoin.model._

/**
  * Author: Jörn Franke <zuinnote@gmail.com>
  *
  */
/**
  * Defines the schema of a BitcoinTransaction for Spark SQL
  *
  */
final case class BitcoinTransactionRelation(location: String,
                                            maxBlockSize: Integer = AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK,
                                            magic: String = AbstractBitcoinRecordReader.DEFAULT_MAGIC,
                                            useDirectBuffer: Boolean = AbstractBitcoinRecordReader.DEFAULT_USEDIRECTBUFFER,
                                            isSplittable: Boolean = AbstractBitcoinFileInputFormat.DEFAULT_ISSPLITABLE,
                                            readAuxPOW: Boolean = AbstractBitcoinRecordReader.DEFAULT_READAUXPOW)
                                           (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with Serializable {

  override def schema: StructType = {
    val encodedSchema=Encoders.product[SingleTransaction].schema

  encodedSchema
  }

  /**
    * Used by Spark to fetch Bitcoin transactions according to the schema specified above from files.
    *
    *
    * returns BitcoinTransactions as rows
    **/
  override def buildScan: RDD[Row] = {
    readRawTransactionRDD()
      .map { case (transactionHash, currentTransaction) => currentTransaction.asScalaSingle(transactionHash.copyBytes()) }
      .map(Row.fromTuple)
  }

  private def readRawTransactionRDD(): RDD[(BytesWritable, BitcoinTransactionWritable)] = {
    // create hadoopConf
    val hadoopConf = new Configuration()
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_MAXBLOCKSIZE, String.valueOf(maxBlockSize))
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_FILTERMAGIC, magic)
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_USEDIRECTBUFFER, String.valueOf(useDirectBuffer))
    hadoopConf.set(AbstractBitcoinFileInputFormat.CONF_ISSPLITABLE, String.valueOf(isSplittable))
    // read BitcoinBlock
    sqlContext.sparkContext.newAPIHadoopFile(
      location,
      classOf[BitcoinTransactionFileInputFormat],
      classOf[BytesWritable],
      classOf[BitcoinTransactionWritable],
      hadoopConf
    )
  }
}
