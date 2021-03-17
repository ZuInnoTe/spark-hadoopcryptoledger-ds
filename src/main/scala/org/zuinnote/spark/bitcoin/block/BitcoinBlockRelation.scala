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
package org.zuinnote.spark.bitcoin.block

import org.apache.hadoop.conf._
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row, SQLContext}
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinBlockWritable => RawBitcoinBlock}
import org.zuinnote.hadoop.bitcoin.format.mapreduce._
import org.zuinnote.spark.bitcoin.model._

import scala.collection.JavaConverters._
/**
  * Author: Jörn Franke <zuinnote@gmail.com>
  *
  * Defines the schema of a BitcoinBlock for Spark SQL
  */
final case class BitcoinBlockRelation(location: String,
                                      maxBlockSize: Integer = AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK,
                                      magic: String = AbstractBitcoinRecordReader.DEFAULT_MAGIC,
                                      useDirectBuffer: Boolean = AbstractBitcoinRecordReader.DEFAULT_USEDIRECTBUFFER,
                                      isSplittable: Boolean = AbstractBitcoinFileInputFormat.DEFAULT_ISSPLITABLE,
                                      readAuxPOW: Boolean = AbstractBitcoinRecordReader.DEFAULT_READAUXPOW,
                                      enrich: Boolean = false)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    var result: StructType = null;
    if (readAuxPOW) {

      if (enrich) {
        result=Encoders.product[EnrichedBitcoinBlockWithAuxPOW].schema
      } else {
        result=Encoders.product[BitcoinBlockWithAuxPOW].schema
      }
    } else {
      if (enrich) {
        result=Encoders.product[EnrichedBitcoinBlock].schema
      } else {
        result=Encoders.product[BitcoinBlock].schema
      }
    }
    result
  }

  /**
    * Used by Spark to fetch Bitcoin blocks according to the schema specified above from files.
    *
    *
    * returns BitcoinBlocks as rows
    **/
  override def buildScan: RDD[Row] = {
    readRawBlockRDD()
      .map { case (_, currentBlock) =>
        val block = BitcoinBlock(
          currentBlock.getBlockSize,
          currentBlock.getMagicNo,
          currentBlock.getVersion,
          currentBlock.getTime,
          currentBlock.getBits,
          currentBlock.getNonce,
          currentBlock.getTransactionCounter,
          currentBlock.getHashPrevBlock,
          currentBlock.getHashMerkleRoot,
          Seq.empty
        )

        val blockWithTransactions: Product with CanAddAuxPOW =
          if (enrich) {
            block.enriched(transactions = currentBlock.getTransactions.asScala.map(_.asScalaEnriched))
          } else {
            block.copy(transactions = currentBlock.getTransactions.asScala.map(_.asScala))
          }

        if (readAuxPOW) {
          blockWithTransactions.withAuxPOW(currentBlock.getAuxPOW.asScala)
        } else {
          blockWithTransactions
        }
      }
      .map(Row.fromTuple)
  }

  private def readRawBlockRDD(): RDD[(BytesWritable, RawBitcoinBlock)] = {
    // create hadoopConf
    val hadoopConf = new Configuration()
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_MAXBLOCKSIZE, String.valueOf(maxBlockSize))
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_FILTERMAGIC, magic)
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_USEDIRECTBUFFER, String.valueOf(useDirectBuffer))
    hadoopConf.set(AbstractBitcoinFileInputFormat.CONF_ISSPLITABLE, String.valueOf(isSplittable))
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_READAUXPOW, String.valueOf(readAuxPOW))
    // read BitcoinBlock
    sqlContext.sparkContext.newAPIHadoopFile(
      location,
      classOf[BitcoinBlockFileInputFormat],
      classOf[BytesWritable],
      classOf[RawBitcoinBlock],
      hadoopConf
    )
  }
}
