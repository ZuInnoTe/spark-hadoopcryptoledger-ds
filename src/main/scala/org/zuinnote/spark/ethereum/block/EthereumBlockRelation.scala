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

import org.apache.hadoop.conf._
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row, SQLContext}
import org.zuinnote.hadoop.ethereum.format.common
import org.zuinnote.hadoop.ethereum.format.mapreduce._
import org.zuinnote.spark.ethereum.model._

/**
  * Author: Jörn Franke <zuinnote@gmail.com>
  *
  */
/**
  * Defines the schema of a EthereumBlock for Spark SQL
  *
  */
final case class EthereumBlockRelation(location: String,
                                       maxBlockSize: Integer = AbstractEthereumRecordReader.DEFAULT_MAXSIZE_ETHEREUMBLOCK,
                                       useDirectBuffer: Boolean = AbstractEthereumRecordReader.DEFAULT_USEDIRECTBUFFER,
                                       enrich: Boolean = false, chainId: Integer=1)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    if (enrich) {
      Encoders.product[EnrichedEthereumBlock].schema
    } else {
      Encoders.product[EthereumBlock].schema
    }
  }

  /**
    * Used by Spark to fetch Ethereum blocks according to the schema specified above from files.
    *
    *
    * returns EthereumBlocks as rows
    **/
  override def buildScan: RDD[Row] = {
    val ethereumBlockRDD: RDD[(BytesWritable, common.EthereumBlockWritable)] = readRawBlockRDD()

    if (enrich) {
      ethereumBlockRDD
        .map { case (_, block) => block.asScalaEnriched(chainId) }
        .map(Row.fromTuple)
    } else {
      ethereumBlockRDD
        .map { case (_, block) => block.asScala }
        .map(Row.fromTuple)
    }
  }

  private def readRawBlockRDD(): RDD[(BytesWritable, common.EthereumBlockWritable)] = {
    // create hadoopConf
    val hadoopConf = new Configuration()
    hadoopConf.set(AbstractEthereumRecordReader.CONF_MAXBLOCKSIZE, String.valueOf(maxBlockSize))
    hadoopConf.set(AbstractEthereumRecordReader.CONF_USEDIRECTBUFFER, String.valueOf(useDirectBuffer))
    // read Ethereum Block
    sqlContext.sparkContext.newAPIHadoopFile(
      location,
      classOf[EthereumBlockFileInputFormat],
      classOf[BytesWritable],
      classOf[common.EthereumBlockWritable],
      hadoopConf
    )
  }
}
