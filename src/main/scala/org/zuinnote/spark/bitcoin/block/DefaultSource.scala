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
package org.zuinnote.spark.bitcoin.block

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import org.zuinnote.hadoop.bitcoin.format.mapreduce._
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
*
* Defines a Spark data source for Bitcoin Blockchcain based on hadoopcryptoledgerlibrary. This is read-only for existing data. It returns BitcoinBlocks.
*
*/

class DefaultSource
  extends RelationProvider
   {

	/**
	 * Used by Spark to fetch information about the data and initiate parsing
	 *
	*
	*/
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BitcoinBlockRelation = {
      val path= parameters.getOrElse("path", sys.error("'path' must be specified with files containing Bitcoin blockchain data."))
      val maxBlockSize = Integer.valueOf(parameters.getOrElse("maxBlockSize", String.valueOf(AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK)))
      val magic = parameters.getOrElse("magic", AbstractBitcoinRecordReader.DEFAULT_MAGIC)
      val useDirectBuffer = parameters.getOrElse("useDirectBuffer", String.valueOf(AbstractBitcoinRecordReader.DEFAULT_USEDIRECTBUFFER)).toBoolean
      val isSplitable = parameters.getOrElse("isSplitable", String.valueOf(AbstractBitcoinFileInputFormat.DEFAULT_ISSPLITABLE)).toBoolean
      // parse the parameters into hadoop objects
      BitcoinBlockRelation(path, maxBlockSize, magic, useDirectBuffer, isSplitable)(sqlContext)
  }

 
}
