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
package org.zuinnote.spark.bitcoin.transaction


import org.apache.spark.sql.{DataFrame, SQLContext}

import org.zuinnote.hadoop.bitcoin.format._

   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/



package object bitcoinblock {

/**
   * Adds a method, `bitcoinTransactionFile`, to SQLContext that allows reading Bitcoin blockchain data as Bitcoin transactions.
   */

 implicit class BitcoinTransactionContext(sqlContext: SQLContext) extends Serializable{
def bitcoinTransactionFile(
        filePath: String,
	maxBlockSize: Integer = AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK,
	magic: String = AbstractBitcoinRecordReader.DEFAULT_MAGIC,
	useDirectBuffer: Boolean = AbstractBitcoinRecordReader.DEFAULT_USEDIRECTBUFFER,
	isSplitable: Boolean = AbstractBitcoinFileInputFormat.DEFAULT_ISSPLITABLE
       ): DataFrame = {
      val bitcoinTransactionRelation = BitcoinTransactionRelation(filePath,maxBlockSize,magic,useDirectBuffer,isSplitable)(sqlContext)
      sqlContext.baseRelationToDataFrame(bitcoinTransactionRelation)
}
}



}
