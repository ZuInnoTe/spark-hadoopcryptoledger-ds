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
package org.zuinnote.spark.ethereum

import org.apache.spark.sql.{DataFrame, SQLContext}

import org.zuinnote.hadoop.ethereum.format.common._
import org.zuinnote.hadoop.ethereum.format.mapreduce._

/**
  * Author: Jörn Franke <zuinnote@gmail.com>
  *
  */
package object block {

  /**
    * Adds a method, `etheruemBlockFile`, to SQLContext that allows reading Etheruem blockchain data as Ethereum blocks.
    */
  implicit class EthereumBlockContext(sqlContext: SQLContext) extends Serializable {
    def ethereumBlockFile(
        filePath: String,
        maxBlockSize: Integer = AbstractEthereumRecordReader.DEFAULT_MAXSIZE_ETHEREUMBLOCK,
        useDirectBuffer: Boolean = AbstractEthereumRecordReader.DEFAULT_USEDIRECTBUFFER
    ): DataFrame = {
      val ethereumBlockRelation = EthereumBlockRelation(filePath, maxBlockSize, useDirectBuffer)(sqlContext)
      sqlContext.baseRelationToDataFrame(ethereumBlockRelation)
    }
  }
}
