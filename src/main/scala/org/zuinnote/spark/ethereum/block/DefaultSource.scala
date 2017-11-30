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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.zuinnote.hadoop.ethereum.format.mapreduce._

/**
  * Author: Jörn Franke <zuinnote@gmail.com>
  *
  * Defines a Spark data source for Ethereum Blockchcain based on hadoopcryptoledgerlibrary. This is read-only for existing data. It returns EthereumBlocks.
  */
class DefaultSource extends RelationProvider {

  /**
    * Used by Spark to fetch information about the data and initiate parsing
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): EthereumBlockRelation = {
    val path =
      parameters.getOrElse("path", sys.error("'path' must be specified with files containing Bitcoin blockchain data."))
    val maxBlockSize = Integer.valueOf(
      parameters.getOrElse("maxBlockSize", String.valueOf(AbstractEthereumRecordReader.DEFAULT_MAXSIZE_ETHEREUMBLOCK)))
    val useDirectBuffer = parameters
      .getOrElse("useDirectBuffer", String.valueOf(AbstractEthereumRecordReader.DEFAULT_USEDIRECTBUFFER))
      .toBoolean
    val enrich = parameters.getOrElse("enrich", "false").toBoolean

    // parse the parameters into hadoop objects
    EthereumBlockRelation(path, maxBlockSize, useDirectBuffer, enrich)(sqlContext)
  }
}
