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

/**
* Author: Omer van Kloeten (https://github.com/omervk)
**/

package org.zuinnote.spark.ethereum

import org.zuinnote.hadoop.ethereum.format.common
import org.zuinnote.hadoop.ethereum.format.common.EthereumUtil

import scala.collection.JavaConverters._

package object model {
  private def toHeader(header: common.EthereumBlockHeader): EthereumBlockHeader = {
    EthereumBlockHeader(
      header.getParentHash, header.getUncleHash, header.getCoinBase, header.getStateRoot, header.getTxTrieRoot,
      header.getReceiptTrieRoot, header.getLogsBloom, header.getDifficulty, header.getTimestamp, header.getNumber,
      header.getGasLimit, header.getGasUsed, header.getMixHash, header.getExtraData, header.getNonce
    )
  }

  implicit class FromJavaTransaction(val transaction: common.EthereumTransaction) extends AnyVal {
    def asScala: EthereumTransaction = {
      EthereumTransaction(
        transaction.getNonce, transaction.getValue, transaction.getReceiveAddress, transaction.getGasPrice,
        transaction.getGasLimit, transaction.getData, transaction.getSig_v, transaction.getSig_r, transaction.getSig_s
      )
    }

    def asScalaEnriched: EnrichedEthereumTransaction = {
      EnrichedEthereumTransaction(
        transaction.getNonce, transaction.getValue, transaction.getReceiveAddress, transaction.getGasPrice,
        transaction.getGasLimit, transaction.getData, transaction.getSig_v, transaction.getSig_r, transaction.getSig_s,
        EthereumUtil.getSendAddress(transaction), EthereumUtil.getTransactionHash(transaction)
      )
    }
  }

  implicit class FromJavaBlock(val block: common.EthereumBlock) extends AnyVal {
    def asScala: EthereumBlock = {
      EthereumBlock(
        toHeader(block.getEthereumBlockHeader),
        block.getEthereumTransactions.asScala.map(_.asScala),
        block.getUncleHeaders.asScala.map(toHeader)
      )
    }

    def asScalaEnriched: EnrichedEthereumBlock = {
      EnrichedEthereumBlock(
        toHeader(block.getEthereumBlockHeader),
        block.getEthereumTransactions.asScala.map(_.asScalaEnriched),
        block.getUncleHeaders.asScala.map(toHeader)
      )
    }
  }
}
