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
* Jörn Franke <zuinnote@gmail.com>
**/

package org.zuinnote.spark.ethereum.model
import java.math.BigInteger

final case class EthereumBlockHeader(parentHash: Array[Byte], uncleHash: Array[Byte], coinBase: Array[Byte],
                                     stateRoot: Array[Byte], txTrieRoot: Array[Byte], receiptTrieRoot: Array[Byte],
                                     logsBloom: Array[Byte], difficulty: Array[Byte], timestamp: Long, number: BigInteger, numberRaw: Array[Byte],
                                     gasLimit: BigInteger, gasLimitRaw: Array[Byte], gasUsed: BigInteger, gasUsedRaw: Array[Byte], mixHash: Array[Byte], extraData: Array[Byte],
                                     nonce: Array[Byte])

final case class EthereumTransaction(nonce: Array[Byte], value: BigInteger, valueRaw: Array[Byte], receiveAddress: Array[Byte], gasPrice: BigInteger, gasPriceRaw: Array[Byte],
                                     gasLimit: BigInteger,gasLimitRaw: Array[Byte], data: Array[Byte], sig_v: Array[Byte], sig_r: Array[Byte],
                                     sig_s: Array[Byte])

final case class EthereumBlock(ethereumBlockHeader: EthereumBlockHeader,
                               ethereumTransactions: Seq[EthereumTransaction],
                               uncleHeaders: Seq[EthereumBlockHeader])

final case class EnrichedEthereumTransaction(nonce: Array[Byte], value: BigInteger, valueRaw: Array[Byte], receiveAddress: Array[Byte], gasPrice: BigInteger, gasPriceRaw: Array[Byte],
                                     gasLimit: BigInteger,gasLimitRaw: Array[Byte], data: Array[Byte], sig_v: Array[Byte], sig_r: Array[Byte],
                                     sig_s: Array[Byte], sendAddress: Array[Byte],
                                             hash: Array[Byte])

final case class EnrichedEthereumBlock(ethereumBlockHeader: EthereumBlockHeader,
                                       ethereumTransactions: Seq[EnrichedEthereumTransaction],
                                       uncleHeaders: Seq[EthereumBlockHeader])
