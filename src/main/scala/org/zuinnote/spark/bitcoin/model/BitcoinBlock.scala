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

package org.zuinnote.spark.bitcoin.model

final case class Input(prevTransactionHash: Seq[Byte], previousTxOutIndex: Long, txInScriptLength: Seq[Byte],
                       txInScript: Seq[Byte], seqNo: Long)

final case class Output(value: Long, txOutScriptLength: Seq[Byte], txOutScript: Seq[Byte])

final case class ScriptWitness(witnessScriptLength: Seq[Byte], witnessScript: Seq[Byte])

final case class ScriptWitnessItem(stackItemCounter: Seq[Byte], scriptWitnessList: Seq[ScriptWitness])

final case class Transaction(version: Int, marker: Byte, flag: Byte, inCounter: Seq[Byte], outCounter: Seq[Byte],
                             listOfInputs: Seq[Input], listOfOutputs: Seq[Output],
                             listOfScriptWitnessItem: Seq[ScriptWitnessItem], lockTime: Int) {

  private[bitcoin] def enriched(currentTransactionHash: Seq[Byte]): EnrichedTransaction = {
    EnrichedTransaction(
      version, marker, flag, inCounter, outCounter, listOfInputs, listOfOutputs, listOfScriptWitnessItem, lockTime,
      currentTransactionHash
    )
  }

  private[bitcoin] def single(transactionHash: Seq[Byte]): SingleTransaction = {
    SingleTransaction(
      transactionHash,
      version, marker, flag, inCounter, outCounter, listOfInputs, listOfOutputs, listOfScriptWitnessItem, lockTime
    )
  }
}

final case class SingleTransaction(currentTransactionHash: Seq[Byte], version: Int, marker: Byte, flag: Byte,
                                   inCounter: Seq[Byte], outCounter: Seq[Byte], listOfInputs: Seq[Input],
                                   listOfOutputs: Seq[Output], listOfScriptWitnessItem: Seq[ScriptWitnessItem],
                                   lockTime: Int)

final case class BitcoinBlock(blockSize: Int, magicNo: Seq[Byte], version: Int, time: Int, bits: Seq[Byte],
                              nonce: Int, transactionCounter: Long, hashPrevBlock: Seq[Byte],
                              hashMerkleRoot: Seq[Byte], transactions: Seq[Transaction])
  extends CanAddAuxPOW {

  private[bitcoin] def withAuxPOW(auxPOW: AuxPOW): BitcoinBlockWithAuxPOW = {
    BitcoinBlockWithAuxPOW(
      blockSize, magicNo, version, time, bits, nonce, transactionCounter, hashPrevBlock, hashMerkleRoot, transactions,
      auxPOW
    )
  }

  private[bitcoin] def enriched(transactions: Seq[EnrichedTransaction]): EnrichedBitcoinBlock = {
    EnrichedBitcoinBlock(
      blockSize, magicNo, version, time, bits, nonce, transactionCounter, hashPrevBlock, hashMerkleRoot, transactions
    )
  }
}

final case class BitcoinBlockWithAuxPOW(blockSize: Int, magicNo: Seq[Byte], version: Int, time: Int,
                                        bits: Seq[Byte], nonce: Int, transactionCounter: Long,
                                        hashPrevBlock: Seq[Byte], hashMerkleRoot: Seq[Byte],
                                        transactions: Seq[Transaction], auxPOW: AuxPOW)

final case class EnrichedTransaction(version: Int, marker: Byte, flag: Byte, inCounter: Seq[Byte],
                                     outCounter: Seq[Byte], listOfInputs: Seq[Input], listOfOutputs: Seq[Output],
                                     listOfScriptWitnessItem: Seq[ScriptWitnessItem], lockTime: Int,
                                     currentTransactionHash: Seq[Byte])

final case class EnrichedBitcoinBlock(blockSize: Int, magicNo: Seq[Byte], version: Int, time: Int, bits: Seq[Byte],
                                      nonce: Int, transactionCounter: Long, hashPrevBlock: Seq[Byte],
                                      hashMerkleRoot: Seq[Byte], transactions: Seq[EnrichedTransaction])
  extends CanAddAuxPOW {

  private[bitcoin] def withAuxPOW(auxPOW: AuxPOW): EnrichedBitcoinBlockWithAuxPOW = {
    EnrichedBitcoinBlockWithAuxPOW(
      blockSize, magicNo, version, time, bits, nonce, transactionCounter, hashPrevBlock, hashMerkleRoot, transactions,
      auxPOW
    )
  }
}

final case class EnrichedBitcoinBlockWithAuxPOW(blockSize: Int, magicNo: Seq[Byte], version: Int, time: Int,
                                                bits: Seq[Byte], nonce: Int, transactionCounter: Long,
                                                hashPrevBlock: Seq[Byte], hashMerkleRoot: Seq[Byte],
                                                transactions: Seq[EnrichedTransaction], auxPOW: AuxPOW)

final case class ParentBlockHeader(version: Int, previousBlockHash: Seq[Byte], merkleRoot: Seq[Byte], time: Int,
                                   bits: Seq[Byte], nonce: Int)

final case class CoinbaseTransaction(version: Int, inCounter: Seq[Byte], outCounter: Seq[Byte],
                                     listOfInputs: Seq[Input], listOfOutputs: Seq[Output], lockTime: Int)

final case class CoinbaseBranch(numberOfLinks: Seq[Byte], links: Seq[Seq[Byte]], branchSideBitmask: Seq[Byte])

final case class AuxBlockChainBranch(numberOfLinks: Seq[Byte], links: Seq[Seq[Byte]],
                                     branchSideBitmask: Seq[Byte])

final case class AuxPOW(version: Int, coinbaseTransaction: CoinbaseTransaction, parentBlockHeaderHash: Seq[Byte],
                        coinbaseBranch: CoinbaseBranch, auxBlockChainBranch: AuxBlockChainBranch,
                        parentBlockHeader: ParentBlockHeader)

sealed trait CanAddAuxPOW {
  private[bitcoin] def withAuxPOW(auxPOW: AuxPOW): Product
}
