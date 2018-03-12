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

package org.zuinnote.spark.bitcoin.model
import java.math.BigInteger


final case class Input(prevTransactionHash: Array[Byte], previousTxOutIndex: Long, txInScriptLength: Array[Byte],
                       txInScript: Array[Byte], seqNo: Long)

final case class Output(value: BigInteger, txOutScriptLength: Array[Byte], txOutScript: Array[Byte])

final case class ScriptWitness(witnessScriptLength: Array[Byte], witnessScript: Array[Byte])

final case class ScriptWitnessItem(stackItemCounter: Array[Byte], scriptWitnessList: Seq[ScriptWitness])

final case class Transaction(version: Int, marker: Byte, flag: Byte, inCounter: Array[Byte], outCounter: Array[Byte],
                             listOfInputs: Seq[Input], listOfOutputs: Seq[Output],
                             listOfScriptWitnessItem: Seq[ScriptWitnessItem], lockTime: Int) {

  private[bitcoin] def enriched(currentTransactionHash: Array[Byte]): EnrichedTransaction = {
    EnrichedTransaction(
      version, marker, flag, inCounter, outCounter, listOfInputs, listOfOutputs, listOfScriptWitnessItem, lockTime,
      currentTransactionHash
    )
  }
}

final case class SingleTransaction(currentTransactionHash: Array[Byte], version: Int, marker: Byte, flag: Byte,
                                   inCounter: Array[Byte], outCounter: Array[Byte], listOfInputs: Seq[Input],
                                   listOfOutputs: Seq[Output], listOfScriptWitnessItem: Seq[ScriptWitnessItem],
                                   lockTime: Int)

final case class BitcoinBlock(blockSize: Int, magicNo: Array[Byte], version: Int, time: Int, bits: Array[Byte],
                              nonce: Int, transactionCounter: Long, hashPrevBlock: Array[Byte],
                              hashMerkleRoot: Array[Byte], transactions: Seq[Transaction])
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

final case class BitcoinBlockWithAuxPOW(blockSize: Int, magicNo: Array[Byte], version: Int, time: Int,
                                        bits: Array[Byte], nonce: Int, transactionCounter: Long,
                                        hashPrevBlock: Array[Byte], hashMerkleRoot: Array[Byte],
                                        transactions: Seq[Transaction], auxPOW: AuxPOW)

final case class EnrichedTransaction(version: Int, marker: Byte, flag: Byte, inCounter: Array[Byte],
                                     outCounter: Array[Byte], listOfInputs: Seq[Input], listOfOutputs: Seq[Output],
                                     listOfScriptWitnessItem: Seq[ScriptWitnessItem], lockTime: Int,
                                     currentTransactionHash: Array[Byte])

final case class EnrichedBitcoinBlock(blockSize: Int, magicNo: Array[Byte], version: Int, time: Int, bits: Array[Byte],
                                      nonce: Int, transactionCounter: Long, hashPrevBlock: Array[Byte],
                                      hashMerkleRoot: Array[Byte], transactions: Seq[EnrichedTransaction])
  extends CanAddAuxPOW {

  private[bitcoin] def withAuxPOW(auxPOW: AuxPOW): EnrichedBitcoinBlockWithAuxPOW = {
    EnrichedBitcoinBlockWithAuxPOW(
      blockSize, magicNo, version, time, bits, nonce, transactionCounter, hashPrevBlock, hashMerkleRoot, transactions,
      auxPOW
    )
  }
}

final case class EnrichedBitcoinBlockWithAuxPOW(blockSize: Int, magicNo: Array[Byte], version: Int, time: Int,
                                                bits: Array[Byte], nonce: Int, transactionCounter: Long,
                                                hashPrevBlock: Array[Byte], hashMerkleRoot: Array[Byte],
                                                transactions: Seq[EnrichedTransaction], auxPOW: AuxPOW)

final case class ParentBlockHeader(version: Int, previousBlockHash: Array[Byte], merkleRoot: Array[Byte], time: Int,
                                   bits: Array[Byte], nonce: Int)

final case class CoinbaseTransaction(version: Int, inCounter: Array[Byte], outCounter: Array[Byte],
                                     listOfInputs: Seq[Input], listOfOutputs: Seq[Output], lockTime: Int)

final case class CoinbaseBranch(numberOfLinks: Array[Byte], links: Seq[Array[Byte]], branchSideBitmask: Array[Byte])

final case class AuxBlockChainBranch(numberOfLinks: Array[Byte], links: Seq[Array[Byte]],
                                     branchSideBitmask: Array[Byte])

final case class AuxPOW(version: Int, coinbaseTransaction: CoinbaseTransaction, parentBlockHeaderHash: Array[Byte],
                        coinbaseBranch: CoinbaseBranch, auxBlockChainBranch: AuxBlockChainBranch,
                        parentBlockHeader: ParentBlockHeader)

sealed trait CanAddAuxPOW {
  private[bitcoin] def withAuxPOW(auxPOW: AuxPOW): Product
}
