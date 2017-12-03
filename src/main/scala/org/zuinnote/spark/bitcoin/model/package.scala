package org.zuinnote.spark.bitcoin

import org.zuinnote.hadoop.bitcoin.format.common._
import scala.collection.JavaConverters._

package object model {
  private def toInput(input: BitcoinTransactionInput): Input = {
    Input(
      input.getPrevTransactionHash, input.getPreviousTxOutIndex, input.getTxInScriptLength, input.getTxInScript,
      input.getSeqNo
    )
  }

  private def toOutput(output: BitcoinTransactionOutput): Output = {
    Output(output.getValue, output.getTxOutScriptLength, output.getTxOutScript)
  }

  implicit class FromJavaTransaction(val transaction: BitcoinTransaction) extends AnyVal {
    private def toScriptWitnessItem(item: BitcoinScriptWitnessItem): ScriptWitnessItem = {
      ScriptWitnessItem(item.getStackItemCounter, item.getScriptWitnessList.asScala.map(toScriptWitnessItem))
    }

    private def toScriptWitnessItem(sw: BitcoinScriptWitness): ScriptWitness = {
      ScriptWitness(sw.getWitnessScriptLength, sw.getWitnessScript)
    }

    def asScala: Transaction = {
      Transaction(
        transaction.getVersion,
        transaction.getMarker,
        transaction.getFlag,
        transaction.getInCounter,
        transaction.getOutCounter,
        transaction.getListOfInputs.asScala.map(toInput),
        transaction.getListOfOutputs.asScala.map(toOutput),
        transaction.getBitcoinScriptWitness.asScala.map(toScriptWitnessItem),
        transaction.getLockTime
      )
    }
    
    def asScalaEnriched: EnrichedTransaction = {
      EnrichedTransaction(
        transaction.getVersion,
        transaction.getMarker,
        transaction.getFlag,
        transaction.getInCounter,
        transaction.getOutCounter,
        transaction.getListOfInputs.asScala.map(toInput),
        transaction.getListOfOutputs.asScala.map(toOutput),
        transaction.getBitcoinScriptWitness.asScala.map(toScriptWitnessItem),
        transaction.getLockTime,
        BitcoinUtil.getTransactionHash(transaction)
      )
    }

    def asScalaSingle(transactionHash: Array[Byte]): SingleTransaction = {
      SingleTransaction(
        transactionHash,
        transaction.getVersion,
        transaction.getMarker,
        transaction.getFlag,
        transaction.getInCounter,
        transaction.getOutCounter,
        transaction.getListOfInputs.asScala.map(toInput),
        transaction.getListOfOutputs.asScala.map(toOutput),
        transaction.getBitcoinScriptWitness.asScala.map(toScriptWitnessItem),
        transaction.getLockTime
      )
    }
  }

  implicit class FromJavaAuxPOW(val auxPOW: BitcoinAuxPOW) extends AnyVal {
    def asScala: AuxPOW = {
      val coinbaseTransaction = CoinbaseTransaction(
        auxPOW.getCoinbaseTransaction.getVersion,
        auxPOW.getCoinbaseTransaction.getInCounter,
        auxPOW.getCoinbaseTransaction.getOutCounter,
        auxPOW.getCoinbaseTransaction.getListOfInputs.asScala.map(toInput),
        auxPOW.getCoinbaseTransaction.getListOfOutputs.asScala.map(toOutput),
        auxPOW.getCoinbaseTransaction.getLockTime
      )

      val coinbaseBranch = CoinbaseBranch(
        auxPOW.getCoinbaseBranch.getNumberOfLinks,
        auxPOW.getCoinbaseBranch.getLinks.asScala,
        auxPOW.getCoinbaseBranch.getBranchSideBitmask
      )

      val auxBlockChainBranch = AuxBlockChainBranch(
        auxPOW.getAuxBlockChainBranch.getNumberOfLinks,
        auxPOW.getAuxBlockChainBranch.getLinks.asScala,
        auxPOW.getCoinbaseBranch.getBranchSideBitmask
      )

      val parentBlockHeader = ParentBlockHeader(
        auxPOW.getParentBlockHeader.getVersion,
        auxPOW.getParentBlockHeader.getPreviousBlockHash,
        auxPOW.getParentBlockHeader.getMerkleRoot,
        auxPOW.getParentBlockHeader.getTime,
        auxPOW.getParentBlockHeader.getBits,
        auxPOW.getParentBlockHeader.getNonce
      )

      AuxPOW(
        auxPOW.getVersion,
        coinbaseTransaction,
        auxPOW.getParentBlockHeaderHash,
        coinbaseBranch,
        auxBlockChainBranch,
        parentBlockHeader
      )
    }
  }
}
