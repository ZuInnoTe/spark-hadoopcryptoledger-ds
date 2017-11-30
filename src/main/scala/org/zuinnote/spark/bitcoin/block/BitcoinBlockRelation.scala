/**
  * Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.spark.bitcoin.block

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf._
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, _}
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransaction, BitcoinUtil, BitcoinAuxPOW => HadoopBitcoinAuxPOW, BitcoinBlock => HadoopBitcoinBlock}
import org.zuinnote.hadoop.bitcoin.format.mapreduce._
import org.zuinnote.spark.bitcoin.model._
import org.zuinnote.spark.bitcoin.util.BitcoinBlockFile

import scala.collection.JavaConversions._

/**
  * Author: Jörn Franke <zuinnote@gmail.com>
  *
  * Defines the schema of a BitcoinBlock for Spark SQL
  */
case class BitcoinBlockRelation(location: String,
                                maxBlockSize: Integer = AbstractBitcoinRecordReader.DEFAULT_MAXSIZE_BITCOINBLOCK,
                                magic: String = AbstractBitcoinRecordReader.DEFAULT_MAGIC,
                                useDirectBuffer: Boolean = AbstractBitcoinRecordReader.DEFAULT_USEDIRECTBUFFER,
                                isSplittable: Boolean = AbstractBitcoinFileInputFormat.DEFAULT_ISSPLITABLE,
                                readAuxPOW: Boolean = AbstractBitcoinRecordReader.DEFAULT_READAUXPOW,
                                enrich: Boolean = false)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  private lazy val LOG = LogFactory.getLog(BitcoinBlockRelation.getClass)

  override def schema: StructType = {
    val structNoAuxPow = StructType(
      Seq(
        StructField("blockSize", IntegerType, nullable = false),
        StructField("magicNo", BinaryType, nullable = false),
        StructField("version", IntegerType, nullable = false),
        StructField("time", IntegerType, nullable = false),
        StructField("bits", BinaryType, nullable = false),
        StructField("nonce", IntegerType, nullable = false),
        StructField("transactionCounter", LongType, nullable = false),
        StructField("hashPrevBlock", BinaryType, nullable = false),
        StructField("hashMerkleRoot", BinaryType, nullable = false),
        StructField(
          "transactions",
          ArrayType(StructType(Seq(
            StructField("version", IntegerType, nullable = false),
            StructField("marker", ByteType, nullable = false),
            StructField("flag", ByteType, nullable = false),
            StructField("inCounter", BinaryType, nullable = false),
            StructField("outCounter", BinaryType, nullable = false),
            StructField(
              "listOfInputs",
              ArrayType(StructType(Seq(
                StructField("prevTransactionHash", BinaryType, nullable = false),
                StructField("previousTxOutIndex", LongType, nullable = false),
                StructField("txInScriptLength", BinaryType, nullable = false),
                StructField("txInScript", BinaryType, nullable = false),
                StructField("seqNo", LongType, nullable = false)
              ))),
              nullable = false
            ),
            StructField(
              "listOfOutputs",
              ArrayType(StructType(Seq(StructField("value", LongType, nullable = false),
                StructField("txOutScriptLength", BinaryType, nullable = false),
                StructField("txOutScript", BinaryType, nullable = false)))),
              nullable = false
            ),
            StructField(
              "listOfScriptWitnessItem",
              ArrayType(
                StructType(Seq(
                  StructField("stackItemCounter", BinaryType, nullable = false),
                  StructField(
                    "scriptWitnessList",
                    ArrayType(StructType(Seq(
                      StructField("witnessScriptLength", BinaryType, nullable = false),
                      StructField("witnessScript", BinaryType, nullable = false)
                    )),
                      containsNull = false)
                  )
                )),
                containsNull = false
              )
            ),
            StructField("lockTime", IntegerType, nullable = false)
          )))
        )
      ))

    val structNoAuxPowEnrich = StructType(
      Seq(
        StructField("blockSize", IntegerType, nullable = false),
        StructField("magicNo", BinaryType, nullable = false),
        StructField("version", IntegerType, nullable = false),
        StructField("time", IntegerType, nullable = false),
        StructField("bits", BinaryType, nullable = false),
        StructField("nonce", IntegerType, nullable = false),
        StructField("transactionCounter", LongType, nullable = false),
        StructField("hashPrevBlock", BinaryType, nullable = false),
        StructField("hashMerkleRoot", BinaryType, nullable = false),
        StructField(
          "transactions",
          ArrayType(StructType(Seq(
            StructField("version", IntegerType, nullable = false),
            StructField("marker", ByteType, nullable = false),
            StructField("flag", ByteType, nullable = false),
            StructField("inCounter", BinaryType, nullable = false),
            StructField("outCounter", BinaryType, nullable = false),
            StructField(
              "listOfInputs",
              ArrayType(StructType(Seq(
                StructField("prevTransactionHash", BinaryType, nullable = false),
                StructField("previousTxOutIndex", LongType, nullable = false),
                StructField("txInScriptLength", BinaryType, nullable = false),
                StructField("txInScript", BinaryType, nullable = false),
                StructField("seqNo", LongType, nullable = false)
              ))),
              nullable = false
            ),
            StructField(
              "listOfOutputs",
              ArrayType(StructType(Seq(StructField("value", LongType, nullable = false),
                StructField("txOutScriptLength", BinaryType, nullable = false),
                StructField("txOutScript", BinaryType, nullable = false)))),
              nullable = false
            ),
            StructField(
              "listOfScriptWitnessItem",
              ArrayType(
                StructType(Seq(
                  StructField("stackItemCounter", BinaryType, nullable = false),
                  StructField(
                    "scriptWitnessList",
                    ArrayType(StructType(Seq(
                      StructField("witnessScriptLength", BinaryType, nullable = false),
                      StructField("witnessScript", BinaryType, nullable = false)
                    )),
                      containsNull = false)
                  )
                )),
                containsNull = false
              )
            ),
            StructField("lockTime", IntegerType, nullable = false),
            StructField("currentTransactionHash", BinaryType, nullable = false)
          )))
        )
      ))

    val structAuxPow = StructType(
      Seq(
        StructField("blockSize", IntegerType, nullable = false),
        StructField("magicNo", BinaryType, nullable = false),
        StructField("version", IntegerType, nullable = false),
        StructField("time", IntegerType, nullable = false),
        StructField("bits", BinaryType, nullable = false),
        StructField("nonce", IntegerType, nullable = false),
        StructField("transactionCounter", LongType, nullable = false),
        StructField("hashPrevBlock", BinaryType, nullable = false),
        StructField("hashMerkleRoot", BinaryType, nullable = false),
        StructField(
          "transactions",
          ArrayType(StructType(Seq(
            StructField("version", IntegerType, nullable = false),
            StructField("marker", ByteType, nullable = false),
            StructField("flag", ByteType, nullable = false),
            StructField("inCounter", BinaryType, nullable = false),
            StructField("outCounter", BinaryType, nullable = false),
            StructField(
              "listOfInputs",
              ArrayType(StructType(Seq(
                StructField("prevTransactionHash", BinaryType, nullable = false),
                StructField("previousTxOutIndex", LongType, nullable = false),
                StructField("txInScriptLength", BinaryType, nullable = false),
                StructField("txInScript", BinaryType, nullable = false),
                StructField("seqNo", LongType, nullable = false)
              ))),
              nullable = false
            ),
            StructField(
              "listOfOutputs",
              ArrayType(StructType(Seq(StructField("value", LongType, nullable = false),
                StructField("txOutScriptLength", BinaryType, nullable = false),
                StructField("txOutScript", BinaryType, nullable = false)))),
              nullable = false
            ),
            StructField(
              "listOfScriptWitnessItem",
              ArrayType(
                StructType(Seq(
                  StructField("stackItemCounter", BinaryType, nullable = false),
                  StructField(
                    "scriptWitnessList",
                    ArrayType(StructType(Seq(
                      StructField("witnessScriptLength", BinaryType, nullable = false),
                      StructField("witnessScript", BinaryType, nullable = false)
                    )),
                      containsNull = false)
                  )
                )),
                containsNull = false
              )
            ),
            StructField("lockTime", IntegerType, nullable = false)
          )))
        ),
        StructField(
          "auxPOW",
          StructType(Seq(
            StructField("version", IntegerType, nullable = false),
            StructField(
              "coinbaseTransaction",
              StructType(Seq(
                StructField("version", IntegerType, nullable = false),
                StructField("inCounter", BinaryType, nullable = false),
                StructField("outCounter", BinaryType, nullable = false),
                StructField(
                  "listOfInputs",
                  ArrayType(StructType(Seq(
                    StructField("prevTransactionHash", BinaryType, nullable = false),
                    StructField("previousTxOutIndex", LongType, nullable = false),
                    StructField("txInScriptLength", BinaryType, nullable = false),
                    StructField("txInScript", BinaryType, nullable = false),
                    StructField("seqNo", LongType, nullable = false)
                  ))),
                  nullable = false
                ),
                StructField(
                  "listOfOutputs",
                  ArrayType(StructType(Seq(StructField("value", LongType, nullable = false),
                    StructField("txOutScriptLength", BinaryType, nullable = false),
                    StructField("txOutScript", BinaryType, nullable = false)))),
                  nullable = false
                ),
                StructField("lockTime", IntegerType, nullable = false)
              )),
              nullable = false
            ),
            StructField("parentBlockHeaderHash", BinaryType, nullable = false),
            StructField(
              "coinbaseBranch",
              StructType(
                Seq(
                  StructField("numberOfLinks", BinaryType, nullable = false),
                  StructField("links", ArrayType(BinaryType), nullable = false),
                  StructField("branchSideBitmask", BinaryType, nullable = false)
                )),
              nullable = false
            ),
            StructField(
              "auxBlockChainBranch",
              StructType(
                Seq(
                  StructField("numberOfLinks", BinaryType, nullable = false),
                  StructField("links", ArrayType(BinaryType), nullable = false),
                  StructField("branchSideBitmask", BinaryType, nullable = false)
                )),
              nullable = false
            ),
            StructField(
              "parentBlockHeader",
              StructType(Seq(
                StructField("version", IntegerType, nullable = false),
                StructField("previousBlockHash", BinaryType, nullable = false),
                StructField("merkleRoot", BinaryType, nullable = false),
                StructField("time", IntegerType, nullable = false),
                StructField("bits", BinaryType, nullable = false),
                StructField("nonce", IntegerType, nullable = false)
              )),
              nullable = false
            )
          ))
        )
      ))

    val structAuxPowEnrich = StructType(
      Seq(
        StructField("blockSize", IntegerType, nullable = false),
        StructField("magicNo", BinaryType, nullable = false),
        StructField("version", IntegerType, nullable = false),
        StructField("time", IntegerType, nullable = false),
        StructField("bits", BinaryType, nullable = false),
        StructField("nonce", IntegerType, nullable = false),
        StructField("transactionCounter", LongType, nullable = false),
        StructField("hashPrevBlock", BinaryType, nullable = false),
        StructField("hashMerkleRoot", BinaryType, nullable = false),
        StructField(
          "transactions",
          ArrayType(StructType(Seq(
            StructField("version", IntegerType, nullable = false),
            StructField("marker", ByteType, nullable = false),
            StructField("flag", ByteType, nullable = false),
            StructField("inCounter", BinaryType, nullable = false),
            StructField("outCounter", BinaryType, nullable = false),
            StructField(
              "listOfInputs",
              ArrayType(StructType(Seq(
                StructField("prevTransactionHash", BinaryType, nullable = false),
                StructField("previousTxOutIndex", LongType, nullable = false),
                StructField("txInScriptLength", BinaryType, nullable = false),
                StructField("txInScript", BinaryType, nullable = false),
                StructField("seqNo", LongType, nullable = false)
              ))),
              nullable = false
            ),
            StructField(
              "listOfOutputs",
              ArrayType(StructType(Seq(StructField("value", LongType, nullable = false),
                StructField("txOutScriptLength", BinaryType, nullable = false),
                StructField("txOutScript", BinaryType, nullable = false)))),
              nullable = false
            ),
            StructField(
              "listOfScriptWitnessItem",
              ArrayType(
                StructType(Seq(
                  StructField("stackItemCounter", BinaryType, nullable = false),
                  StructField(
                    "scriptWitnessList",
                    ArrayType(StructType(Seq(
                      StructField("witnessScriptLength", BinaryType, nullable = false),
                      StructField("witnessScript", BinaryType, nullable = false)
                    )),
                      containsNull = false)
                  )
                )),
                containsNull = false
              )
            ),
            StructField("lockTime", IntegerType, nullable = false),
            StructField("currentTransactionHash", BinaryType, nullable = false)
          )))
        ),
        StructField(
          "auxPOW",
          StructType(Seq(
            StructField("version", IntegerType, nullable = false),
            StructField(
              "coinbaseTransaction",
              StructType(Seq(
                StructField("version", IntegerType, nullable = false),
                StructField("inCounter", BinaryType, nullable = false),
                StructField("outCounter", BinaryType, nullable = false),
                StructField(
                  "listOfInputs",
                  ArrayType(StructType(Seq(
                    StructField("prevTransactionHash", BinaryType, nullable = false),
                    StructField("previousTxOutIndex", LongType, nullable = false),
                    StructField("txInScriptLength", BinaryType, nullable = false),
                    StructField("txInScript", BinaryType, nullable = false),
                    StructField("seqNo", LongType, nullable = false)
                  ))),
                  nullable = false
                ),
                StructField(
                  "listOfOutputs",
                  ArrayType(StructType(Seq(StructField("value", LongType, nullable = false),
                    StructField("txOutScriptLength", BinaryType, nullable = false),
                    StructField("txOutScript", BinaryType, nullable = false)))),
                  nullable = false
                ),
                StructField("lockTime", IntegerType, nullable = false)
              )),
              nullable = false
            ),
            StructField("parentBlockHeaderHash", BinaryType, nullable = false),
            StructField(
              "coinbaseBranch",
              StructType(
                Seq(
                  StructField("numberOfLinks", BinaryType, nullable = false),
                  StructField("links", ArrayType(BinaryType), nullable = false),
                  StructField("branchSideBitmask", BinaryType, nullable = false)
                )),
              nullable = false
            ),
            StructField(
              "auxBlockChainBranch",
              StructType(
                Seq(
                  StructField("numberOfLinks", BinaryType, nullable = false),
                  StructField("links", ArrayType(BinaryType), nullable = false),
                  StructField("branchSideBitmask", BinaryType, nullable = false)
                )),
              nullable = false
            ),
            StructField(
              "parentBlockHeader",
              StructType(Seq(
                StructField("version", IntegerType, nullable = false),
                StructField("previousBlockHash", BinaryType, nullable = false),
                StructField("merkleRoot", BinaryType, nullable = false),
                StructField("time", IntegerType, nullable = false),
                StructField("bits", BinaryType, nullable = false),
                StructField("nonce", IntegerType, nullable = false)
              )),
              nullable = false
            )
          ))
        )
      ))

    if (readAuxPOW) {
      if (enrich) {
        structAuxPowEnrich
      } else {
        structAuxPow
      }
    } else {
      if (enrich) {
        structNoAuxPowEnrich
      } else {
        structNoAuxPow
      }
    }
  }

  private def toTransaction(currentTransaction: BitcoinTransaction): Transaction = {
    val inputs: Seq[Input] = currentTransaction.getListOfInputs
      .map { currentTransactionInput =>
        Input(
          currentTransactionInput.getPrevTransactionHash,
          currentTransactionInput.getPreviousTxOutIndex,
          currentTransactionInput.getTxInScriptLength,
          currentTransactionInput.getTxInScript,
          currentTransactionInput.getSeqNo
        )
      }

    val outputs: Seq[Output] = currentTransaction.getListOfOutputs
      .map { currentTransactionOutput =>
        Output(
          currentTransactionOutput.getValue,
          currentTransactionOutput.getTxOutScriptLength,
          currentTransactionOutput.getTxOutScript
        )
      }

    val scriptWitnesses: Seq[ScriptWitnessItem] = currentTransaction.getBitcoinScriptWitness
      .map { currentTransactionScriptWitnessItem =>
        ScriptWitnessItem(
          currentTransactionScriptWitnessItem.getStackItemCounter,
          currentTransactionScriptWitnessItem.getScriptWitnessList
            .map { currentScriptWitness =>
              ScriptWitness(
                currentScriptWitness.getWitnessScriptLength,
                currentScriptWitness.getWitnessScript
              )
            }
        )
      }

    Transaction(
      currentTransaction.getVersion,
      currentTransaction.getMarker,
      currentTransaction.getFlag,
      currentTransaction.getInCounter,
      currentTransaction.getOutCounter,
      inputs,
      outputs,
      scriptWitnesses,
      currentTransaction.getLockTime
    )
  }

  private def toAuxPOW(auxPOW: HadoopBitcoinAuxPOW): AuxPOW = {
    val inputs: Seq[Input] = auxPOW.getCoinbaseTransaction.getListOfInputs
      .map { currentCoinBaseTransactionInput =>
        Input(
          currentCoinBaseTransactionInput.getPrevTransactionHash,
          currentCoinBaseTransactionInput.getPreviousTxOutIndex,
          currentCoinBaseTransactionInput.getTxInScriptLength,
          currentCoinBaseTransactionInput.getTxInScript,
          currentCoinBaseTransactionInput.getSeqNo
        )
      }

    val outputs: Seq[Output] = auxPOW.getCoinbaseTransaction.getListOfOutputs
      .map { currentCoinBaseTransactionOutput =>
        Output(
          currentCoinBaseTransactionOutput.getValue,
          currentCoinBaseTransactionOutput.getTxOutScriptLength,
          currentCoinBaseTransactionOutput.getTxOutScript
        )
      }

    val coinbaseTransaction = CoinbaseTransaction(
      auxPOW.getCoinbaseTransaction.getVersion,
      auxPOW.getCoinbaseTransaction.getInCounter,
      auxPOW.getCoinbaseTransaction.getOutCounter,
      inputs,
      outputs,
      auxPOW.getCoinbaseTransaction.getLockTime
    )

    val coinbaseBranch = CoinbaseBranch(
      auxPOW.getCoinbaseBranch.getNumberOfLinks,
      auxPOW.getCoinbaseBranch.getLinks.toSeq,
      auxPOW.getCoinbaseBranch.getBranchSideBitmask
    )

    val auxBlockChainBranch = AuxBlockChainBranch(
      auxPOW.getAuxBlockChainBranch.getNumberOfLinks,
      auxPOW.getAuxBlockChainBranch.getLinks.toSeq,
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

  /**
    * Used by Spark to fetch Bitcoin blocks according to the schema specified above from files.
    *
    *
    * returns BitcoinBlocks as rows
    **/
  override def buildScan: RDD[Row] = {
    val bitcoinBlockRDD: RDD[(BytesWritable, HadoopBitcoinBlock)] = readRawBlockRDD()

    bitcoinBlockRDD
      .map { case (_, currentBlock) =>
        val block = BitcoinBlock(
          currentBlock.getBlockSize,
          currentBlock.getMagicNo,
          currentBlock.getVersion,
          currentBlock.getTime,
          currentBlock.getBits,
          currentBlock.getNonce,
          currentBlock.getTransactionCounter,
          currentBlock.getHashPrevBlock,
          currentBlock.getHashMerkleRoot,
          Seq.empty
        )

        val blockWithTransactions: Product { def withAuxPOW(auxPOW: AuxPOW): Product } =
          if (enrich) {
            val transactions: Seq[EnrichedTransaction] = currentBlock.getTransactions
              .map(transaction => toTransaction(transaction).enriched(BitcoinUtil.getTransactionHash(transaction)))

            block.enriched(transactions)
          } else {
            block.copy(transactions = currentBlock.getTransactions.map(toTransaction))
          }

        if (readAuxPOW) {
          blockWithTransactions.withAuxPOW(toAuxPOW(currentBlock.getAuxPOW))
        } else {
          blockWithTransactions
        }
      }
      .map(Row.fromTuple)
  }

  private def readRawBlockRDD(): RDD[(BytesWritable, HadoopBitcoinBlock)] = {
    // create hadoopConf
    val hadoopConf = new Configuration()
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_MAXBLOCKSIZE, String.valueOf(maxBlockSize))
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_FILTERMAGIC, magic)
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_USEDIRECTBUFFER, String.valueOf(useDirectBuffer))
    hadoopConf.set(AbstractBitcoinFileInputFormat.CONF_ISSPLITABLE, String.valueOf(isSplittable))
    hadoopConf.set(AbstractBitcoinRecordReader.CONF_READAUXPOW, String.valueOf(readAuxPOW))
    // read BitcoinBlock
    BitcoinBlockFile.load(sqlContext, location, hadoopConf)
  }
}
