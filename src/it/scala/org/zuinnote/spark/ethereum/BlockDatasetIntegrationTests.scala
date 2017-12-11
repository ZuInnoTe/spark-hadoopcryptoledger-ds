package org.zuinnote.spark.ethereum

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import org.zuinnote.spark.ethereum.model._

import TestData._

class BlockDatasetIntegrationTests extends FlatSpec with Matchers {
  "Block 1346406" should "be parsed correctly" in {
    val fileName = "eth1346406.bin"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.ethereum.block")
      .option("useDirectBuffer", "false")
      .load(fileNameFullLocal)
      .as[EthereumBlock]

    val block = ds.head()

    block should equal(block1346406)
  }

  "Block 1346406" should "be parsed correctly with enrichment" in {
    val fileName = "eth1346406.bin"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.ethereum.block")
      .option("useDirectBuffer", "false")
      .option("enrich", "true")
      .load(fileNameFullLocal)
      .as[EnrichedEthereumBlock]

    val block = ds.head()

    val transactions = block.ethereumTransactions
      .zip(block1346406EnrichedTransactionData)
      .map { case (transaction, enrichment) =>
          EnrichedEthereumTransaction(
            transaction.nonce, transaction.value, transaction.receiveAddress, transaction.gasPrice,
            transaction.gasLimit, transaction.data, transaction.sig_v, transaction.sig_r, transaction.sig_s,
            enrichment.sendAddress, enrichment.hash
          )
      }

    val expectedBlock = EnrichedEthereumBlock(
      block.ethereumBlockHeader,
      transactions,
      block.uncleHeaders
    )

    block should equal(expectedBlock)
  }
}