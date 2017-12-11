package org.zuinnote.spark.bitcoin

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import org.zuinnote.spark.bitcoin.model._

import TestData._

class BlockDatasetIntegrationTests extends FlatSpec with Matchers {
  "Genesis block" should "be parsed correctly" in {
    val fileName = "genesis.blk"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.bitcoin.block")
      .option("magic", "F9BEB4D9")
      .load(fileNameFullLocal)
      .as[BitcoinBlock]

    val block = ds.head()

    block should equal(expectedGenesisBlock)
  }

  "Genesis block" should "be parsed correctly with enrichment" in {
    val fileName = "genesis.blk"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.bitcoin.block")
      .option("magic", "F9BEB4D9")
      .option("enrich", "true")
      .load(fileNameFullLocal)
      .as[EnrichedBitcoinBlock]

    val block = ds.head()

    block should equal(expectedEnrichedGenesisBlock)
  }

  "Namecoin block" should "be parsed correctly with auxiliary proof of work" in {
    val fileName = "namecointhreedifferentopinoneblock.blk"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.bitcoin.block")
      .option("magic", "F9BEB4FE")
      .option("readAuxPOW", "true")
      .load(fileNameFullLocal)
      .as[BitcoinBlockWithAuxPOW]

    val block = ds.head()

    block should equal(expectedNamecoinBlockWithAuxPOW)
  }

  "Namecoin block" should "be parsed correctly with enrichment and auxiliary proof of work" in {
    val fileName = "namecointhreedifferentopinoneblock.blk"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.bitcoin.block")
      .option("magic", "F9BEB4FE")
      .option("enrich", "true")
      .option("readAuxPOW", "true")
      .load(fileNameFullLocal)
      .as[EnrichedBitcoinBlockWithAuxPOW]

    val block = ds.head()

    block should equal(expectedEnrichedNamecoinBlockWithAuxPOW)
  }

  "Script witness block" should "be parsed correctly" in {
    val fileName = "scriptwitness.blk"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.bitcoin.block")
      .option("magic", "F9BEB4D9")
      .load(fileNameFullLocal)
      .as[BitcoinBlock]

    val block = ds.head()

    // This block contains 470 transactions, so we just check that the number is correct instead of checking them one
    // by one
    val actualBlockExceptTransactions = block.productIterator.toSeq.reverse.drop(1).reverse
    val expectedBlockExceptTransactions = expectedScriptWitness.productIterator.toSeq.reverse.drop(1).reverse

    actualBlockExceptTransactions should contain theSameElementsInOrderAs expectedBlockExceptTransactions
    block.transactions.size should equal(block.transactionCounter)
  }

  "Script witness 2 block" should "be parsed correctly" in {
    val fileName = "scriptwitness2.blk"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.bitcoin.block")
      .option("magic", "F9BEB4D9")
      .load(fileNameFullLocal)
      .as[BitcoinBlock]

    val blocks = ds.collect()

    // These blocks contains 4699 transactions, so we just check that the number is correct instead of checking them one
    // by one
    (blocks zip expectedScriptWitness2Blocks).foreach { case (actualBlock, expectedBlock) =>
      val actualBlockExceptTransactions = actualBlock.productIterator.toSeq.reverse.drop(1).reverse
      val expectedBlockExceptTransactions = expectedBlock.productIterator.toSeq.reverse.drop(1).reverse

      actualBlockExceptTransactions should contain theSameElementsInOrderAs expectedBlockExceptTransactions
      actualBlock.transactions.size should equal(actualBlock.transactionCounter)
    }
  }
}