package org.zuinnote.spark.bitcoin

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import org.zuinnote.spark._
import org.zuinnote.spark.bitcoin.TestData._
import org.zuinnote.spark.bitcoin.model._

class TransactionDatasetIntegrationTests extends FlatSpec with Matchers {
  "Genesis transaction" should "be parsed correctly" in {
    val fileName = "genesis.blk"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.bitcoin.transaction")
      .option("magic", "F9BEB4D9")
      .load(fileNameFullLocal)
      .as[SingleTransaction]

    val transaction = ds.collect()

    val expectedSingleTransaction = expectedGenesisBlock.transactions.head
      .single("3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a".bytes)

    transaction should contain theSameElementsInOrderAs Seq(expectedSingleTransaction)
  }

  "Script witness block" should "be parsed correctly" in {
    val fileName = "scriptwitness.blk"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.bitcoin.transaction")
      .option("magic", "F9BEB4D9")
      .load(fileNameFullLocal)
      .as[SingleTransaction]

    val transactionCount = ds.count()

    transactionCount should equal(expectedScriptWitness.transactionCounter)
  }

  "Script witness 2 block" should "be parsed correctly" in {
    val fileName = "scriptwitness2.blk"
    val fileNameFullLocal = getClass.getClassLoader.getResource(s"testdata/$fileName").getFile
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.read.format("org.zuinnote.spark.bitcoin.transaction")
      .option("magic", "F9BEB4D9")
      .load(fileNameFullLocal)
      .as[SingleTransaction]

    val transactionCount = ds.count()

    transactionCount should equal(expectedScriptWitness2Blocks.map(_.transactionCounter).sum)
  }
}