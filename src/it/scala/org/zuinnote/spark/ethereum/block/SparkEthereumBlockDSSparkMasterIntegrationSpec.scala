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
  *
  * This test integrates HDFS and Spark
  *
  */

package org.zuinnote.spark.ethereum.block



import java.io.{File, IOException}
import java.nio.file.{FileVisitResult, Files, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes
import java.text.SimpleDateFormat


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.io.compress.{CodecPool,Decompressor
}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import org.zuinnote.spark.ethereum.model.{EnrichedEthereumBlock, EthereumBlock}

class SparkEthereumBlockDSSparkMasterIntegrationSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {

  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _
  private val master: String = "local[2]"
  private val appName: String = "spark-hadoocryptoledger-ds-integrationtest"
  private val tmpPrefix: String = "hcl-integrationtest"
  private var tmpPath: java.nio.file.Path = _
  private val CLUSTERNAME: String = "hcl-minicluster"
  private val DFS_INPUT_DIR_NAME: String = "/input"
  private val DFS_OUTPUT_DIR_NAME: String = "/output"
  private val DEFAULT_OUTPUT_FILENAME: String = "part-00000"
  private val DFS_INPUT_DIR: Path = new Path(DFS_INPUT_DIR_NAME)
  private val DFS_OUTPUT_DIR: Path = new Path(DFS_OUTPUT_DIR_NAME)
  private val NOOFDATANODES: Int = 4
  private var dfsCluster: MiniDFSCluster = _
  private var conf: Configuration = _
  private var openDecompressors = ArrayBuffer[Decompressor]()

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create temporary directory for HDFS base and shutdownhook
    // create temp directory
    tmpPath = Files.createTempDirectory(tmpPrefix)
    // create shutdown hook to remove temp files (=HDFS MiniCluster) after shutdown, may need to rethink to avoid many threads are created
    Runtime.getRuntime.addShutdownHook(new Thread("remove temporary directory") {
      override def run(): Unit = {
        try {
          Files.walkFileTree(tmpPath, new SimpleFileVisitor[java.nio.file.Path]() {

            override def visitFile(file: java.nio.file.Path, attrs: BasicFileAttributes): FileVisitResult = {
              Files.delete(file)
              FileVisitResult.CONTINUE
            }

            override def postVisitDirectory(dir: java.nio.file.Path, e: IOException): FileVisitResult = {
              if (e != null) {
                throw e
              }
              Files.delete(dir)
              FileVisitResult.CONTINUE
            }
          })
        } catch {
          case e: IOException => throw new RuntimeException("Error temporary files in following path could not be deleted " + tmpPath, e)
        }
      }
    })
    // create DFS mini cluster
    conf = new Configuration()
    val baseDir = new File(tmpPath.toString).getAbsoluteFile
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(conf)
    dfsCluster = builder.numDataNodes(NOOFDATANODES).build()
    conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri.toString)
    // create local Spark cluster
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    sc = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    // close Spark Context
    if (sc != null) {
      sc.stop()
    }
    // close decompressor
    for (currentDecompressor <- this.openDecompressors) {
      if (currentDecompressor != null) {
        CodecPool.returnDecompressor(currentDecompressor)
      }
    }
    // close dfs cluster
    dfsCluster.shutdown()
    super.afterAll()
  }


"The block 1346406 on DFS" should "be fully read in dataframe" in {
	Given("Block 1346406 on DFSCluster")
	// create input directory
   dfsCluster.getFileSystem().delete(DFS_INPUT_DIR,true)
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
	// copy bitcoin blocks
	val classLoader = getClass.getClassLoader
    	// put testdata on DFS
    	val fileName: String="eth1346406.bin"
    	val fileNameFullLocal=classLoader.getResource("testdata/"+fileName).getFile
    	val inputFile=new Path(fileNameFullLocal)
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)
	When("reading block 1346406 using datasource")
	val df = sqlContext.read.format("org.zuinnote.spark.ethereum.block").option("useDirectBuffer", "false").load(dfsCluster.getFileSystem().getUri.toString+DFS_INPUT_DIR_NAME)
	Then("all fields should be readable trough Spark SQL")
	// check first if structure is correct
	assert("ethereumBlockHeader"==df.columns(0))
	assert("ethereumTransactions"==df.columns(1))
	assert("uncleHeaders"==df.columns(2))

    val ethereumTransactionsDF = df.select(explode(df("ethereumTransactions")).alias("ethereumTransactions"))
    val ethereumUncleHeaderDF = df.select(explode(df("uncleHeaders")).alias("uncleHeaders"))
    // check if content is correct
    // sanity checks
    assert(6 == ethereumTransactionsDF.count())
    assert(0 == ethereumUncleHeaderDF.count())
    // check details
    // block header
    val expectedBhParentHash = Array(0xba.toByte, 0x6d.toByte, 0xd2.toByte, 0x60.toByte, 0x12.toByte, 0xb3.toByte, 0x71.toByte, 0x90.toByte, 0x48.toByte, 0xf3.toByte, 0x16.toByte, 0xc6.toByte, 0xed.toByte, 0xb3.toByte, 0x34.toByte, 0x9b.toByte, 0xdf.toByte, 0xbd.toByte, 0x61.toByte, 0x31.toByte, 0x9f.toByte, 0xa9.toByte, 0x7c.toByte, 0x61.toByte, 0x6a.toByte, 0x61.toByte, 0x31.toByte, 0x18.toByte, 0xa1.toByte, 0xaf.toByte, 0x30.toByte, 0x67.toByte)
    val bhParentHash = df.select("ethereumBlockHeader.parentHash").collect
    assert(expectedBhParentHash.deep == bhParentHash(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectdBhUncleHash = Array(0x1D.toByte, 0xCC.toByte, 0x4D.toByte, 0xE8.toByte, 0xDE.toByte, 0xC7.toByte, 0x5D.toByte, 0x7A.toByte, 0xAB.toByte, 0x85.toByte, 0xB5.toByte, 0x67.toByte, 0xB6.toByte, 0xCC.toByte, 0xD4.toByte, 0x1A.toByte, 0xD3.toByte, 0x12.toByte, 0x45.toByte, 0x1B.toByte, 0x94.toByte, 0x8A.toByte, 0x74.toByte, 0x13.toByte, 0xF0.toByte, 0xA1.toByte, 0x42.toByte, 0xFD.toByte, 0x40.toByte, 0xD4.toByte, 0x93.toByte, 0x47.toByte)
    val bhUncleHash = df.select("ethereumBlockHeader.uncleHash").collect
    assert(expectdBhUncleHash.deep == bhUncleHash(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhCoinBase = Array(0x1A.toByte, 0x06.toByte, 0x0B.toByte, 0x06.toByte, 0x04.toByte, 0x88.toByte, 0x3A.toByte, 0x99.toByte, 0x80.toByte, 0x9E.toByte, 0xB3.toByte, 0xF7.toByte, 0x98.toByte, 0xDF.toByte, 0x71.toByte, 0xBE.toByte, 0xF6.toByte, 0xC3.toByte, 0x58.toByte, 0xF1.toByte)
    val bhCoinBase = df.select("ethereumBlockHeader.coinBase").collect
    assert(expectedBhCoinBase.deep == bhCoinBase(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhStateRoot = Array(0x21.toByte, 0xBA.toByte, 0x88.toByte, 0x6F.toByte, 0xD2.toByte, 0x6F.toByte, 0x17.toByte, 0xB4.toByte, 0x01.toByte, 0xF5.toByte, 0x39.toByte, 0x20.toByte, 0x15.toByte, 0x33.toByte, 0x10.toByte, 0xB6.toByte, 0x93.toByte, 0x9B.toByte, 0xAD.toByte, 0x8A.toByte, 0x5F.toByte, 0xC3.toByte, 0xBF.toByte, 0x8C.toByte, 0x50.toByte, 0x5C.toByte, 0x55.toByte, 0x6D.toByte, 0xDB.toByte, 0xAF.toByte, 0xBC.toByte, 0x5C.toByte)
    val bhStateRoot = df.select("ethereumBlockHeader.stateRoot").collect
    assert(expectedBhStateRoot.deep == bhStateRoot(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhTxTrieRoot = Array(0xB3.toByte, 0xCB.toByte, 0xC7.toByte, 0xF0.toByte, 0xD7.toByte, 0x87.toByte, 0xE5.toByte, 0x7D.toByte, 0x93.toByte, 0x70.toByte, 0xB8.toByte, 0x02.toByte, 0xAB.toByte, 0x94.toByte, 0x5E.toByte, 0x21.toByte, 0x99.toByte, 0x1C.toByte, 0x3E.toByte, 0x12.toByte, 0x7D.toByte, 0x70.toByte, 0x12.toByte, 0x0C.toByte, 0x37.toByte, 0xE9.toByte, 0xFD.toByte, 0xAE.toByte, 0x3E.toByte, 0xF3.toByte, 0xEB.toByte, 0xFC.toByte)
    val bhTxTrieRoot = df.select("ethereumBlockHeader.txTrieRoot").collect
    assert(expectedBhTxTrieRoot.deep == bhTxTrieRoot(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhReceiptTrieRoot = Array(0x9B.toByte, 0xCE.toByte, 0x71.toByte, 0x32.toByte, 0xF5.toByte, 0x2D.toByte, 0x4D.toByte, 0x45.toByte, 0xA8.toByte, 0xA2.toByte, 0x47.toByte, 0x48.toByte, 0x47.toByte, 0x86.toByte, 0xC7.toByte, 0x0B.toByte, 0xB2.toByte, 0xE6.toByte, 0x39.toByte, 0x59.toByte, 0xC8.toByte, 0x56.toByte, 0x1B.toByte, 0x3A.toByte, 0xBF.toByte, 0xD4.toByte, 0xE7.toByte, 0x22.toByte, 0xE6.toByte, 0x00.toByte, 0x6A.toByte, 0x27.toByte)
    val bhReceiptTrieRoot = df.select("ethereumBlockHeader.receiptTrieRoot").collect
    assert(expectedBhReceiptTrieRoot.deep == bhReceiptTrieRoot(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhLogsBloom = Array(0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte)
    val bhLogsBloom = df.select("ethereumBlockHeader.logsBloom").collect
    assert(expectedBhLogsBloom.deep == bhLogsBloom(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhDifficulty = Array(0x19.toByte, 0xFF.toByte, 0x9E.toByte, 0xC4.toByte, 0x35.toByte, 0xE0.toByte)
    val bhDifficulty = df.select("ethereumBlockHeader.difficulty").collect
    assert(expectedBhDifficulty.deep == bhDifficulty(0).get(0).asInstanceOf[Array[Byte]].deep)
    val format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z")
    val expectedDTStr = "16-04-2016 09:34:29 UTC"
    val expectedBhTimestamp = format.parse(expectedDTStr).getTime / 1000
    val bhTimestamp = df.select("ethereumBlockHeader.timestamp").collect
    assert(expectedBhTimestamp == bhTimestamp(0).get(0))
    val expectedBhNumber = 1346406
    val bhNumber = df.select("ethereumBlockHeader.number").collect
    assert(expectedBhNumber == bhNumber(0).get(0))
    val expectedBhGasLimit = 4712388
    val bhGasLimit = df.select("ethereumBlockHeader.gasLimit").collect
    assert(expectedBhGasLimit == bhGasLimit(0).get(0))
    val expectedBhGasUsed = 126000
    val bhGasUsed = df.select("ethereumBlockHeader.gasUsed").collect
    assert(expectedBhGasUsed == bhGasUsed(0).get(0))
    val expectedBhMixHash = Array(0x4F.toByte, 0x57.toByte, 0x71.toByte, 0xB7.toByte, 0x9A.toByte, 0x8E.toByte, 0x6E.toByte, 0x21.toByte, 0x99.toByte, 0x35.toByte, 0x53.toByte, 0x9C.toByte, 0x47.toByte, 0x3E.toByte, 0x23.toByte, 0xBA.toByte, 0xFD.toByte, 0x2C.toByte, 0xA3.toByte, 0x5C.toByte, 0xC1.toByte, 0x86.toByte, 0x20.toByte, 0x66.toByte, 0x31.toByte, 0xC3.toByte, 0xB0.toByte, 0x9E.toByte, 0xD5.toByte, 0x76.toByte, 0x19.toByte, 0x4A.toByte)
    val bhMixHash = df.select("ethereumBlockHeader.mixHash").collect
    assert(expectedBhMixHash.deep == bhMixHash(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhExtraData = Array(0xD7.toByte, 0x83.toByte, 0x01.toByte, 0x03.toByte, 0x05.toByte, 0x84.toByte, 0x47.toByte, 0x65.toByte, 0x74.toByte, 0x68.toByte, 0x87.toByte, 0x67.toByte, 0x6F.toByte, 0x31.toByte, 0x2E.toByte, 0x35.toByte, 0x2E.toByte, 0x31.toByte, 0x85.toByte, 0x6C.toByte, 0x69.toByte, 0x6E.toByte, 0x75.toByte, 0x78.toByte)
    val bhExtraData = df.select("ethereumBlockHeader.extraData").collect
    assert(expectedBhExtraData.deep == bhExtraData(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhNonce = Array(0xFF.toByte, 0x7C.toByte, 0x7A.toByte, 0xEE.toByte, 0x0E.toByte, 0x88.toByte, 0xC5.toByte, 0x2D.toByte)
    val bhNonce = df.select("ethereumBlockHeader.nonce").collect
    assert(expectedBhNonce.deep == bhNonce(0).get(0).asInstanceOf[Array[Byte]].deep)
    // transactions
    val tnonces = ethereumTransactionsDF.select("ethereumTransactions.nonce").collect
    val tvalues = ethereumTransactionsDF.select("ethereumTransactions.value").collect
    val treceiveAddresses = ethereumTransactionsDF.select("ethereumTransactions.receiveAddress").collect
    val tgasPrices = ethereumTransactionsDF.select("ethereumTransactions.gasPrice").collect
    val tgasLimits = ethereumTransactionsDF.select("ethereumTransactions.gasLimit").collect
    val tdatas = ethereumTransactionsDF.select("ethereumTransactions.data").collect
    val tsigvs = ethereumTransactionsDF.select("ethereumTransactions.sig_v").collect
    val tsigrs = ethereumTransactionsDF.select("ethereumTransactions.sig_r").collect
    val tsigss = ethereumTransactionsDF.select("ethereumTransactions.sig_s").collect
    var transactNum = 0
    val expectedt1Nonce = Array(0x0C.toByte)
    assert(expectedt1Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1Value = 1069000990000000000L
    assert(expectedt1Value == tvalues(transactNum).get(0))
    val expectedt1ReceiveAddress = Array(0x1E.toByte, 0x75.toByte, 0xF0.toByte, 0x2A.toByte, 0x6E.toByte, 0x9F.toByte, 0xF4.toByte, 0xFF.toByte, 0x16.toByte, 0x33.toByte, 0x38.toByte, 0x25.toByte, 0xD9.toByte, 0x09.toByte, 0xBB.toByte, 0x03.toByte, 0x33.toByte, 0x06.toByte, 0xB7.toByte, 0x8B.toByte)
    assert(expectedt1ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1GasPrice = 20000000000L
    assert(expectedt1GasPrice == tgasPrices(transactNum).get(0))
    val expectedt1GasLimit = 21000
    assert(expectedt1GasLimit == tgasLimits(transactNum).get(0))
    val expectedt1Data: Array[Byte] = Array()
    assert(expectedt1Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1sigv = Array(0x1B.toByte)
    assert(expectedt1sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1sigr = Array(0x47.toByte, 0xDD.toByte, 0xF9.toByte, 0x37.toByte, 0x68.toByte, 0x97.toByte, 0x76.toByte, 0x78.toByte, 0x13.toByte, 0x95.toByte, 0x5A.toByte, 0x9D.toByte, 0x46.toByte, 0xB6.toByte, 0xF1.toByte, 0xAA.toByte, 0x77.toByte, 0x73.toByte, 0xE5.toByte, 0xC8.toByte, 0xC6.toByte, 0x21.toByte, 0x67.toByte, 0x54.toByte, 0x1C.toByte, 0x80.toByte, 0xBF.toByte, 0x25.toByte, 0x2D.toByte, 0xC7.toByte, 0xDC.toByte, 0xD2.toByte)
    assert(expectedt1sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1sigs = Array(0x0A.toByte, 0x31.toByte, 0x3F.toByte, 0x35.toByte, 0x60.toByte, 0x32.toByte, 0x37.toByte, 0x56.toByte, 0xB7.toByte, 0x28.toByte, 0x5F.toByte, 0x62.toByte, 0x38.toByte, 0x51.toByte, 0x86.toByte, 0x05.toByte, 0x82.toByte, 0x1A.toByte, 0x2B.toByte, 0xEE.toByte, 0x03.toByte, 0x7D.toByte, 0xEA.toByte, 0x8F.toByte, 0x09.toByte, 0x22.toByte, 0x66.toByte, 0x20.toByte, 0x89.toByte, 0x03.toByte, 0x74.toByte, 0x59.toByte)
    assert(expectedt1sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    transactNum = 1
    val expectedt2Nonce = Array(0xFF.toByte, 0xD7.toByte)
    assert(expectedt2Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2Value = 5110508700000000000L
    assert(expectedt2Value == tvalues(transactNum).get(0))
    val expectedt2ReceiveAddress = Array(0x54.toByte, 0x67.toByte, 0xFA.toByte, 0xBD.toByte, 0x30.toByte, 0xEB.toByte, 0x61.toByte, 0xA1.toByte, 0x84.toByte, 0x61.toByte, 0xD1.toByte, 0x53.toByte, 0xD8.toByte, 0xC6.toByte, 0xFF.toByte, 0xB1.toByte, 0x9D.toByte, 0xD4.toByte, 0x7A.toByte, 0x25.toByte)
    assert(expectedt2ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2GasPrice = 20000000000L
    assert(expectedt2GasPrice == tgasPrices(transactNum).get(0))
    val expectedt2GasLimit = 90000
    assert(expectedt2GasLimit == tgasLimits(transactNum).get(0))
    val expectedt2Data: Array[Byte] = Array()
    assert(expectedt2Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2sigv = Array(0x1B.toByte)
    assert(expectedt2sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2sigr = Array(0x62.toByte, 0x85.toByte, 0x3C.toByte, 0x63.toByte, 0x9A.toByte, 0x9B.toByte, 0x9E.toByte, 0xC4.toByte, 0x9B.toByte, 0xA9.toByte, 0xAC.toByte, 0x53.toByte, 0xE2.toByte, 0x85.toByte, 0xB3.toByte, 0x4E.toByte, 0xD0.toByte, 0xB7.toByte, 0x65.toByte, 0x5C.toByte, 0x1B.toByte, 0xE3.toByte, 0x29.toByte, 0xFB.toByte, 0x8B.toByte, 0x34.toByte, 0x70.toByte, 0x74.toByte, 0x0C.toByte, 0x3D.toByte, 0x0A.toByte, 0x9A.toByte)
    assert(expectedt2sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2sigs = Array(0x03.toByte, 0xFA.toByte, 0xA6.toByte, 0xF4.toByte, 0xFF.toByte, 0x1A.toByte, 0x45.toByte, 0x76.toByte, 0xDF.toByte, 0x08.toByte, 0x9A.toByte, 0x9F.toByte, 0x9C.toByte, 0xB7.toByte, 0x9C.toByte, 0xF2.toByte, 0xED.toByte, 0xC1.toByte, 0xC5.toByte, 0xBD.toByte, 0xEC.toByte, 0x0F.toByte, 0xE7.toByte, 0x9C.toByte, 0x79.toByte, 0x2A.toByte, 0xCB.toByte, 0x9E.toByte, 0x83.toByte, 0xF2.toByte, 0x41.toByte)
    assert(expectedt2sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    transactNum = 2
    val expectedt3Nonce = Array(0x02.toByte, 0xD7.toByte, 0xDD.toByte)
    assert(expectedt3Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3Value = 11667800000000000L
    assert(expectedt3Value == tvalues(transactNum).get(0))
    val expectedt3ReceiveAddress = Array(0xB4.toByte, 0xD0.toByte, 0xCA.toByte, 0x2B.toByte, 0x7E.toByte, 0x4C.toByte, 0xB1.toByte, 0xE0.toByte, 0x61.toByte, 0x0D.toByte, 0x02.toByte, 0x15.toByte, 0x4A.toByte, 0x10.toByte, 0x16.toByte, 0x3A.toByte, 0xB0.toByte, 0xF4.toByte, 0x2E.toByte, 0x65.toByte)
    assert(expectedt3ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3GasPrice = 20000000000L
    assert(expectedt3GasPrice == tgasPrices(transactNum).get(0))
    val expectedt3GasLimit = 90000
    assert(expectedt3GasLimit == tgasLimits(transactNum).get(0))
    val expectedt3Data: Array[Byte] = Array()
    assert(expectedt3Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3sigv = Array(0x1C.toByte)
    assert(expectedt3sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3sigr = Array(0x89.toByte, 0xFD.toByte, 0x7A.toByte, 0x62.toByte, 0xCF.toByte, 0x44.toByte, 0x77.toByte, 0xBF.toByte, 0xE5.toByte, 0xDB.toByte, 0xF0.toByte, 0xEE.toByte, 0xCF.toByte, 0x3A.toByte, 0x4A.toByte, 0x96.toByte, 0x71.toByte, 0x96.toByte, 0x96.toByte, 0xFB.toByte, 0xBE.toByte, 0x16.toByte, 0xBA.toByte, 0x0A.toByte, 0xBA.toByte, 0x1D.toByte, 0x63.toByte, 0x1D.toByte, 0x44.toByte, 0xC1.toByte, 0xEB.toByte, 0x58.toByte)
    assert(expectedt3sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3sigs = Array(0x24.toByte, 0x34.toByte, 0x48.toByte, 0x64.toByte, 0xEB.toByte, 0x6A.toByte, 0x60.toByte, 0xC6.toByte, 0x6F.toByte, 0xB5.toByte, 0xDA.toByte, 0xED.toByte, 0x02.toByte, 0xB5.toByte, 0x63.toByte, 0x52.toByte, 0xE8.toByte, 0x17.toByte, 0x42.toByte, 0x16.toByte, 0xB8.toByte, 0xA2.toByte, 0xD3.toByte, 0x33.toByte, 0xB7.toByte, 0xF3.toByte, 0x32.toByte, 0xFF.toByte, 0x6B.toByte, 0xA0.toByte, 0x69.toByte, 0x9C.toByte)
    assert(expectedt3sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    transactNum = 3
    val expectedt4Nonce = Array(0x02.toByte, 0xD7.toByte, 0xDE.toByte)
    assert(expectedt4Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4Value = 130970170000000000L
    assert(expectedt4Value == tvalues(transactNum).get(0))
    val expectedt4ReceiveAddress = Array(0x1F.toByte, 0x57.toByte, 0xF8.toByte, 0x26.toByte, 0xCA.toByte, 0xF5.toByte, 0x94.toByte, 0xF7.toByte, 0xA8.toByte, 0x37.toByte, 0xD9.toByte, 0xFC.toByte, 0x09.toByte, 0x24.toByte, 0x56.toByte, 0x87.toByte, 0x0A.toByte, 0x28.toByte, 0x93.toByte, 0x65.toByte)
    assert(expectedt4ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4GasPrice = 20000000000L
    assert(expectedt4GasPrice == tgasPrices(transactNum).get(0))
    val expectedt4GasLimit = 90000
    assert(expectedt4GasLimit == tgasLimits(transactNum).get(0))
    val expectedt4Data: Array[Byte] = Array()
    assert(expectedt4Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4sigv = Array(0x1B.toByte)
    assert(expectedt4sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4sigr = Array(0x46.toByte, 0x01.toByte, 0x57.toByte, 0xDC.toByte, 0xE4.toByte, 0xE9.toByte, 0x5D.toByte, 0x1D.toByte, 0xCC.toByte, 0x7A.toByte, 0xED.toByte, 0x0D.toByte, 0x9B.toByte, 0x7E.toByte, 0x3D.toByte, 0x65.toByte, 0x37.toByte, 0x0C.toByte, 0x53.toByte, 0xD2.toByte, 0x9E.toByte, 0xA9.toByte, 0xB1.toByte, 0xAA.toByte, 0x4C.toByte, 0x9C.toByte, 0x22.toByte, 0x14.toByte, 0x91.toByte, 0x1C.toByte, 0xD9.toByte, 0x5E.toByte)
    assert(expectedt4sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4sigs = Array(0x6A.toByte, 0x84.toByte, 0x4F.toByte, 0x95.toByte, 0x6D.toByte, 0x02.toByte, 0x46.toByte, 0x94.toByte, 0x1B.toByte, 0x94.toByte, 0x30.toByte, 0x91.toByte, 0x34.toByte, 0x21.toByte, 0x20.toByte, 0xBD.toByte, 0x48.toByte, 0xE7.toByte, 0xC6.toByte, 0x35.toByte, 0x77.toByte, 0xF0.toByte, 0xBA.toByte, 0x3D.toByte, 0x87.toByte, 0x59.toByte, 0xC9.toByte, 0xEC.toByte, 0x58.toByte, 0x70.toByte, 0x4E.toByte, 0xEC.toByte)
    assert(expectedt4sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    transactNum = 4
    val expectedt5Nonce = Array(0x02.toByte, 0xD7.toByte, 0xDF.toByte)
    assert(expectedt5Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5Value = 144683800000000000L
    assert(expectedt5Value == tvalues(transactNum).get(0))
    val expectedt5ReceiveAddress = Array(0x1F.toByte, 0x57.toByte, 0xF8.toByte, 0x26.toByte, 0xCA.toByte, 0xF5.toByte, 0x94.toByte, 0xF7.toByte, 0xA8.toByte, 0x37.toByte, 0xD9.toByte, 0xFC.toByte, 0x09.toByte, 0x24.toByte, 0x56.toByte, 0x87.toByte, 0x0A.toByte, 0x28.toByte, 0x93.toByte, 0x65.toByte)
    assert(expectedt5ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5GasPrice = 20000000000L
    assert(expectedt5GasPrice == tgasPrices(transactNum).get(0))
    val expectedt5GasLimit = 90000
    assert(expectedt5GasLimit == tgasLimits(transactNum).get(0))
    val expectedt5Data: Array[Byte] = Array()
    assert(expectedt5Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5sigv = Array(0x1C.toByte)
    assert(expectedt5sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5sigr = Array(0xE4.toByte, 0xBE.toByte, 0x97.toByte, 0xD5.toByte, 0xAF.toByte, 0xF1.toByte, 0xB5.toByte, 0xE7.toByte, 0x99.toByte, 0x12.toByte, 0x96.toByte, 0x98.toByte, 0x2B.toByte, 0xDF.toByte, 0xC1.toByte, 0xC2.toByte, 0x2F.toByte, 0x75.toByte, 0x21.toByte, 0x13.toByte, 0x4F.toByte, 0x7E.toByte, 0x1A.toByte, 0x9D.toByte, 0xA3.toByte, 0x00.toByte, 0x42.toByte, 0x0D.toByte, 0xAD.toByte, 0x33.toByte, 0x6F.toByte, 0x34.toByte)
    assert(expectedt5sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5sigs = Array(0x62.toByte, 0xDE.toByte, 0xF8.toByte, 0xAA.toByte, 0x83.toByte, 0x65.toByte, 0x58.toByte, 0xC7.toByte, 0xB0.toByte, 0xA5.toByte, 0x65.toByte, 0xB9.toByte, 0x7C.toByte, 0x9B.toByte, 0x27.toByte, 0xB2.toByte, 0x0E.toByte, 0xD9.toByte, 0xA0.toByte, 0x51.toByte, 0xDE.toByte, 0x22.toByte, 0xAD.toByte, 0x8D.toByte, 0xBD.toByte, 0x62.toByte, 0x52.toByte, 0x44.toByte, 0xCE.toByte, 0x64.toByte, 0x9E.toByte, 0x3D.toByte)
    assert(expectedt5sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    transactNum = 5
    val expectedt6Nonce = Array(0x02.toByte, 0xD7.toByte, 0xE0.toByte)
    assert(expectedt6Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6Value = 143694920000000000L
    assert(expectedt6Value == tvalues(transactNum).get(0))
    val expectedt6ReceiveAddress = Array(0x1F.toByte, 0x57.toByte, 0xF8.toByte, 0x26.toByte, 0xCA.toByte, 0xF5.toByte, 0x94.toByte, 0xF7.toByte, 0xA8.toByte, 0x37.toByte, 0xD9.toByte, 0xFC.toByte, 0x09.toByte, 0x24.toByte, 0x56.toByte, 0x87.toByte, 0x0A.toByte, 0x28.toByte, 0x93.toByte, 0x65.toByte)
    assert(expectedt6ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6GasPrice = 20000000000L
    assert(expectedt6GasPrice == tgasPrices(transactNum).get(0))
    val expectedt6GasLimit = 90000
    assert(expectedt6GasLimit == tgasLimits(transactNum).get(0))
    val expectedt6Data: Array[Byte] = Array()
    assert(expectedt6Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6sigv = Array(0x1C.toByte)
    assert(expectedt6sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6sigr = Array(0x0A.toByte, 0x4C.toByte, 0xA2.toByte, 0x18.toByte, 0x46.toByte, 0x0D.toByte, 0xC8.toByte, 0x5B.toByte, 0x99.toByte, 0x07.toByte, 0x46.toByte, 0xFB.toByte, 0xB9.toByte, 0x0C.toByte, 0x06.toByte, 0xF8.toByte, 0x25.toByte, 0x87.toByte, 0x82.toByte, 0x80.toByte, 0x87.toByte, 0x27.toByte, 0x98.toByte, 0x3C.toByte, 0x8B.toByte, 0x8D.toByte, 0x6A.toByte, 0x92.toByte, 0x1E.toByte, 0x19.toByte, 0x9B.toByte, 0xCA.toByte)
    assert(expectedt6sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6sigs = Array(0x08.toByte, 0xA3.toByte, 0xB9.toByte, 0xA4.toByte, 0x5D.toByte, 0x83.toByte, 0x1A.toByte, 0xC4.toByte, 0xAD.toByte, 0x37.toByte, 0x9D.toByte, 0x14.toByte, 0xF0.toByte, 0xAE.toByte, 0x3C.toByte, 0x03.toByte, 0xC8.toByte, 0x73.toByte, 0x1C.toByte, 0xB4.toByte, 0x4D.toByte, 0x8A.toByte, 0x79.toByte, 0xAC.toByte, 0xD4.toByte, 0xCD.toByte, 0x6C.toByte, 0xEA.toByte, 0x1B.toByte, 0x54.toByte, 0x80.toByte, 0x02.toByte)
    assert(expectedt6sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)

    // uncle headers
    // block does not contain uncleheaders
}

"The block 1346406 on DFS" should "be fully read in dataframe (with rich data types)" in {
	Given("Block 1346406 on DFSCluster")
	// create input directory
   dfsCluster.getFileSystem().delete(DFS_INPUT_DIR,true)
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
	// copy bitcoin blocks
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="eth1346406.bin"
    	val fileNameFullLocal=classLoader.getResource("testdata/"+fileName).getFile()
    	val inputFile=new Path(fileNameFullLocal)
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)
	When("reading block 1346406 using datasource")
	val df = sqlContext.read.format("org.zuinnote.spark.ethereum.block").option("useDirectBuffer", "false").load(dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME)
	Then("all fields should be readable trough Spark SQL")
	// check first if structure is correct
	assert("ethereumBlockHeader"==df.columns(0))
	assert("ethereumTransactions"==df.columns(1))
	assert("uncleHeaders"==df.columns(2))

  val ethereumTransactionsDF = df.select(explode(df("ethereumTransactions")).alias("ethereumTransactions"))
    val ethereumUncleHeaderDF = df.select(explode(df("uncleHeaders")).alias("uncleHeaders"))
  // check if content is correct
  // sanity checks
  assert(6==ethereumTransactionsDF.count())
  assert(0==ethereumUncleHeaderDF.count())
  // check details
  // block header
  val expectedBhParentHash = Array(0xba.toByte,0x6d.toByte,0xd2.toByte,0x60.toByte,0x12.toByte,0xb3.toByte,0x71.toByte,0x90.toByte,0x48.toByte,0xf3.toByte,0x16.toByte,0xc6.toByte,0xed.toByte,0xb3.toByte,0x34.toByte,0x9b.toByte,0xdf.toByte,0xbd.toByte,0x61.toByte,0x31.toByte,0x9f.toByte,0xa9.toByte,0x7c.toByte,0x61.toByte,0x6a.toByte,0x61.toByte,0x31.toByte,0x18.toByte,0xa1.toByte,0xaf.toByte,0x30.toByte,0x67.toByte)
  val bhParentHash = df.select("ethereumBlockHeader.parentHash").collect
  assert(expectedBhParentHash.deep==bhParentHash(0).get(0).asInstanceOf[Array[Byte]].deep)
  val expectdBhUncleHash = Array(0x1D.toByte,0xCC.toByte,0x4D.toByte,0xE8.toByte,0xDE.toByte,0xC7.toByte,0x5D.toByte,0x7A.toByte,0xAB.toByte,0x85.toByte,0xB5.toByte,0x67.toByte,0xB6.toByte,0xCC.toByte,0xD4.toByte,0x1A.toByte,0xD3.toByte,0x12.toByte,0x45.toByte,0x1B.toByte,0x94.toByte,0x8A.toByte,0x74.toByte,0x13.toByte,0xF0.toByte,0xA1.toByte,0x42.toByte,0xFD.toByte,0x40.toByte,0xD4.toByte,0x93.toByte,0x47.toByte)
  val bhUncleHash = df.select("ethereumBlockHeader.uncleHash").collect
  assert(expectdBhUncleHash.deep==bhUncleHash(0).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedBhCoinBase = Array(0x1A.toByte,0x06.toByte,0x0B.toByte,0x06.toByte,0x04.toByte,0x88.toByte,0x3A.toByte,0x99.toByte,0x80.toByte,0x9E.toByte,0xB3.toByte,0xF7.toByte,0x98.toByte,0xDF.toByte,0x71.toByte,0xBE.toByte,0xF6.toByte,0xC3.toByte,0x58.toByte,0xF1.toByte)
  val bhCoinBase = df.select("ethereumBlockHeader.coinBase").collect
  assert(expectedBhCoinBase.deep==bhCoinBase(0).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedBhStateRoot = Array(0x21.toByte,0xBA.toByte,0x88.toByte,0x6F.toByte,0xD2.toByte,0x6F.toByte,0x17.toByte,0xB4.toByte,0x01.toByte,0xF5.toByte,0x39.toByte,0x20.toByte,0x15.toByte,0x33.toByte,0x10.toByte,0xB6.toByte,0x93.toByte,0x9B.toByte,0xAD.toByte,0x8A.toByte,0x5F.toByte,0xC3.toByte,0xBF.toByte,0x8C.toByte,0x50.toByte,0x5C.toByte,0x55.toByte,0x6D.toByte,0xDB.toByte,0xAF.toByte,0xBC.toByte,0x5C.toByte)
  val bhStateRoot = df.select("ethereumBlockHeader.stateRoot").collect
  assert(expectedBhStateRoot.deep==bhStateRoot(0).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedBhTxTrieRoot = Array(0xB3.toByte,0xCB.toByte,0xC7.toByte,0xF0.toByte,0xD7.toByte,0x87.toByte,0xE5.toByte,0x7D.toByte,0x93.toByte,0x70.toByte,0xB8.toByte,0x02.toByte,0xAB.toByte,0x94.toByte,0x5E.toByte,0x21.toByte,0x99.toByte,0x1C.toByte,0x3E.toByte,0x12.toByte,0x7D.toByte,0x70.toByte,0x12.toByte,0x0C.toByte,0x37.toByte,0xE9.toByte,0xFD.toByte,0xAE.toByte,0x3E.toByte,0xF3.toByte,0xEB.toByte,0xFC.toByte)
  val bhTxTrieRoot = df.select("ethereumBlockHeader.txTrieRoot").collect
  assert(expectedBhTxTrieRoot.deep==bhTxTrieRoot(0).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedBhReceiptTrieRoot = Array(0x9B.toByte,0xCE.toByte,0x71.toByte,0x32.toByte,0xF5.toByte,0x2D.toByte,0x4D.toByte,0x45.toByte,0xA8.toByte,0xA2.toByte,0x47.toByte,0x48.toByte,0x47.toByte,0x86.toByte,0xC7.toByte,0x0B.toByte,0xB2.toByte,0xE6.toByte,0x39.toByte,0x59.toByte,0xC8.toByte,0x56.toByte,0x1B.toByte,0x3A.toByte,0xBF.toByte,0xD4.toByte,0xE7.toByte,0x22.toByte,0xE6.toByte,0x00.toByte,0x6A.toByte,0x27.toByte)
  val bhReceiptTrieRoot = df.select("ethereumBlockHeader.receiptTrieRoot").collect
  assert(expectedBhReceiptTrieRoot.deep==bhReceiptTrieRoot(0).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedBhLogsBloom = Array(0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte)
  val bhLogsBloom = df.select("ethereumBlockHeader.logsBloom").collect
  assert(expectedBhLogsBloom.deep==bhLogsBloom(0).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedBhDifficulty = Array(0x19.toByte,0xFF.toByte,0x9E.toByte,0xC4.toByte,0x35.toByte,0xE0.toByte)
  val bhDifficulty = df.select("ethereumBlockHeader.difficulty").collect
  assert(expectedBhDifficulty.deep==bhDifficulty(0).get(0).asInstanceOf[Array[Byte]].deep)
  val format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
	val expectedDTStr = "16-04-2016 09:34:29 UTC";
  val expectedBhTimestamp = format.parse(expectedDTStr).getTime() / 1000;
  val bhTimestamp = df.select("ethereumBlockHeader.timestamp").collect
  assert(expectedBhTimestamp==bhTimestamp(0).get(0))
  val expectedBhNumber = 1346406
  val bhNumber = df.select("ethereumBlockHeader.number").collect
  assert(expectedBhNumber==bhNumber(0).get(0))
  val expectedBhGasLimit = 4712388
  val bhGasLimit = df.select("ethereumBlockHeader.gasLimit").collect
  assert(expectedBhGasLimit==bhGasLimit(0).get(0))
  val expectedBhGasUsed = 126000
  val bhGasUsed = df.select("ethereumBlockHeader.gasUsed").collect
  assert(expectedBhGasUsed==bhGasUsed(0).get(0))
  val expectedBhMixHash = Array(0x4F.toByte,0x57.toByte,0x71.toByte,0xB7.toByte,0x9A.toByte,0x8E.toByte,0x6E.toByte,0x21.toByte,0x99.toByte,0x35.toByte,0x53.toByte,0x9C.toByte,0x47.toByte,0x3E.toByte,0x23.toByte,0xBA.toByte,0xFD.toByte,0x2C.toByte,0xA3.toByte,0x5C.toByte,0xC1.toByte,0x86.toByte,0x20.toByte,0x66.toByte,0x31.toByte,0xC3.toByte,0xB0.toByte,0x9E.toByte,0xD5.toByte,0x76.toByte,0x19.toByte,0x4A.toByte)
  val bhMixHash = df.select("ethereumBlockHeader.mixHash").collect
  assert(expectedBhMixHash.deep==bhMixHash(0).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedBhExtraData = Array(0xD7.toByte,0x83.toByte,0x01.toByte,0x03.toByte,0x05.toByte,0x84.toByte,0x47.toByte,0x65.toByte,0x74.toByte,0x68.toByte,0x87.toByte,0x67.toByte,0x6F.toByte,0x31.toByte,0x2E.toByte,0x35.toByte,0x2E.toByte,0x31.toByte,0x85.toByte,0x6C.toByte,0x69.toByte,0x6E.toByte,0x75.toByte,0x78.toByte)
  val bhExtraData = df.select("ethereumBlockHeader.extraData").collect
    assert(expectedBhExtraData.deep==bhExtraData(0).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedBhNonce = Array(0xFF.toByte,0x7C.toByte,0x7A.toByte,0xEE.toByte,0x0E.toByte,0x88.toByte,0xC5.toByte,0x2D.toByte)
  val bhNonce = df.select("ethereumBlockHeader.nonce").collect
    assert(expectedBhNonce.deep==bhNonce(0).get(0).asInstanceOf[Array[Byte]].deep)
  // transactions
  val tnonces = ethereumTransactionsDF.select("ethereumTransactions.nonce").collect
  val tvalues = ethereumTransactionsDF.select("ethereumTransactions.value").collect
  val treceiveAddresses = ethereumTransactionsDF.select("ethereumTransactions.receiveAddress").collect
  val tgasPrices = ethereumTransactionsDF.select("ethereumTransactions.gasPrice").collect
  val tgasLimits = ethereumTransactionsDF.select("ethereumTransactions.gasLimit").collect
  val tdatas = ethereumTransactionsDF.select("ethereumTransactions.data").collect
  val tsigvs = ethereumTransactionsDF.select("ethereumTransactions.sig_v").collect
  val tsigrs = ethereumTransactionsDF.select("ethereumTransactions.sig_r").collect
  val tsigss = ethereumTransactionsDF.select("ethereumTransactions.sig_s").collect
  var transactNum=0
  val expectedt1Nonce = Array(0x0C.toByte)
  assert(expectedt1Nonce.deep==tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt1Value = 1069000990000000000L
  assert(expectedt1Value==tvalues(transactNum).get(0))
  val expectedt1ReceiveAddress= Array(0x1E.toByte,0x75.toByte,0xF0.toByte,0x2A.toByte,0x6E.toByte,0x9F.toByte,0xF4.toByte,0xFF.toByte,0x16.toByte,0x33.toByte,0x38.toByte,0x25.toByte,0xD9.toByte,0x09.toByte,0xBB.toByte,0x03.toByte,0x33.toByte,0x06.toByte,0xB7.toByte,0x8B.toByte)
  assert(expectedt1ReceiveAddress.deep==treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt1GasPrice=20000000000L
  assert(expectedt1GasPrice==tgasPrices(transactNum).get(0))
  val expectedt1GasLimit=21000
  assert(expectedt1GasLimit==tgasLimits(transactNum).get(0))
  val expectedt1Data : Array[Byte] = Array()
  assert(expectedt1Data.deep==tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt1sigv = Array(0x1B.toByte)
  assert(expectedt1sigv.deep==tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt1sigr = Array(0x47.toByte,0xDD.toByte,0xF9.toByte,0x37.toByte,0x68.toByte,0x97.toByte,0x76.toByte,0x78.toByte,0x13.toByte,0x95.toByte,0x5A.toByte,0x9D.toByte,0x46.toByte,0xB6.toByte,0xF1.toByte,0xAA.toByte,0x77.toByte,0x73.toByte,0xE5.toByte,0xC8.toByte,0xC6.toByte,0x21.toByte,0x67.toByte,0x54.toByte,0x1C.toByte,0x80.toByte,0xBF.toByte,0x25.toByte,0x2D.toByte,0xC7.toByte,0xDC.toByte,0xD2.toByte)
  assert(expectedt1sigr.deep==tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt1sigs = Array(0x0A.toByte,0x31.toByte,0x3F.toByte,0x35.toByte,0x60.toByte,0x32.toByte,0x37.toByte,0x56.toByte,0xB7.toByte,0x28.toByte,0x5F.toByte,0x62.toByte,0x38.toByte,0x51.toByte,0x86.toByte,0x05.toByte,0x82.toByte,0x1A.toByte,0x2B.toByte,0xEE.toByte,0x03.toByte,0x7D.toByte,0xEA.toByte,0x8F.toByte,0x09.toByte,0x22.toByte,0x66.toByte,0x20.toByte,0x89.toByte,0x03.toByte,0x74.toByte,0x59.toByte)
  assert(expectedt1sigs.deep==tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  transactNum=1
  val expectedt2Nonce = Array(0xFF.toByte,0xD7.toByte)
  assert(expectedt2Nonce.deep==tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt2Value = 5110508700000000000L
  assert(expectedt2Value==tvalues(transactNum).get(0))
  val expectedt2ReceiveAddress= Array(0x54.toByte,0x67.toByte,0xFA.toByte,0xBD.toByte,0x30.toByte,0xEB.toByte,0x61.toByte,0xA1.toByte,0x84.toByte,0x61.toByte,0xD1.toByte,0x53.toByte,0xD8.toByte,0xC6.toByte,0xFF.toByte,0xB1.toByte,0x9D.toByte,0xD4.toByte,0x7A.toByte,0x25.toByte)
  assert(expectedt2ReceiveAddress.deep==treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt2GasPrice=20000000000L
  assert(expectedt2GasPrice==tgasPrices(transactNum).get(0))
  val expectedt2GasLimit=90000
  assert(expectedt2GasLimit==tgasLimits(transactNum).get(0))
  val expectedt2Data : Array[Byte] = Array()
  assert(expectedt2Data.deep==tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt2sigv = Array(0x1B.toByte)
  assert(expectedt2sigv.deep==tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt2sigr = Array(0x62.toByte,0x85.toByte,0x3C.toByte,0x63.toByte,0x9A.toByte,0x9B.toByte,0x9E.toByte,0xC4.toByte,0x9B.toByte,0xA9.toByte,0xAC.toByte,0x53.toByte,0xE2.toByte,0x85.toByte,0xB3.toByte,0x4E.toByte,0xD0.toByte,0xB7.toByte,0x65.toByte,0x5C.toByte,0x1B.toByte,0xE3.toByte,0x29.toByte,0xFB.toByte,0x8B.toByte,0x34.toByte,0x70.toByte,0x74.toByte,0x0C.toByte,0x3D.toByte,0x0A.toByte,0x9A.toByte)
   assert(expectedt2sigr.deep==tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt2sigs =  Array(0x03.toByte,0xFA.toByte,0xA6.toByte,0xF4.toByte,0xFF.toByte,0x1A.toByte,0x45.toByte,0x76.toByte,0xDF.toByte,0x08.toByte,0x9A.toByte,0x9F.toByte,0x9C.toByte,0xB7.toByte,0x9C.toByte,0xF2.toByte,0xED.toByte,0xC1.toByte,0xC5.toByte,0xBD.toByte,0xEC.toByte,0x0F.toByte,0xE7.toByte,0x9C.toByte,0x79.toByte,0x2A.toByte,0xCB.toByte,0x9E.toByte,0x83.toByte,0xF2.toByte,0x41.toByte)
  assert(expectedt2sigs.deep==tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  transactNum=2
  val expectedt3Nonce = Array(0x02.toByte,0xD7.toByte,0xDD.toByte)
  assert(expectedt3Nonce.deep==tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt3Value = 11667800000000000L
  assert(expectedt3Value==tvalues(transactNum).get(0))
  val expectedt3ReceiveAddress= Array(0xB4.toByte,0xD0.toByte,0xCA.toByte,0x2B.toByte,0x7E.toByte,0x4C.toByte,0xB1.toByte,0xE0.toByte,0x61.toByte,0x0D.toByte,0x02.toByte,0x15.toByte,0x4A.toByte,0x10.toByte,0x16.toByte,0x3A.toByte,0xB0.toByte,0xF4.toByte,0x2E.toByte,0x65.toByte)
     assert(expectedt3ReceiveAddress.deep==treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt3GasPrice=20000000000L
  assert(expectedt3GasPrice==tgasPrices(transactNum).get(0))
  val expectedt3GasLimit=90000
  assert(expectedt3GasLimit==tgasLimits(transactNum).get(0))
  val expectedt3Data : Array[Byte] = Array()
  assert(expectedt3Data.deep==tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt3sigv = Array(0x1C.toByte)
  assert(expectedt3sigv.deep==tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt3sigr = Array(0x89.toByte,0xFD.toByte,0x7A.toByte,0x62.toByte,0xCF.toByte,0x44.toByte,0x77.toByte,0xBF.toByte,0xE5.toByte,0xDB.toByte,0xF0.toByte,0xEE.toByte,0xCF.toByte,0x3A.toByte,0x4A.toByte,0x96.toByte,0x71.toByte,0x96.toByte,0x96.toByte,0xFB.toByte,0xBE.toByte,0x16.toByte,0xBA.toByte,0x0A.toByte,0xBA.toByte,0x1D.toByte,0x63.toByte,0x1D.toByte,0x44.toByte,0xC1.toByte,0xEB.toByte,0x58.toByte)
   assert(expectedt3sigr.deep==tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt3sigs =  Array(0x24.toByte,0x34.toByte,0x48.toByte,0x64.toByte,0xEB.toByte,0x6A.toByte,0x60.toByte,0xC6.toByte,0x6F.toByte,0xB5.toByte,0xDA.toByte,0xED.toByte,0x02.toByte,0xB5.toByte,0x63.toByte,0x52.toByte,0xE8.toByte,0x17.toByte,0x42.toByte,0x16.toByte,0xB8.toByte,0xA2.toByte,0xD3.toByte,0x33.toByte,0xB7.toByte,0xF3.toByte,0x32.toByte,0xFF.toByte,0x6B.toByte,0xA0.toByte,0x69.toByte,0x9C.toByte)
  assert(expectedt3sigs.deep==tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  transactNum=3
  val expectedt4Nonce = Array(0x02.toByte,0xD7.toByte,0xDE.toByte)
  assert(expectedt4Nonce.deep==tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt4Value = 130970170000000000L
  assert(expectedt4Value==tvalues(transactNum).get(0))
  val expectedt4ReceiveAddress= Array(0x1F.toByte,0x57.toByte,0xF8.toByte,0x26.toByte,0xCA.toByte,0xF5.toByte,0x94.toByte,0xF7.toByte,0xA8.toByte,0x37.toByte,0xD9.toByte,0xFC.toByte,0x09.toByte,0x24.toByte,0x56.toByte,0x87.toByte,0x0A.toByte,0x28.toByte,0x93.toByte,0x65.toByte)
     assert(expectedt4ReceiveAddress.deep==treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt4GasPrice=20000000000L
  assert(expectedt4GasPrice==tgasPrices(transactNum).get(0))
  val expectedt4GasLimit=90000
  assert(expectedt4GasLimit==tgasLimits(transactNum).get(0))
  val expectedt4Data : Array[Byte] = Array()
  assert(expectedt4Data.deep==tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt4sigv = Array(0x1B.toByte)
  assert(expectedt4sigv.deep==tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt4sigr = Array(0x46.toByte,0x01.toByte,0x57.toByte,0xDC.toByte,0xE4.toByte,0xE9.toByte,0x5D.toByte,0x1D.toByte,0xCC.toByte,0x7A.toByte,0xED.toByte,0x0D.toByte,0x9B.toByte,0x7E.toByte,0x3D.toByte,0x65.toByte,0x37.toByte,0x0C.toByte,0x53.toByte,0xD2.toByte,0x9E.toByte,0xA9.toByte,0xB1.toByte,0xAA.toByte,0x4C.toByte,0x9C.toByte,0x22.toByte,0x14.toByte,0x91.toByte,0x1C.toByte,0xD9.toByte,0x5E.toByte)
    assert(expectedt4sigr.deep==tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt4sigs = Array(0x6A.toByte,0x84.toByte,0x4F.toByte,0x95.toByte,0x6D.toByte,0x02.toByte,0x46.toByte,0x94.toByte,0x1B.toByte,0x94.toByte,0x30.toByte,0x91.toByte,0x34.toByte,0x21.toByte,0x20.toByte,0xBD.toByte,0x48.toByte,0xE7.toByte,0xC6.toByte,0x35.toByte,0x77.toByte,0xF0.toByte,0xBA.toByte,0x3D.toByte,0x87.toByte,0x59.toByte,0xC9.toByte,0xEC.toByte,0x58.toByte,0x70.toByte,0x4E.toByte,0xEC.toByte)
    assert(expectedt4sigs.deep==tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  transactNum=4
  val expectedt5Nonce = Array(0x02.toByte,0xD7.toByte,0xDF.toByte)
  assert(expectedt5Nonce.deep==tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt5Value = 144683800000000000L
  assert(expectedt5Value==tvalues(transactNum).get(0))
  val expectedt5ReceiveAddress= Array(0x1F.toByte,0x57.toByte,0xF8.toByte,0x26.toByte,0xCA.toByte,0xF5.toByte,0x94.toByte,0xF7.toByte,0xA8.toByte,0x37.toByte,0xD9.toByte,0xFC.toByte,0x09.toByte,0x24.toByte,0x56.toByte,0x87.toByte,0x0A.toByte,0x28.toByte,0x93.toByte,0x65.toByte)
assert(expectedt5ReceiveAddress.deep==treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt5GasPrice=20000000000L
  assert(expectedt5GasPrice==tgasPrices(transactNum).get(0))
  val expectedt5GasLimit=90000
  assert(expectedt5GasLimit==tgasLimits(transactNum).get(0))
  val expectedt5Data : Array[Byte] = Array()
  assert(expectedt5Data.deep==tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt5sigv = Array(0x1C.toByte)
  assert(expectedt5sigv.deep==tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt5sigr = Array(0xE4.toByte,0xBE.toByte,0x97.toByte,0xD5.toByte,0xAF.toByte,0xF1.toByte,0xB5.toByte,0xE7.toByte,0x99.toByte,0x12.toByte,0x96.toByte,0x98.toByte,0x2B.toByte,0xDF.toByte,0xC1.toByte,0xC2.toByte,0x2F.toByte,0x75.toByte,0x21.toByte,0x13.toByte,0x4F.toByte,0x7E.toByte,0x1A.toByte,0x9D.toByte,0xA3.toByte,0x00.toByte,0x42.toByte,0x0D.toByte,0xAD.toByte,0x33.toByte,0x6F.toByte,0x34.toByte)
    assert(expectedt5sigr.deep==tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt5sigs = Array(0x62.toByte,0xDE.toByte,0xF8.toByte,0xAA.toByte,0x83.toByte,0x65.toByte,0x58.toByte,0xC7.toByte,0xB0.toByte,0xA5.toByte,0x65.toByte,0xB9.toByte,0x7C.toByte,0x9B.toByte,0x27.toByte,0xB2.toByte,0x0E.toByte,0xD9.toByte,0xA0.toByte,0x51.toByte,0xDE.toByte,0x22.toByte,0xAD.toByte,0x8D.toByte,0xBD.toByte,0x62.toByte,0x52.toByte,0x44.toByte,0xCE.toByte,0x64.toByte,0x9E.toByte,0x3D.toByte)
     assert(expectedt5sigs.deep==tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  transactNum=5
  val expectedt6Nonce =  Array(0x02.toByte,0xD7.toByte,0xE0.toByte)
  assert(expectedt6Nonce.deep==tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt6Value = 143694920000000000L
  assert(expectedt6Value==tvalues(transactNum).get(0))
  val expectedt6ReceiveAddress= Array(0x1F.toByte,0x57.toByte,0xF8.toByte,0x26.toByte,0xCA.toByte,0xF5.toByte,0x94.toByte,0xF7.toByte,0xA8.toByte,0x37.toByte,0xD9.toByte,0xFC.toByte,0x09.toByte,0x24.toByte,0x56.toByte,0x87.toByte,0x0A.toByte,0x28.toByte,0x93.toByte,0x65.toByte)
    assert(expectedt6ReceiveAddress.deep==treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt6GasPrice=20000000000L
  assert(expectedt6GasPrice==tgasPrices(transactNum).get(0))
  val expectedt6GasLimit=90000
  assert(expectedt6GasLimit==tgasLimits(transactNum).get(0))
  val expectedt6Data : Array[Byte] = Array()
  assert(expectedt6Data.deep==tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt6sigv = Array(0x1C.toByte)
  assert(expectedt6sigv.deep==tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt6sigr = Array(0x0A.toByte,0x4C.toByte,0xA2.toByte,0x18.toByte,0x46.toByte,0x0D.toByte,0xC8.toByte,0x5B.toByte,0x99.toByte,0x07.toByte,0x46.toByte,0xFB.toByte,0xB9.toByte,0x0C.toByte,0x06.toByte,0xF8.toByte,0x25.toByte,0x87.toByte,0x82.toByte,0x80.toByte,0x87.toByte,0x27.toByte,0x98.toByte,0x3C.toByte,0x8B.toByte,0x8D.toByte,0x6A.toByte,0x92.toByte,0x1E.toByte,0x19.toByte,0x9B.toByte,0xCA.toByte)
   assert(expectedt6sigr.deep==tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
  val expectedt6sigs = Array(0x08.toByte,0xA3.toByte,0xB9.toByte,0xA4.toByte,0x5D.toByte,0x83.toByte,0x1A.toByte,0xC4.toByte,0xAD.toByte,0x37.toByte,0x9D.toByte,0x14.toByte,0xF0.toByte,0xAE.toByte,0x3C.toByte,0x03.toByte,0xC8.toByte,0x73.toByte,0x1C.toByte,0xB4.toByte,0x4D.toByte,0x8A.toByte,0x79.toByte,0xAC.toByte,0xD4.toByte,0xCD.toByte,0x6C.toByte,0xEA.toByte,0x1B.toByte,0x54.toByte,0x80.toByte,0x02.toByte)
  assert(expectedt6sigs.deep==tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)

  // uncle headers
    // block does not contain uncleheaders

  import df.sparkSession.implicits._

  df.as[EthereumBlock].collect()}

  "The block 1346406 on DFS" should "be fully read in dataframe with enriched information" in {
    Given("Block 1346406 on DFSCluster")
    // create input directory
    dfsCluster.getFileSystem().delete(DFS_INPUT_DIR, true)
    dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
    // copy bitcoin blocks
    val classLoader = getClass.getClassLoader
    // put testdata on DFS
    val fileName: String = "eth1346406.bin"
    val fileNameFullLocal = classLoader.getResource("testdata/" + fileName).getFile
    val inputFile = new Path(fileNameFullLocal)
    dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)
    When("reading block 1346406 using datasource")
    val df = sqlContext.read.format("org.zuinnote.spark.ethereum.block").option("enrich", "true").load(dfsCluster.getFileSystem().getUri.toString + DFS_INPUT_DIR_NAME)
    Then("all fields should be readable trough Spark SQL")
    // check first if structure is correct
    assert("ethereumBlockHeader" == df.columns(0))
    assert("ethereumTransactions" == df.columns(1))
    assert("uncleHeaders" == df.columns(2))

    val ethereumTransactionsDF = df.select(explode(df("ethereumTransactions")).alias("ethereumTransactions"))
    val ethereumUncleHeaderDF = df.select(explode(df("uncleHeaders")).alias("uncleHeaders"))
    // check if content is correct
    // sanity checks
    assert(6 == ethereumTransactionsDF.count())
    assert(0 == ethereumUncleHeaderDF.count())
    // check details
    // block header
    val expectedBhParentHash = Array(0xba.toByte, 0x6d.toByte, 0xd2.toByte, 0x60.toByte, 0x12.toByte, 0xb3.toByte, 0x71.toByte, 0x90.toByte, 0x48.toByte, 0xf3.toByte, 0x16.toByte, 0xc6.toByte, 0xed.toByte, 0xb3.toByte, 0x34.toByte, 0x9b.toByte, 0xdf.toByte, 0xbd.toByte, 0x61.toByte, 0x31.toByte, 0x9f.toByte, 0xa9.toByte, 0x7c.toByte, 0x61.toByte, 0x6a.toByte, 0x61.toByte, 0x31.toByte, 0x18.toByte, 0xa1.toByte, 0xaf.toByte, 0x30.toByte, 0x67.toByte)
    val bhParentHash = df.select("ethereumBlockHeader.parentHash").collect
    assert(expectedBhParentHash.deep == bhParentHash(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectdBhUncleHash = Array(0x1D.toByte, 0xCC.toByte, 0x4D.toByte, 0xE8.toByte, 0xDE.toByte, 0xC7.toByte, 0x5D.toByte, 0x7A.toByte, 0xAB.toByte, 0x85.toByte, 0xB5.toByte, 0x67.toByte, 0xB6.toByte, 0xCC.toByte, 0xD4.toByte, 0x1A.toByte, 0xD3.toByte, 0x12.toByte, 0x45.toByte, 0x1B.toByte, 0x94.toByte, 0x8A.toByte, 0x74.toByte, 0x13.toByte, 0xF0.toByte, 0xA1.toByte, 0x42.toByte, 0xFD.toByte, 0x40.toByte, 0xD4.toByte, 0x93.toByte, 0x47.toByte)
    val bhUncleHash = df.select("ethereumBlockHeader.uncleHash").collect
    assert(expectdBhUncleHash.deep == bhUncleHash(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhCoinBase = Array(0x1A.toByte, 0x06.toByte, 0x0B.toByte, 0x06.toByte, 0x04.toByte, 0x88.toByte, 0x3A.toByte, 0x99.toByte, 0x80.toByte, 0x9E.toByte, 0xB3.toByte, 0xF7.toByte, 0x98.toByte, 0xDF.toByte, 0x71.toByte, 0xBE.toByte, 0xF6.toByte, 0xC3.toByte, 0x58.toByte, 0xF1.toByte)
    val bhCoinBase = df.select("ethereumBlockHeader.coinBase").collect
    assert(expectedBhCoinBase.deep == bhCoinBase(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhStateRoot = Array(0x21.toByte, 0xBA.toByte, 0x88.toByte, 0x6F.toByte, 0xD2.toByte, 0x6F.toByte, 0x17.toByte, 0xB4.toByte, 0x01.toByte, 0xF5.toByte, 0x39.toByte, 0x20.toByte, 0x15.toByte, 0x33.toByte, 0x10.toByte, 0xB6.toByte, 0x93.toByte, 0x9B.toByte, 0xAD.toByte, 0x8A.toByte, 0x5F.toByte, 0xC3.toByte, 0xBF.toByte, 0x8C.toByte, 0x50.toByte, 0x5C.toByte, 0x55.toByte, 0x6D.toByte, 0xDB.toByte, 0xAF.toByte, 0xBC.toByte, 0x5C.toByte)
    val bhStateRoot = df.select("ethereumBlockHeader.stateRoot").collect
    assert(expectedBhStateRoot.deep == bhStateRoot(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhTxTrieRoot = Array(0xB3.toByte, 0xCB.toByte, 0xC7.toByte, 0xF0.toByte, 0xD7.toByte, 0x87.toByte, 0xE5.toByte, 0x7D.toByte, 0x93.toByte, 0x70.toByte, 0xB8.toByte, 0x02.toByte, 0xAB.toByte, 0x94.toByte, 0x5E.toByte, 0x21.toByte, 0x99.toByte, 0x1C.toByte, 0x3E.toByte, 0x12.toByte, 0x7D.toByte, 0x70.toByte, 0x12.toByte, 0x0C.toByte, 0x37.toByte, 0xE9.toByte, 0xFD.toByte, 0xAE.toByte, 0x3E.toByte, 0xF3.toByte, 0xEB.toByte, 0xFC.toByte)
    val bhTxTrieRoot = df.select("ethereumBlockHeader.txTrieRoot").collect
    assert(expectedBhTxTrieRoot.deep == bhTxTrieRoot(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhReceiptTrieRoot = Array(0x9B.toByte, 0xCE.toByte, 0x71.toByte, 0x32.toByte, 0xF5.toByte, 0x2D.toByte, 0x4D.toByte, 0x45.toByte, 0xA8.toByte, 0xA2.toByte, 0x47.toByte, 0x48.toByte, 0x47.toByte, 0x86.toByte, 0xC7.toByte, 0x0B.toByte, 0xB2.toByte, 0xE6.toByte, 0x39.toByte, 0x59.toByte, 0xC8.toByte, 0x56.toByte, 0x1B.toByte, 0x3A.toByte, 0xBF.toByte, 0xD4.toByte, 0xE7.toByte, 0x22.toByte, 0xE6.toByte, 0x00.toByte, 0x6A.toByte, 0x27.toByte)
    val bhReceiptTrieRoot = df.select("ethereumBlockHeader.receiptTrieRoot").collect
    assert(expectedBhReceiptTrieRoot.deep == bhReceiptTrieRoot(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhLogsBloom = Array(0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte)
    val bhLogsBloom = df.select("ethereumBlockHeader.logsBloom").collect
    assert(expectedBhLogsBloom.deep == bhLogsBloom(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhDifficulty = Array(0x19.toByte, 0xFF.toByte, 0x9E.toByte, 0xC4.toByte, 0x35.toByte, 0xE0.toByte)
    val bhDifficulty = df.select("ethereumBlockHeader.difficulty").collect
    assert(expectedBhDifficulty.deep == bhDifficulty(0).get(0).asInstanceOf[Array[Byte]].deep)
    val format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z")
    val expectedDTStr = "16-04-2016 09:34:29 UTC"
    val expectedBhTimestamp = format.parse(expectedDTStr).getTime / 1000
    val bhTimestamp = df.select("ethereumBlockHeader.timestamp").collect
    assert(expectedBhTimestamp == bhTimestamp(0).get(0))
    val expectedBhNumber = 1346406
    val bhNumber = df.select("ethereumBlockHeader.number").collect
    assert(expectedBhNumber == bhNumber(0).get(0))
    val expectedBhGasLimit = 4712388
    val bhGasLimit = df.select("ethereumBlockHeader.gasLimit").collect
    assert(expectedBhGasLimit == bhGasLimit(0).get(0))
    val expectedBhGasUsed = 126000
    val bhGasUsed = df.select("ethereumBlockHeader.gasUsed").collect
    assert(expectedBhGasUsed == bhGasUsed(0).get(0))
    val expectedBhMixHash = Array(0x4F.toByte, 0x57.toByte, 0x71.toByte, 0xB7.toByte, 0x9A.toByte, 0x8E.toByte, 0x6E.toByte, 0x21.toByte, 0x99.toByte, 0x35.toByte, 0x53.toByte, 0x9C.toByte, 0x47.toByte, 0x3E.toByte, 0x23.toByte, 0xBA.toByte, 0xFD.toByte, 0x2C.toByte, 0xA3.toByte, 0x5C.toByte, 0xC1.toByte, 0x86.toByte, 0x20.toByte, 0x66.toByte, 0x31.toByte, 0xC3.toByte, 0xB0.toByte, 0x9E.toByte, 0xD5.toByte, 0x76.toByte, 0x19.toByte, 0x4A.toByte)
    val bhMixHash = df.select("ethereumBlockHeader.mixHash").collect
    assert(expectedBhMixHash.deep == bhMixHash(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhExtraData = Array(0xD7.toByte, 0x83.toByte, 0x01.toByte, 0x03.toByte, 0x05.toByte, 0x84.toByte, 0x47.toByte, 0x65.toByte, 0x74.toByte, 0x68.toByte, 0x87.toByte, 0x67.toByte, 0x6F.toByte, 0x31.toByte, 0x2E.toByte, 0x35.toByte, 0x2E.toByte, 0x31.toByte, 0x85.toByte, 0x6C.toByte, 0x69.toByte, 0x6E.toByte, 0x75.toByte, 0x78.toByte)
    val bhExtraData = df.select("ethereumBlockHeader.extraData").collect
    assert(expectedBhExtraData.deep == bhExtraData(0).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedBhNonce = Array(0xFF.toByte, 0x7C.toByte, 0x7A.toByte, 0xEE.toByte, 0x0E.toByte, 0x88.toByte, 0xC5.toByte, 0x2D.toByte)
    val bhNonce = df.select("ethereumBlockHeader.nonce").collect
    assert(expectedBhNonce.deep == bhNonce(0).get(0).asInstanceOf[Array[Byte]].deep)
    // transactions
    val tnonces = ethereumTransactionsDF.select("ethereumTransactions.nonce").collect
    val tvalues = ethereumTransactionsDF.select("ethereumTransactions.value").collect
    val treceiveAddresses = ethereumTransactionsDF.select("ethereumTransactions.receiveAddress").collect
    val tgasPrices = ethereumTransactionsDF.select("ethereumTransactions.gasPrice").collect
    val tgasLimits = ethereumTransactionsDF.select("ethereumTransactions.gasLimit").collect
    val tdatas = ethereumTransactionsDF.select("ethereumTransactions.data").collect
    val tsigvs = ethereumTransactionsDF.select("ethereumTransactions.sig_v").collect
    val tsigrs = ethereumTransactionsDF.select("ethereumTransactions.sig_r").collect
    val tsigss = ethereumTransactionsDF.select("ethereumTransactions.sig_s").collect
    val tsendAddresses = ethereumTransactionsDF.select("ethereumTransactions.sendAddress").collect
    val thashes = ethereumTransactionsDF.select("ethereumTransactions.hash").collect
    var transactNum = 0
    val expectedt1Nonce = Array(0x0C.toByte)
    assert(expectedt1Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1Value = 1069000990000000000L
    assert(expectedt1Value == tvalues(transactNum).get(0))
    val expectedt1ReceiveAddress = Array(0x1E.toByte, 0x75.toByte, 0xF0.toByte, 0x2A.toByte, 0x6E.toByte, 0x9F.toByte, 0xF4.toByte, 0xFF.toByte, 0x16.toByte, 0x33.toByte, 0x38.toByte, 0x25.toByte, 0xD9.toByte, 0x09.toByte, 0xBB.toByte, 0x03.toByte, 0x33.toByte, 0x06.toByte, 0xB7.toByte, 0x8B.toByte)
    assert(expectedt1ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1GasPrice = 20000000000L
    assert(expectedt1GasPrice == tgasPrices(transactNum).get(0))
    val expectedt1GasLimit = 21000
    assert(expectedt1GasLimit == tgasLimits(transactNum).get(0))
    val expectedt1Data: Array[Byte] = Array()
    assert(expectedt1Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1sigv = Array(0x1B.toByte)
    assert(expectedt1sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1sigr = Array(0x47.toByte, 0xDD.toByte, 0xF9.toByte, 0x37.toByte, 0x68.toByte, 0x97.toByte, 0x76.toByte, 0x78.toByte, 0x13.toByte, 0x95.toByte, 0x5A.toByte, 0x9D.toByte, 0x46.toByte, 0xB6.toByte, 0xF1.toByte, 0xAA.toByte, 0x77.toByte, 0x73.toByte, 0xE5.toByte, 0xC8.toByte, 0xC6.toByte, 0x21.toByte, 0x67.toByte, 0x54.toByte, 0x1C.toByte, 0x80.toByte, 0xBF.toByte, 0x25.toByte, 0x2D.toByte, 0xC7.toByte, 0xDC.toByte, 0xD2.toByte)
    assert(expectedt1sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1sigs = Array(0x0A.toByte, 0x31.toByte, 0x3F.toByte, 0x35.toByte, 0x60.toByte, 0x32.toByte, 0x37.toByte, 0x56.toByte, 0xB7.toByte, 0x28.toByte, 0x5F.toByte, 0x62.toByte, 0x38.toByte, 0x51.toByte, 0x86.toByte, 0x05.toByte, 0x82.toByte, 0x1A.toByte, 0x2B.toByte, 0xEE.toByte, 0x03.toByte, 0x7D.toByte, 0xEA.toByte, 0x8F.toByte, 0x09.toByte, 0x22.toByte, 0x66.toByte, 0x20.toByte, 0x89.toByte, 0x03.toByte, 0x74.toByte, 0x59.toByte)
    assert(expectedt1sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1sendAddress = Array(0x39.toByte, 0x42.toByte, 0x4B.toByte, 0xD2.toByte, 0x8A.toByte, 0x22.toByte, 0x23.toByte, 0xDA.toByte, 0x3E.toByte, 0x14.toByte, 0xBF.toByte, 0x79.toByte, 0x3C.toByte, 0xF7.toByte, 0xF8.toByte, 0x20.toByte, 0x8E.toByte, 0xE9.toByte, 0x98.toByte, 0x0A.toByte)
    assert(expectedt1sendAddress.deep == tsendAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt1hash = Array(0xE2.toByte, 0x7E.toByte, 0x92.toByte, 0x88.toByte, 0xE2.toByte, 0x9C.toByte, 0xC8.toByte, 0xEB.toByte, 0x78.toByte, 0xF9.toByte, 0xF7.toByte, 0x68.toByte, 0xD8.toByte, 0x9B.toByte, 0xF1.toByte, 0xCD.toByte, 0x4B.toByte, 0x68.toByte, 0xB7.toByte, 0x15.toByte, 0xA3.toByte, 0x8B.toByte, 0x95.toByte, 0xD4.toByte, 0x6D.toByte, 0x77.toByte, 0x86.toByte, 0x18.toByte, 0xCB.toByte, 0x10.toByte, 0x4D.toByte, 0x58.toByte)
    assert(expectedt1hash.deep == thashes(transactNum).get(0).asInstanceOf[Array[Byte]].deep)

    transactNum = 1
    val expectedt2Nonce = Array(0xFF.toByte, 0xD7.toByte)
    assert(expectedt2Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2Value = 5110508700000000000L
    assert(expectedt2Value == tvalues(transactNum).get(0))
    val expectedt2ReceiveAddress = Array(0x54.toByte, 0x67.toByte, 0xFA.toByte, 0xBD.toByte, 0x30.toByte, 0xEB.toByte, 0x61.toByte, 0xA1.toByte, 0x84.toByte, 0x61.toByte, 0xD1.toByte, 0x53.toByte, 0xD8.toByte, 0xC6.toByte, 0xFF.toByte, 0xB1.toByte, 0x9D.toByte, 0xD4.toByte, 0x7A.toByte, 0x25.toByte)
    assert(expectedt2ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2GasPrice = 20000000000L
    assert(expectedt2GasPrice == tgasPrices(transactNum).get(0))
    val expectedt2GasLimit = 90000
    assert(expectedt2GasLimit == tgasLimits(transactNum).get(0))
    val expectedt2Data: Array[Byte] = Array()
    assert(expectedt2Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2sigv = Array(0x1B.toByte)
    assert(expectedt2sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2sigr = Array(0x62.toByte, 0x85.toByte, 0x3C.toByte, 0x63.toByte, 0x9A.toByte, 0x9B.toByte, 0x9E.toByte, 0xC4.toByte, 0x9B.toByte, 0xA9.toByte, 0xAC.toByte, 0x53.toByte, 0xE2.toByte, 0x85.toByte, 0xB3.toByte, 0x4E.toByte, 0xD0.toByte, 0xB7.toByte, 0x65.toByte, 0x5C.toByte, 0x1B.toByte, 0xE3.toByte, 0x29.toByte, 0xFB.toByte, 0x8B.toByte, 0x34.toByte, 0x70.toByte, 0x74.toByte, 0x0C.toByte, 0x3D.toByte, 0x0A.toByte, 0x9A.toByte)
    assert(expectedt2sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2sigs = Array(0x03.toByte, 0xFA.toByte, 0xA6.toByte, 0xF4.toByte, 0xFF.toByte, 0x1A.toByte, 0x45.toByte, 0x76.toByte, 0xDF.toByte, 0x08.toByte, 0x9A.toByte, 0x9F.toByte, 0x9C.toByte, 0xB7.toByte, 0x9C.toByte, 0xF2.toByte, 0xED.toByte, 0xC1.toByte, 0xC5.toByte, 0xBD.toByte, 0xEC.toByte, 0x0F.toByte, 0xE7.toByte, 0x9C.toByte, 0x79.toByte, 0x2A.toByte, 0xCB.toByte, 0x9E.toByte, 0x83.toByte, 0xF2.toByte, 0x41.toByte)
    assert(expectedt2sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2sendAddress = Array(0x4B.toByte, 0xB9.toByte, 0x60.toByte, 0x91.toByte, 0xEE.toByte, 0x9D.toByte, 0x80.toByte, 0x2E.toByte, 0xD0.toByte, 0x39.toByte, 0xC4.toByte, 0xD1.toByte, 0xA5.toByte, 0xF6.toByte, 0x21.toByte, 0x6F.toByte, 0x90.toByte, 0xF8.toByte, 0x1B.toByte, 0x01.toByte)
    assert(expectedt2sendAddress.deep == tsendAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt2hash = Array(0x7A.toByte, 0x23.toByte, 0x2A.toByte, 0xA2.toByte, 0xAE.toByte, 0x6A.toByte, 0x5E.toByte, 0x1F.toByte, 0x32.toByte, 0xCA.toByte, 0x3A.toByte, 0xC9.toByte, 0x3F.toByte, 0x4F.toByte, 0xDB.toByte, 0x77.toByte, 0x98.toByte, 0x3E.toByte, 0x93.toByte, 0x2B.toByte, 0x38.toByte, 0x09.toByte, 0x93.toByte, 0x56.toByte, 0x44.toByte, 0x42.toByte, 0x08.toByte, 0xC6.toByte, 0x9D.toByte, 0x40.toByte, 0x86.toByte, 0x81.toByte)
    assert(expectedt2hash.deep == thashes(transactNum).get(0).asInstanceOf[Array[Byte]].deep)

    transactNum = 2
    val expectedt3Nonce = Array(0x02.toByte, 0xD7.toByte, 0xDD.toByte)
    assert(expectedt3Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3Value = 11667800000000000L
    assert(expectedt3Value == tvalues(transactNum).get(0))
    val expectedt3ReceiveAddress = Array(0xB4.toByte, 0xD0.toByte, 0xCA.toByte, 0x2B.toByte, 0x7E.toByte, 0x4C.toByte, 0xB1.toByte, 0xE0.toByte, 0x61.toByte, 0x0D.toByte, 0x02.toByte, 0x15.toByte, 0x4A.toByte, 0x10.toByte, 0x16.toByte, 0x3A.toByte, 0xB0.toByte, 0xF4.toByte, 0x2E.toByte, 0x65.toByte)
    assert(expectedt3ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3GasPrice = 20000000000L
    assert(expectedt3GasPrice == tgasPrices(transactNum).get(0))
    val expectedt3GasLimit = 90000
    assert(expectedt3GasLimit == tgasLimits(transactNum).get(0))
    val expectedt3Data: Array[Byte] = Array()
    assert(expectedt3Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3sigv = Array(0x1C.toByte)
    assert(expectedt3sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3sigr = Array(0x89.toByte, 0xFD.toByte, 0x7A.toByte, 0x62.toByte, 0xCF.toByte, 0x44.toByte, 0x77.toByte, 0xBF.toByte, 0xE5.toByte, 0xDB.toByte, 0xF0.toByte, 0xEE.toByte, 0xCF.toByte, 0x3A.toByte, 0x4A.toByte, 0x96.toByte, 0x71.toByte, 0x96.toByte, 0x96.toByte, 0xFB.toByte, 0xBE.toByte, 0x16.toByte, 0xBA.toByte, 0x0A.toByte, 0xBA.toByte, 0x1D.toByte, 0x63.toByte, 0x1D.toByte, 0x44.toByte, 0xC1.toByte, 0xEB.toByte, 0x58.toByte)
    assert(expectedt3sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3sigs = Array(0x24.toByte, 0x34.toByte, 0x48.toByte, 0x64.toByte, 0xEB.toByte, 0x6A.toByte, 0x60.toByte, 0xC6.toByte, 0x6F.toByte, 0xB5.toByte, 0xDA.toByte, 0xED.toByte, 0x02.toByte, 0xB5.toByte, 0x63.toByte, 0x52.toByte, 0xE8.toByte, 0x17.toByte, 0x42.toByte, 0x16.toByte, 0xB8.toByte, 0xA2.toByte, 0xD3.toByte, 0x33.toByte, 0xB7.toByte, 0xF3.toByte, 0x32.toByte, 0xFF.toByte, 0x6B.toByte, 0xA0.toByte, 0x69.toByte, 0x9C.toByte)
    assert(expectedt3sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3sendAddress = Array(0x63.toByte, 0xA9.toByte, 0x97.toByte, 0x5B.toByte, 0xA3.toByte, 0x1B.toByte, 0x0B.toByte, 0x96.toByte, 0x26.toByte, 0xB3.toByte, 0x43.toByte, 0x00.toByte, 0xF7.toByte, 0xF6.toByte, 0x27.toByte, 0x14.toByte, 0x7D.toByte, 0xF1.toByte, 0xF5.toByte, 0x26.toByte)
    assert(expectedt3sendAddress.deep == tsendAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt3hash = Array(0x14.toByte, 0x33.toByte, 0xE3.toByte, 0xCB.toByte, 0x66.toByte, 0x2F.toByte, 0x66.toByte, 0x8D.toByte, 0x87.toByte, 0xB8.toByte, 0x35.toByte, 0x55.toByte, 0x34.toByte, 0x5A.toByte, 0x20.toByte, 0xCC.toByte, 0xF8.toByte, 0x70.toByte, 0x6F.toByte, 0x25.toByte, 0x21.toByte, 0x49.toByte, 0x18.toByte, 0xE2.toByte, 0xF8.toByte, 0x1F.toByte, 0xE3.toByte, 0xD2.toByte, 0x1C.toByte, 0x9D.toByte, 0x5B.toByte, 0x23.toByte)
    assert(expectedt3hash.deep == thashes(transactNum).get(0).asInstanceOf[Array[Byte]].deep)

    transactNum = 3
    val expectedt4Nonce = Array(0x02.toByte, 0xD7.toByte, 0xDE.toByte)
    assert(expectedt4Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4Value = 130970170000000000L
    assert(expectedt4Value == tvalues(transactNum).get(0))
    val expectedt4ReceiveAddress = Array(0x1F.toByte, 0x57.toByte, 0xF8.toByte, 0x26.toByte, 0xCA.toByte, 0xF5.toByte, 0x94.toByte, 0xF7.toByte, 0xA8.toByte, 0x37.toByte, 0xD9.toByte, 0xFC.toByte, 0x09.toByte, 0x24.toByte, 0x56.toByte, 0x87.toByte, 0x0A.toByte, 0x28.toByte, 0x93.toByte, 0x65.toByte)
    assert(expectedt4ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4GasPrice = 20000000000L
    assert(expectedt4GasPrice == tgasPrices(transactNum).get(0))
    val expectedt4GasLimit = 90000
    assert(expectedt4GasLimit == tgasLimits(transactNum).get(0))
    val expectedt4Data: Array[Byte] = Array()
    assert(expectedt4Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4sigv = Array(0x1B.toByte)
    assert(expectedt4sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4sigr = Array(0x46.toByte, 0x01.toByte, 0x57.toByte, 0xDC.toByte, 0xE4.toByte, 0xE9.toByte, 0x5D.toByte, 0x1D.toByte, 0xCC.toByte, 0x7A.toByte, 0xED.toByte, 0x0D.toByte, 0x9B.toByte, 0x7E.toByte, 0x3D.toByte, 0x65.toByte, 0x37.toByte, 0x0C.toByte, 0x53.toByte, 0xD2.toByte, 0x9E.toByte, 0xA9.toByte, 0xB1.toByte, 0xAA.toByte, 0x4C.toByte, 0x9C.toByte, 0x22.toByte, 0x14.toByte, 0x91.toByte, 0x1C.toByte, 0xD9.toByte, 0x5E.toByte)
    assert(expectedt4sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4sigs = Array(0x6A.toByte, 0x84.toByte, 0x4F.toByte, 0x95.toByte, 0x6D.toByte, 0x02.toByte, 0x46.toByte, 0x94.toByte, 0x1B.toByte, 0x94.toByte, 0x30.toByte, 0x91.toByte, 0x34.toByte, 0x21.toByte, 0x20.toByte, 0xBD.toByte, 0x48.toByte, 0xE7.toByte, 0xC6.toByte, 0x35.toByte, 0x77.toByte, 0xF0.toByte, 0xBA.toByte, 0x3D.toByte, 0x87.toByte, 0x59.toByte, 0xC9.toByte, 0xEC.toByte, 0x58.toByte, 0x70.toByte, 0x4E.toByte, 0xEC.toByte)
    assert(expectedt4sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4sendAddress = Array(0x63.toByte, 0xA9.toByte, 0x97.toByte, 0x5B.toByte, 0xA3.toByte, 0x1B.toByte, 0x0B.toByte, 0x96.toByte, 0x26.toByte, 0xB3.toByte, 0x43.toByte, 0x00.toByte, 0xF7.toByte, 0xF6.toByte, 0x27.toByte, 0x14.toByte, 0x7D.toByte, 0xF1.toByte, 0xF5.toByte, 0x26.toByte)
    assert(expectedt4sendAddress.deep == tsendAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt4hash = Array(0x39.toByte, 0x22.toByte, 0xF7.toByte, 0xF6.toByte, 0x0A.toByte, 0x33.toByte, 0xA1.toByte, 0x2D.toByte, 0x13.toByte, 0x9D.toByte, 0x67.toByte, 0xFA.toByte, 0x53.toByte, 0x30.toByte, 0xDB.toByte, 0xFD.toByte, 0xBA.toByte, 0x42.toByte, 0xA4.toByte, 0xB7.toByte, 0x67.toByte, 0x29.toByte, 0x6E.toByte, 0xFF.toByte, 0x64.toByte, 0x15.toByte, 0xEE.toByte, 0xA3.toByte, 0x2D.toByte, 0x8A.toByte, 0x7B.toByte, 0x2B.toByte)
    assert(expectedt4hash.deep == thashes(transactNum).get(0).asInstanceOf[Array[Byte]].deep)

    transactNum = 4
    val expectedt5Nonce = Array(0x02.toByte, 0xD7.toByte, 0xDF.toByte)
    assert(expectedt5Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5Value = 144683800000000000L
    assert(expectedt5Value == tvalues(transactNum).get(0))
    val expectedt5ReceiveAddress = Array(0x1F.toByte, 0x57.toByte, 0xF8.toByte, 0x26.toByte, 0xCA.toByte, 0xF5.toByte, 0x94.toByte, 0xF7.toByte, 0xA8.toByte, 0x37.toByte, 0xD9.toByte, 0xFC.toByte, 0x09.toByte, 0x24.toByte, 0x56.toByte, 0x87.toByte, 0x0A.toByte, 0x28.toByte, 0x93.toByte, 0x65.toByte)
    assert(expectedt5ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5GasPrice = 20000000000L
    assert(expectedt5GasPrice == tgasPrices(transactNum).get(0))
    val expectedt5GasLimit = 90000
    assert(expectedt5GasLimit == tgasLimits(transactNum).get(0))
    val expectedt5Data: Array[Byte] = Array()
    assert(expectedt5Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5sigv = Array(0x1C.toByte)
    assert(expectedt5sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5sigr = Array(0xE4.toByte, 0xBE.toByte, 0x97.toByte, 0xD5.toByte, 0xAF.toByte, 0xF1.toByte, 0xB5.toByte, 0xE7.toByte, 0x99.toByte, 0x12.toByte, 0x96.toByte, 0x98.toByte, 0x2B.toByte, 0xDF.toByte, 0xC1.toByte, 0xC2.toByte, 0x2F.toByte, 0x75.toByte, 0x21.toByte, 0x13.toByte, 0x4F.toByte, 0x7E.toByte, 0x1A.toByte, 0x9D.toByte, 0xA3.toByte, 0x00.toByte, 0x42.toByte, 0x0D.toByte, 0xAD.toByte, 0x33.toByte, 0x6F.toByte, 0x34.toByte)
    assert(expectedt5sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5sigs = Array(0x62.toByte, 0xDE.toByte, 0xF8.toByte, 0xAA.toByte, 0x83.toByte, 0x65.toByte, 0x58.toByte, 0xC7.toByte, 0xB0.toByte, 0xA5.toByte, 0x65.toByte, 0xB9.toByte, 0x7C.toByte, 0x9B.toByte, 0x27.toByte, 0xB2.toByte, 0x0E.toByte, 0xD9.toByte, 0xA0.toByte, 0x51.toByte, 0xDE.toByte, 0x22.toByte, 0xAD.toByte, 0x8D.toByte, 0xBD.toByte, 0x62.toByte, 0x52.toByte, 0x44.toByte, 0xCE.toByte, 0x64.toByte, 0x9E.toByte, 0x3D.toByte)
    assert(expectedt5sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5sendAddress = Array(0x63.toByte, 0xA9.toByte, 0x97.toByte, 0x5B.toByte, 0xA3.toByte, 0x1B.toByte, 0x0B.toByte, 0x96.toByte, 0x26.toByte, 0xB3.toByte, 0x43.toByte, 0x00.toByte, 0xF7.toByte, 0xF6.toByte, 0x27.toByte, 0x14.toByte, 0x7D.toByte, 0xF1.toByte, 0xF5.toByte, 0x26.toByte)
    assert(expectedt5sendAddress.deep == tsendAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt5hash = Array(0xBB.toByte, 0x7C.toByte, 0xAA.toByte, 0x23.toByte, 0x38.toByte, 0x5A.toByte, 0x0F.toByte, 0x73.toByte, 0x75.toByte, 0x3F.toByte, 0x9E.toByte, 0x28.toByte, 0xD8.toByte, 0xF0.toByte, 0x60.toByte, 0x2F.toByte, 0xE2.toByte, 0xE7.toByte, 0x2D.toByte, 0x87.toByte, 0xE1.toByte, 0xE0.toByte, 0x95.toByte, 0x52.toByte, 0x75.toByte, 0x28.toByte, 0xD1.toByte, 0x44.toByte, 0x88.toByte, 0x5D.toByte, 0x6B.toByte, 0x51.toByte)
    assert(expectedt5hash.deep == thashes(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    transactNum = 5
    val expectedt6Nonce = Array(0x02.toByte, 0xD7.toByte, 0xE0.toByte)
    assert(expectedt6Nonce.deep == tnonces(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6Value = 143694920000000000L
    assert(expectedt6Value == tvalues(transactNum).get(0))
    val expectedt6ReceiveAddress = Array(0x1F.toByte, 0x57.toByte, 0xF8.toByte, 0x26.toByte, 0xCA.toByte, 0xF5.toByte, 0x94.toByte, 0xF7.toByte, 0xA8.toByte, 0x37.toByte, 0xD9.toByte, 0xFC.toByte, 0x09.toByte, 0x24.toByte, 0x56.toByte, 0x87.toByte, 0x0A.toByte, 0x28.toByte, 0x93.toByte, 0x65.toByte)
    assert(expectedt6ReceiveAddress.deep == treceiveAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6GasPrice = 20000000000L
    assert(expectedt6GasPrice == tgasPrices(transactNum).get(0))
    val expectedt6GasLimit = 90000
    assert(expectedt6GasLimit == tgasLimits(transactNum).get(0))
    val expectedt6Data: Array[Byte] = Array()
    assert(expectedt6Data.deep == tdatas(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6sigv = Array(0x1C.toByte)
    assert(expectedt6sigv.deep == tsigvs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6sigr = Array(0x0A.toByte, 0x4C.toByte, 0xA2.toByte, 0x18.toByte, 0x46.toByte, 0x0D.toByte, 0xC8.toByte, 0x5B.toByte, 0x99.toByte, 0x07.toByte, 0x46.toByte, 0xFB.toByte, 0xB9.toByte, 0x0C.toByte, 0x06.toByte, 0xF8.toByte, 0x25.toByte, 0x87.toByte, 0x82.toByte, 0x80.toByte, 0x87.toByte, 0x27.toByte, 0x98.toByte, 0x3C.toByte, 0x8B.toByte, 0x8D.toByte, 0x6A.toByte, 0x92.toByte, 0x1E.toByte, 0x19.toByte, 0x9B.toByte, 0xCA.toByte)
    assert(expectedt6sigr.deep == tsigrs(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6sigs = Array(0x08.toByte, 0xA3.toByte, 0xB9.toByte, 0xA4.toByte, 0x5D.toByte, 0x83.toByte, 0x1A.toByte, 0xC4.toByte, 0xAD.toByte, 0x37.toByte, 0x9D.toByte, 0x14.toByte, 0xF0.toByte, 0xAE.toByte, 0x3C.toByte, 0x03.toByte, 0xC8.toByte, 0x73.toByte, 0x1C.toByte, 0xB4.toByte, 0x4D.toByte, 0x8A.toByte, 0x79.toByte, 0xAC.toByte, 0xD4.toByte, 0xCD.toByte, 0x6C.toByte, 0xEA.toByte, 0x1B.toByte, 0x54.toByte, 0x80.toByte, 0x02.toByte)
    assert(expectedt6sigs.deep == tsigss(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6sendAddress = Array(0x63.toByte, 0xA9.toByte, 0x97.toByte, 0x5B.toByte, 0xA3.toByte, 0x1B.toByte, 0x0B.toByte, 0x96.toByte, 0x26.toByte, 0xB3.toByte, 0x43.toByte, 0x00.toByte, 0xF7.toByte, 0xF6.toByte, 0x27.toByte, 0x14.toByte, 0x7D.toByte, 0xF1.toByte, 0xF5.toByte, 0x26.toByte)
    assert(expectedt6sendAddress.deep == tsendAddresses(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    val expectedt6hash = Array(0xBC.toByte, 0xDE.toByte, 0x6F.toByte, 0x49.toByte, 0x84.toByte, 0x2C.toByte, 0x6D.toByte, 0x73.toByte, 0x8D.toByte, 0x64.toByte, 0x32.toByte, 0x8F.toByte, 0x78.toByte, 0x09.toByte, 0xB1.toByte, 0xD4.toByte, 0x9B.toByte, 0xF0.toByte, 0xFF.toByte, 0x3F.toByte, 0xFA.toByte, 0x46.toByte, 0x0F.toByte, 0xDD.toByte, 0xD2.toByte, 0x7F.toByte, 0xD4.toByte, 0x2B.toByte, 0x7A.toByte, 0x01.toByte, 0xFC.toByte, 0x9A.toByte)
    assert(expectedt6hash.deep == thashes(transactNum).get(0).asInstanceOf[Array[Byte]].deep)
    // uncle headers
    // block does not contain uncleheaders


import df.sparkSession.implicits._

  df.as[EnrichedEthereumBlock].collect()

}


}
