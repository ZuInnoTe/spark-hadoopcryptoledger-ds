/**
* Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/

/**
*
* This test intregrates HDFS and Spark
*
*/

package org.zuinnote.spark.bitcoin.transactionelement


import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path

import java.io.BufferedReader
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.SimpleFileVisitor
import java.util.ArrayList
import java.util.List


import org.apache.hadoop.io.compress.CodecPool
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.Decompressor
import org.apache.hadoop.io.compress.SplittableCompressionCodec
import org.apache.hadoop.io.compress.SplitCompressionInputStream


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.functions._


import scala.collection.mutable.ArrayBuffer
import org.scalatest.{FlatSpec, BeforeAndAfterAll, GivenWhenThen, Matchers}

class SparkBitcoinTransactionElementDSSparkMasterIntegrationSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {
 
private var sc: SparkContext = _
private var sqlContext: SQLContext = _
private val master: String = "local[2]"
private val appName: String = "spark-hadoocryptoledger-ds-integrationtest"
private val tmpPrefix: String = "hcl-integrationtest"
private var tmpPath: java.nio.file.Path = _
private val CLUSTERNAME: String ="hcl-minicluster"
private val DFS_INPUT_DIR_NAME: String = "/input"
private val DFS_OUTPUT_DIR_NAME: String = "/output"
private val DEFAULT_OUTPUT_FILENAME: String = "part-00000"
private val DFS_INPUT_DIR : Path = new Path(DFS_INPUT_DIR_NAME)
private val DFS_OUTPUT_DIR : Path = new Path(DFS_OUTPUT_DIR_NAME)
private val NOOFDATANODES: Int =1
private var dfsCluster: MiniDFSCluster = _
private var conf: Configuration = _
private var openDecompressors = ArrayBuffer[Decompressor]();

override def beforeAll(): Unit = {
    super.beforeAll()

		// Create temporary directory for HDFS base and shutdownhook 
	// create temp directory
      tmpPath = Files.createTempDirectory(tmpPrefix)
      // create shutdown hook to remove temp files (=HDFS MiniCluster) after shutdown, may need to rethink to avoid many threads are created
	Runtime.getRuntime.addShutdownHook(new Thread("remove temporary directory") {
      	 override def run(): Unit =  {
        	try {
          		Files.walkFileTree(tmpPath, new SimpleFileVisitor[java.nio.file.Path]() {

            		override def visitFile(file: java.nio.file.Path,attrs: BasicFileAttributes): FileVisitResult = {
                		Files.delete(file)
             			return FileVisitResult.CONTINUE
        			}

        		override def postVisitDirectory(dir: java.nio.file.Path, e: IOException): FileVisitResult = {
          			if (e == null) {
            				Files.delete(dir)
            				return FileVisitResult.CONTINUE
          			}
          			throw e
        			}
        	})
      	} catch {
        case e: IOException => throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e)
    }}})
	// create DFS mini cluster
	 conf = new Configuration()
	val baseDir = new File(tmpPath.toString()).getAbsoluteFile()
	conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
	val builder = new MiniDFSCluster.Builder(conf)
 	 dfsCluster = builder.numDataNodes(NOOFDATANODES).build()
	conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString()) 
	// create local Spark cluster
 	val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
	sc = new SparkContext(sparkConf)
	sqlContext = new SQLContext(sc)
 }

  
  override def afterAll(): Unit = {
   // close Spark Context
    if (sc!=null) {
	sc.stop()
    } 
    // close decompressor
	for ( currentDecompressor <- this.openDecompressors) {
		if (currentDecompressor!=null) {
			 CodecPool.returnDecompressor(currentDecompressor)
		}
 	}
    // close dfs cluster
    dfsCluster.shutdown()
    super.afterAll()
}


"The genesis block on DFS" should "be fully read in dataframe" in {
	Given("Genesis Block on DFSCluster")
	// create input directory
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
	// copy bitcoin blocks
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="genesis.blk"
    	val fileNameFullLocal=classLoader.getResource("testdata/"+fileName).getFile()
    	val inputFile=new Path(fileNameFullLocal)
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)	
	When("reading Genesis block using datasource")
	val df = sqlContext.read.format("org.zuinnote.spark.bitcoin.transactionelement").option("magic", "F9BEB4D9").load(dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME)
	Then("all fields should be readable trough Spark SQL")
	// check first if structure is correct
	assert("blockHash"==df.columns(0))
	assert("transactionIdxInBlock"==df.columns(1))
	assert("transactionHash"==df.columns(2))
	assert("type"==df.columns(3))
	assert("indexInTransaction"==df.columns(4))
	assert("amount"==df.columns(5))
	assert("script"==df.columns(6))
	// validate transaction data
	val currentBlockHash = df.select("blockHash").collect
	val currentBlockHashExpected : Array[Byte] = Array(0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x19.toByte,0xD6.toByte,0x68.toByte,0x9C.toByte,0x08.toByte,0x5A.toByte,0xE1.toByte,0x65.toByte,0x83.toByte,0x1E.toByte,0x93.toByte,
0x4F.toByte,0xF7.toByte,0x63.toByte,0xAE.toByte,0x46.toByte,0xA2.toByte,0xA6.toByte,0xC1.toByte,0x72.toByte,0xB3.toByte,0xF1.toByte,0xB6.toByte,0x0A.toByte,0x8C.toByte,0xE2.toByte,0x6F.toByte)
	assert(currentBlockHashExpected.deep==currentBlockHash(0).get(0).asInstanceOf[Array[Byte]].deep)
	val transactionIdxInBlock = df.select("transactionIdxInBlock").collect
	assert(0==transactionIdxInBlock(0).getInt(0))
val transactionHash = df.select("transactionHash").collect
	val transactionHashExpected : Array[Byte] = Array(0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte,0x00.toByte)
	assert(transactionHashExpected.deep==transactionHash(0).get(0).asInstanceOf[Array[Byte]].deep)
	val transactionType = df.select("type").collect
	assert(0==transactionType(0).getInt(0))
	val indexInTransaction = df.select("indexInTransaction").collect
	assert(4294967295L==indexInTransaction(0).getLong(0))
	val amount = df.select("amount").collect
	assert(0==amount(0).getLong(0))
	val script = df.select("script").collect
	val scriptExpected: Array[Byte] = Array(0x04.toByte,0xFF.toByte,0xFF.toByte,0x00.toByte,0x1D.toByte,0x01.toByte,0x04.toByte,0x45.toByte,0x54.toByte,0x68.toByte,0x65.toByte,0x20.toByte,0x54.toByte,0x69.toByte,0x6D.toByte,0x65.toByte,
0x73.toByte,0x20.toByte,0x30.toByte,0x33.toByte,0x2F.toByte,0x4A.toByte,0x61.toByte,0x6E.toByte,0x2F.toByte,0x32.toByte,0x30.toByte,0x30.toByte,0x39.toByte,0x20.toByte,0x43.toByte,0x68.toByte,
0x61.toByte,0x6E.toByte,0x63.toByte,0x65.toByte,0x6C.toByte,0x6C.toByte,0x6F.toByte,0x72.toByte,0x20.toByte,0x6F.toByte,0x6E.toByte,0x20.toByte,0x62.toByte,0x72.toByte,0x69.toByte,0x6E.toByte,0x6B.toByte,
0x20.toByte,0x6F.toByte,0x66.toByte,0x20.toByte,0x73.toByte,0x65.toByte,0x63.toByte,0x6F.toByte,0x6E.toByte,0x64.toByte,0x20.toByte,0x62.toByte,0x61.toByte,0x69.toByte,0x6C.toByte,0x6F.toByte,
0x75.toByte,0x74.toByte,0x20.toByte,0x66.toByte,0x6F.toByte,0x72.toByte,0x20.toByte,0x62.toByte,0x61.toByte,0x6E.toByte,0x6B.toByte,0x73.toByte)
	assert(scriptExpected.deep==script(0).get(0).asInstanceOf[Array[Byte]].deep)
}



}
