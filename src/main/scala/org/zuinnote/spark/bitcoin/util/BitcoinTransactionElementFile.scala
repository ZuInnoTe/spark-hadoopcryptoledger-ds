/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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

package org.zuinnote.spark.bitcoin.util




import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext 


import org.apache.hadoop.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.mapred._


import org.apache.hadoop.fs.Path

import org.zuinnote.hadoop.bitcoin.format._


private[bitcoin] object BitcoinTransactionElementFile {
 
  def load(context: SQLContext, location: String, hadoopConf: JobConf): RDD[(BytesWritable, BitcoinTransactionElement)] = {
	FileInputFormat.addInputPath(hadoopConf, new Path(location));
	context.sparkContext.hadoopRDD(hadoopConf, classOf[BitcoinTransactionElementFileInputFormat], classOf[BytesWritable], classOf[BitcoinTransactionElement], 2);
  }
}
