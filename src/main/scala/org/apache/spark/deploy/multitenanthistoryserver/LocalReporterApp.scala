/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.deploy.multitenanthistoryserver

import java.io.{BufferedInputStream, InputStream}
import java.net.URI

import com.ning.compress.lzf.LZFInputStream
import com.qubole.sparklens.QuboleJobListener
import com.qubole.sparklens.analyzer.{AppAnalyzer, CriticalPathResult}
import com.qubole.sparklens.common.Json4sWrapper
import com.qubole.sparklens.helper.HDFSConfigHelper
import net.jpountz.lz4.LZ4BlockInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.scheduler.ReplayListenerBus
import org.json4s.DefaultFormats
import org.xerial.snappy.SnappyInputStream

object LocalReporterApp {
  lazy val bus = new ReplayListenerBus
  var criticalPathResult: Option[CriticalPathResult] = Option.empty

  def reportFromEventHistory(eventFile: String): Unit = {
    val sparkConf: SparkConf = new SparkConf
    val quboleSparkListener = new QuboleJobListener(sparkConf)

    bus.addListener(quboleSparkListener)
    bus.replay(getDecodedInputStream(eventFile, sparkConf), eventFile, boolean2Boolean(false), getFilter _)
    bus.removeListener(quboleSparkListener)

    criticalPathResult = Option(AppAnalyzer.criticalPathAnalyzer.criticalPathResult)
  }

  // Borrowed from CompressionCodecs in spark
  private def getDecodedInputStream(file: String, conf: SparkConf): InputStream = {

    val fs = FileSystem.get(new URI(file), HDFSConfigHelper.getHadoopConf(Some(conf)))
    val path = new Path(file)
    val bufStream = new BufferedInputStream(fs.open(path))

    val logName = path.getName.stripSuffix(".inprogress")
    val codecName: Option[String] = logName.split("\\.").tail.lastOption

    codecName.getOrElse("") match {
      case "lz4" => new LZ4BlockInputStream(bufStream)
      case "lzf" => new LZFInputStream(bufStream)
      case "snappy" => new SnappyInputStream(bufStream)
      case _ => bufStream
    }

  }

  private def getFilter(eventString: String): Boolean = {
    implicit val formats = DefaultFormats
    eventFilter.contains(Json4sWrapper.parse(eventString).extract[Map[String, Any]].get("Event")
      .get.asInstanceOf[String])
  }

  private def eventFilter: Set[String] = {
    Set(
      "SparkListenerTaskEnd",
      "SparkListenerApplicationStart",
      "SparkListenerApplicationEnd",
      "SparkListenerExecutorAdded",
      "SparkListenerExecutorRemoved",
      "SparkListenerJobStart",
      "SparkListenerJobEnd",
      "SparkListenerStageSubmitted",
      "SparkListenerStageCompleted"
    )
  }

  def main(args: Array[String]): Unit = {
    //    val eventLogFile = "/Users/ruifang/Downloads/application_1593167700677_0076_1.inprogress"
    //    val eventLogFile = "/Users/ruifang/Downloads/application_1596531547620_0014_1"
    val eventLogFile = "/Users/ruifang/Downloads/application_1597747776597_0005_1"
    reportFromEventHistory(eventLogFile)
    println(s"Print debug info\n${criticalPathResult.get.debugInfo}")
    println(criticalPathResult.get.criticalPathJsonString)
    println(criticalPathResult.get.criticalPathWithAllStagesJsonString)
    println(criticalPathResult.get.criticalPathWithoutShortStagesJsonString(Option(0.70735)))
    println(criticalPathResult.get.jobsTimeLineJsonString)
  }
}
