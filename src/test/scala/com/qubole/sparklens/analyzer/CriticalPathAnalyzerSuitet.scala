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
package com.qubole.sparklens.analyzer

import com.qubole.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import org.scalatest.FunSuite

import scala.collection.mutable

class CriticalPathAnalyzerSuitet extends FunSuite {
  def createDummyAppContext(jobMap: mutable.HashMap[Long, JobTimeSpan]): AppContext = {
    val applicationInfo = new ApplicationInfo("application_1597388408472_0001", 0L, Long.MaxValue)

    new AppContext(
      applicationInfo,
      new AggregateMetrics(),
      mutable.HashMap[String, HostTimeSpan](),
      mutable.HashMap[String, ExecutorTimeSpan](),
      jobMap,
      mutable.HashMap[Long, Long](),
      mutable.HashMap[Int, StageTimeSpan](),
      mutable.HashMap[Int, Long]())
  }

  def createJobMap(): mutable.HashMap[Long, JobTimeSpan] = {
    val jobMap = new mutable.HashMap[Long, JobTimeSpan]
    for (i <- 1 to 7) {
      jobMap(i) = new JobTimeSpan(i)
    }

    jobMap(1).setStartTime(1L)
    jobMap(1).setEndTime(2L)

    jobMap(2).setStartTime(1L)
    jobMap(2).setEndTime(6L)

    jobMap(3).setStartTime(1L)
    jobMap(3).setEndTime(7L)

    jobMap(4).setStartTime(3L)
    jobMap(4).setEndTime(4L)

    jobMap(5).setStartTime(5L)
    jobMap(5).setEndTime(8L)

    jobMap(6).setStartTime(10L)
    jobMap(6).setEndTime(11L)

    jobMap(7).setStartTime(13L)
    jobMap(7).setEndTime(15L)

    jobMap
  }

  test("Critical path basic test") {
    val ac = createDummyAppContext(createJobMap())
    val criticalPathOutput = new CriticalPathAnalyzer
    val output = criticalPathOutput.analyze(ac)
    println(output)
  }
}
