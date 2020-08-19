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
import com.qubole.sparklens.common.AppContext
import com.qubole.sparklens.timespan.{JobTimeSpan, StageTimeSpan, TimeSpan}

import scala.collection.mutable

class CriticalPathAnalyzer extends AppAnalyzer {
  override def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val jobLevelCriticalPath = findCriticalPath(ac.jobMap.values.toArray)
    val out = new mutable.StringBuilder()
    out.println("\nCritical path analysis")
    out.println("\nCritical path in job level:")
    printTimeSpans(out, jobLevelCriticalPath)

    out.println("\nCritical path in stage level:")
    jobLevelCriticalPath.foreach(timeSpan => {
      val jobTimeSpan = timeSpan.asInstanceOf[JobTimeSpan]
      val stageLevelCriticalPath = findCriticalPath(jobTimeSpan.stageMap.values.toArray)
      out.println(s"Critical path in stage level for job ${jobTimeSpan.jobID}:")
      printTimeSpans(out, stageLevelCriticalPath)
    })
    out.toString()
  }

  def findCriticalPath(timeSpans: Array[TimeSpan]): Array[TimeSpan] = {
    val sortByEndTimeSpans = timeSpans.sortWith((a, b) => a.endTime <= b.endTime)
    var currentIndex = sortByEndTimeSpans.length - 1
    val resultStack = new mutable.ArrayStack[TimeSpan]()
    while (currentIndex >= 0) {
      if (resultStack.isEmpty || atLeft(sortByEndTimeSpans(currentIndex), resultStack.top)) {
        resultStack.push(sortByEndTimeSpans(currentIndex))
      }
      currentIndex = currentIndex - 1
    }
    resultStack.toArray   // stack.top will become the first element in the array
  }

  /**
   * Print the formatted output for critical path in different level
   * @param timeSpans time spans which constitute the critical path
   */
  def printTimeSpans(out: mutable.StringBuilder, timeSpans: Array[TimeSpan]): Unit = {
    timeSpans.foreach(timeSpan => {
      timeSpan match {
        case span: JobTimeSpan =>
          out.print(s"Job ${span.jobID}\t")
        case span: StageTimeSpan => {
          out.print(s"Stage ${span.stageID}\t")
        }
      }
      out.println(s"Start time: ${pt(timeSpan.startTime)}.\tEnd time: ${pt(timeSpan.endTime)}." +
        s"\tDuration: ${pd(timeSpan.duration().getOrElse(0))}")
    })
  }

  def atLeft(firstTime: TimeSpan, secondTime: TimeSpan): Boolean = {
    firstTime.endTime <= secondTime.startTime
  }
}