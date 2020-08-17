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
import com.qubole.sparklens.timespan.{JobTimeSpan, TimeSpan}

import scala.collection.mutable

class CriticalPathAnalyzer extends AppAnalyzer {
  override def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val jobLevelCriticalPath: Array[JobTimeSpan] = findDependentPath(ac.jobMap.values.toArray)
    val out = new mutable.StringBuilder()
    out.println("\nCritical path analysis\n")
    out.println("Critical path in job level:")
    jobLevelCriticalPath.foreach(job => out.println(s"Job ${job.jobID}\tStart time: ${job.startTime}.\tEnd time: ${job.endTime}.\tDuration: ${job.duration().getOrElse("")}"))
    out.toString()
  }

  def findDependentPath(timeSpans: Array[JobTimeSpan]): Array[JobTimeSpan] = {
    val sortByEndTimeSpans = timeSpans.sortWith((a, b) => a.endTime <= b.endTime)
    var currentIndex = sortByEndTimeSpans.length - 1
    val resultStack = new mutable.ArrayStack[JobTimeSpan]()
    while (currentIndex >= 0) {
      if (resultStack.isEmpty || earlyThan(sortByEndTimeSpans(currentIndex), resultStack.top)) {
        resultStack.push(sortByEndTimeSpans(currentIndex))
      }
      currentIndex = currentIndex - 1
    }
    resultStack.toArray   // stack.top will become the first element in the array
  }

  def earlyThan(firstTime: TimeSpan, secondTime: TimeSpan): Boolean = {
    firstTime.endTime <= secondTime.startTime
  }
}