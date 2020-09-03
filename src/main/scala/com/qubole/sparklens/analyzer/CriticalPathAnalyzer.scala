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
import com.qubole.sparklens.timespan.{JobTimeSpan, StageTimeSpan, TaskTimeSpan, TimeSpan}

import scala.collection.mutable

case class StageData(stageID: Long, startTime: Long, endTime: Long, duration: Long, inCriticalPath: Boolean = true)

case class CriticalPathData(jobID: Long, stagesData: List[StageData])

case class TaskData(taskID: Long, startTime: Long, endTime: Long, duration: Long)

case class LongRunningTasksData(stageID: Long, tasksData: List[TaskData])

case class CriticalPathResult(criticalPath: List[CriticalPathData],
                              criticalPathWithAllStages: List[CriticalPathData],
                              tasks: List[LongRunningTasksData],
                              debugInfo: String)

class CriticalPathAnalyzer extends AppAnalyzer {
  val LONG_RUNNING_TASK_DURATION_LOWER_BOUND = 600000 // Lower time bound for long running tasks (millisecond)
  val LONG_RUNNING_TASK_SKEW_RATIO = 3 // Task duration skew ratio for long running tasks
  var criticalPathResult = CriticalPathResult(List.empty, List.empty, List.empty, "")
  val stageIdsInCriticalPath = new mutable.HashSet[Long]()

  override def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val stageLevelCriticalPath: mutable.HashMap[Long, List[StageTimeSpan]] = mutable.HashMap.empty
    val longRunningTasks: mutable.HashMap[Long, List[TaskTimeSpan]] = mutable.HashMap.empty

    val out = new mutable.StringBuilder()
    out.println("----------------------------------------DEBUG INFO START----------------------------------------")
    out.println(s"Original time span for each job:")
    printTimeSpans(out, ac.jobMap.values.toList.sortBy(_.jobID))
    out.println(s"Original time span for each stage:")
    printTimeSpans(out, ac.jobMap.values.flatMap(jobTimeSpan => jobTimeSpan.stageMap.values).toList.sortBy(_.stageID),
      ac.stageIDToJobID)

    out.println("\nCritical path analysis")
    ac.jobMap.values.toList.sortBy(_.jobID).foreach(jobTimeSpan => {
      val currentJobCriticalPath = findLongestPathTimeSpans(jobTimeSpan.stageMap.values.toList)
      if (currentJobCriticalPath.nonEmpty) {
        out.println(s"Critical path for job ${jobTimeSpan.jobID}:")
        printTimeSpans(out, currentJobCriticalPath)
        currentJobCriticalPath.foreach(stageTimeSpan => stageIdsInCriticalPath.add(stageTimeSpan.stageID))
      }
      stageLevelCriticalPath(jobTimeSpan.jobID) = currentJobCriticalPath
    })

    out.println(
      s"""
         |Long running tasks analysis
         |1) Long running task has duration greater than ${pd(LONG_RUNNING_TASK_DURATION_LOWER_BOUND)}
         |2) Long running task has duration greater than ${LONG_RUNNING_TASK_SKEW_RATIO} * average tasks duration in the stage\n""".stripMargin)
    stageLevelCriticalPath.values.flatten.toList.sortBy(_.stageID).foreach(stageTimeSpan => {
      val currentStageLongRunningTasks = findLongRunningTimeSpans(stageTimeSpan.taskMap.values.toList)
      if (currentStageLongRunningTasks.nonEmpty) {
        out.println(s"Long running tasks for stage ${stageTimeSpan.stageID}:")
        printTimeSpans(out, currentStageLongRunningTasks)
      }
      longRunningTasks(stageTimeSpan.stageID) = currentStageLongRunningTasks
    })

    // Generate critical path result
    val criticalPaths = stageLevelCriticalPath.map(item => {
      val jobID = item._1
      val stagesData = item._2.map(stageTimeSpan =>
        StageData(stageTimeSpan.stageID,
          stageTimeSpan.startTime,
          stageTimeSpan.endTime,
          stageTimeSpan.duration().getOrElse(0L)))
      CriticalPathData(jobID, stagesData)
    }).toList.sortBy(_.jobID)
    val tasks = longRunningTasks.map(item => {
      val stageID = item._1
      val tasksData = item._2.map(taskTimeSpan =>
        TaskData(taskTimeSpan.taskID,
          taskTimeSpan.startTime,
          taskTimeSpan.endTime,
          taskTimeSpan.duration().getOrElse(0L)))
      LongRunningTasksData(stageID, tasksData)
    }).toList.sortBy(_.stageID)

    val criticalPathWithAllStages = ac.jobMap.toList.sortBy(_._1).map(item => {
      val jobId = item._1
      val allStagesInJob = item._2.stageMap.values.toList.sortBy(_.stageID)
      val stagesData = allStagesInJob.map(stageTimeSpan =>
        StageData(stageTimeSpan.stageID,
          stageTimeSpan.startTime,
          stageTimeSpan.endTime,
          stageTimeSpan.duration().getOrElse(0L),
          stageIdsInCriticalPath.contains(stageTimeSpan.stageID.toLong)
        ))
      CriticalPathData(jobId, stagesData)
    }).sortBy(_.jobID)
    out.println("----------------------------------------DEBUG INFO END----------------------------------------")
    criticalPathResult = CriticalPathResult(criticalPaths, criticalPathWithAllStages, tasks, out.toString())

    // We print nothing by default. If user wants to know detailed info, they can call criticalPathResult.debugInfo
    ""
  }

  def findLongestPathTimeSpans[P <: TimeSpan](timeSpans: List[P]): List[P] = {
    val sortByEndTimeSpans = timeSpans.sortWith((a, b) => a.endTime <= b.endTime)
    var currentIndex = sortByEndTimeSpans.length - 1
    val resultStack = new mutable.ArrayStack[P]()
    while (currentIndex >= 0) {
      if (resultStack.isEmpty || atLeft(sortByEndTimeSpans(currentIndex), resultStack.top)) {
        resultStack.push(sortByEndTimeSpans(currentIndex))
      }
      currentIndex = currentIndex - 1
    }
    resultStack.toList // stack.top will become the first element in the list
  }

  def findLongRunningTimeSpans[P <: TimeSpan](timeSpans: List[P]): List[P] = {
    val validTimeSpans = timeSpans.filter(_.duration().isDefined)
    if (validTimeSpans.isEmpty) {
      return List.empty
    }

    val averageDuration = validTimeSpans.map(_.duration().get).sum / (0.0 + validTimeSpans.size)
    validTimeSpans
      .filter(taskTimeSpan => taskTimeSpan.duration().get > LONG_RUNNING_TASK_DURATION_LOWER_BOUND
        && taskTimeSpan.duration().get > LONG_RUNNING_TASK_SKEW_RATIO * averageDuration)
      .sortWith(_.duration().get > _.duration().get)
  }

  /**
   * Print the formatted output for critical path in different level
   *
   * @param timeSpans time spans which constitute the critical path
   */
  def printTimeSpans[P <: TimeSpan](out: mutable.StringBuilder,
                                    timeSpans: List[P],
                                    stageIDToJobID: mutable.HashMap[Int, Long] = new mutable.HashMap[Int, Long]): Unit = {
    timeSpans.foreach(timeSpan => {
      timeSpan match {
        case span: JobTimeSpan =>
          out.print(f"Job ${span.jobID}%3s    ")
        case span: StageTimeSpan => {
          if (!stageIDToJobID.isEmpty) {
            out.print(f"Job ${stageIDToJobID(span.stageID)}%3s    ")
          }
          out.print(f"Stage ${span.stageID}%3s    ")
        }
        case span: TaskTimeSpan => {
          out.print(f"Task ${span.taskID}%5s    ")
        }
      }
      out.println(s"Start time: ${pt(timeSpan.startTime)}    End time: ${pt(timeSpan.endTime)}" +
        s"    Duration: ${pd(timeSpan.duration().getOrElse(0))}")
    })
  }

  def atLeft(firstTime: TimeSpan, secondTime: TimeSpan): Boolean = {
    firstTime.endTime <= secondTime.startTime
  }
}