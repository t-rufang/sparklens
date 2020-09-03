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

package com.qubole.sparklens.helper

import com.qubole.sparklens.analyzer.{CriticalPathData, CriticalPathResult}

object CriticalPathResultHelper {
  def getCriticalPathJsonString(CriticalPathResult: CriticalPathResult): String = {
    JsonHelper.convertScalaObjectToJsonString(CriticalPathResult.criticalPath)
  }

  def getCriticalPathWithAllStagesJsonString(criticalPathResult: CriticalPathResult): String = {
    JsonHelper.convertScalaObjectToJsonString(criticalPathResult.criticalPathWithAllStages)
  }

  def getCriticalPathJsonStringWithoutShortStage(CriticalPathResult: CriticalPathResult,
                                                 durationThreshold: Option[Double] = Option.empty): String = {
    JsonHelper.convertScalaObjectToJsonString(
      getCriticalPathWithoutShortStage(CriticalPathResult.criticalPath, durationThreshold))
  }

  def getLongRunningTasksJsonString(CriticalPathResult: CriticalPathResult): String = {
    JsonHelper.convertScalaObjectToJsonString(CriticalPathResult.tasks)
  }

  /**
   * Filter stages whose duration is shorter than specific value. If all the stages in a job is filtered out,
   * we will remove the job as well.
   * If durationThreshold is specified, we will filter out all the stage whose duration is shorter than this value.
   * Otherwise, we will filter out all the stages whose duration is shorter than average stage duration
   * than this value.
   * @param criticalPaths critical path data of a Spark application
   * @param durationThreshold threshold that used to filter out short stages. If durationThreshold is 10, we will filter
   *                          out all the stages whose duration is shorter than 10 minutes
   * @return filtered critical path
   */
  def getCriticalPathWithoutShortStage(criticalPaths: List[CriticalPathData],
                                       durationThreshold: Option[Double] = Option.empty): List[CriticalPathData] = {
    val allStages = criticalPaths.flatMap(criticalPathData => criticalPathData.stagesData)
    val averageStageDuration = allStages.map(_.duration).sum / (0.0 + allStages.size)
    val filterThreshold = if (durationThreshold.isDefined) durationThreshold.get * 60000 else averageStageDuration

    criticalPaths.map(criticalPathData => {
      val jobID = criticalPathData.jobID
      val filteredStageData = criticalPathData.stagesData.filter(_.duration >= filterThreshold)
      CriticalPathData(jobID, filteredStageData)
    }).filter(criticalPathData => criticalPathData.stagesData.nonEmpty)
  }
}
