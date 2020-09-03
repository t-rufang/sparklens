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

import com.fasterxml.jackson.core.{JsonFactory, JsonProcessingException}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.exception.ExceptionUtils

object JsonHelper {
  val jsonFactory = new JsonFactory()
  val objectMapper = new ObjectMapper(jsonFactory)
  objectMapper.registerModule(DefaultScalaModule)

  def convertScalaObjectToJsonString(obj: Any): String = {
    try {
      objectMapper.writeValueAsString(obj)
    } catch {
      case e: Throwable => {
        println(s"Error when convert scala object to json string. ${ExceptionUtils.getStackTrace(e)}")
        ""
      }
    }
  }
}