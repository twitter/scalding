/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Prop._

// Be careful here in that Array[String] equality isn't contents based. its java referenced based.
object ExecutionAppProperties extends Properties("ExecutionApp Properties") {
  def debugPrint(inputArgs: Array[String], resultingHadoop: HadoopArgs, resultingNonHadoop: NonHadoopArgs): Unit = {
    val errorMsg = "Input Args: " + inputArgs.map("\"" + _ + "\"").mkString(",") + "\n" +
      "Hadoop Args: " + resultingHadoop.toArray.mkString(",") + "\n" +
      "Non-Hadoop Args: " + resultingNonHadoop.toArray.mkString(",") + "\n"
    sys.error(errorMsg)
  }

  property("Non-hadoop random args will all end up in the right bucket") = forAll { (args: Array[String]) =>
    val (hadoopArgs, nonHadoop) = ExecutionApp.extractUserHadoopArgs(args)
    val res = hadoopArgs.toArray.isEmpty && nonHadoop.toArray.sameElements(args)
    if (!res) debugPrint(args, hadoopArgs, nonHadoop)
    res
  }

  property("adding an hadoop lib jars in the middle will extract it right") = forAll { (leftArgs: Array[String], rightArgs: Array[String]) =>
    // in the process of validating the hadoop args we give this to generic options parser
    // as a result this file must exist. the parser enforces this.
    val inputHadoopArgs = Array("-libjars", "/etc/hosts")
    val totalArgStr = leftArgs ++ inputHadoopArgs ++ rightArgs
    val (hadoopArgs, nonHadoop) = ExecutionApp.extractUserHadoopArgs(totalArgStr)
    val res = (!hadoopArgs.toArray.isEmpty) &&
      (nonHadoop.toArray.sameElements(leftArgs ++ rightArgs)) &&
      (inputHadoopArgs.sameElements(hadoopArgs.toArray))
    if (!res) debugPrint(totalArgStr, hadoopArgs, nonHadoop)
    res
  }

  property("adding an hadoop -D parameter in the middle will extract it right") = forAll { (leftArgs: Array[String], rightArgs: Array[String]) =>
    val inputHadoopArgs = Array("-Dx.y.z=123")
    val totalArgStr = leftArgs ++ inputHadoopArgs ++ rightArgs
    val (hadoopArgs, nonHadoop) = ExecutionApp.extractUserHadoopArgs(totalArgStr)
    val res = (!hadoopArgs.toArray.isEmpty) &&
      (nonHadoop.toArray.sameElements(leftArgs ++ rightArgs)) &&
      (inputHadoopArgs.sameElements(hadoopArgs.toArray))
    if (!res) debugPrint(totalArgStr, hadoopArgs, nonHadoop)
    res
  }
}
