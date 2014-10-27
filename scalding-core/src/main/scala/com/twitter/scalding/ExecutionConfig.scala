/*
Copyright 2014 Twitter, Inc.

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

import cascading.flow.{ FlowListener, FlowStepListener }

/*
 * This trait is used to hold configuration options for cascading,
 * for example to add observability to planned Flows via FlowListener.
 * This config is implicitly available to type of Execution.
 */
trait ExecutionConfig {
  def listeners: List[FlowListener]
  def stepListeners: List[FlowStepListener]
}

object ExecutionConfig {
  implicit val default = new ExecutionConfig {
    override val listeners = List.empty
    override val stepListeners = List.empty
  }

  def apply(l: List[FlowListener] = List.empty, sl: List[FlowStepListener] = List.empty) = new ExecutionConfig {
    override val listeners = l
    override val stepListeners = sl
  }
}