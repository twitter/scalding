/*
Copyright 2015 Twitter, Inc.

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
package com.twitter.scalding.typed

/**
 * Used for objects that may have a description set to be used in .dot and MR step names.
 */
trait HasDescription {
  def descriptions: Seq[String]
}

/**
 * Used for objects that may _set_ a description to be used in .dot and MR step names.
 */
trait WithDescription[+This <: WithDescription[This]] extends HasDescription {
  /** never mutates this, instead returns a new item. */
  def withDescription(description: String): This
}
