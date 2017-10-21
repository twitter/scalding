/*
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
package com.twitter.scalding.serialization

// We wrap types in Exported to provide low priority implicits
// the real type has a low priority implicit to extract from Exported
// into the original type.
// See more @ https://github.com/milessabin/export-hook
case class Exported[T](instance: T) extends AnyVal
