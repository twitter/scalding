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
package com.twitter.scalding.serialization;

public class Undeprecated {
  /**
   * This method is faster for ASCII data, but unsafe otherwise
   * it is used by our macros AFTER checking that the string is ASCII
   * following a pattern seen in Kryo, which benchmarking showed helped.
   * Scala cannot supress warnings like this so we do it here
   */
  @SuppressWarnings("deprecation")
  public static void getAsciiBytes(String element, int charStart, int charLen, byte[] bytes, int byteOffset) {
    element.getBytes(charStart, charLen, bytes, byteOffset);
  }
}
