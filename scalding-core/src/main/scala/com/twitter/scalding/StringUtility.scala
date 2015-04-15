package com.twitter.scalding

object StringUtility {
  private def fastSplitHelper(text: String, key: String, from: Int, textLength: Int, keyLength: Int): List[String] = {
    val firstIndex = text.indexOf(key, from)
    if (firstIndex == -1) {
      if (from < textLength) {
        List(text.substring(from))
      } else {
        List("")
      }
    } else {
      // the text till the separator should be kept in any case
      text.substring(from, firstIndex) :: fastSplitHelper(text, key, firstIndex + keyLength, textLength, keyLength)
    }
  }

  def fastSplit(text: String, key: String): List[String] = {
    fastSplitHelper(text, key, 0, text.length, key.length)
  }
}