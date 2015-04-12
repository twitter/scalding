package com.twitter.scalding

import java.util

import scala.collection.mutable.ArrayBuffer

object StringUtility {
  private def fastSplitHelper(text: String, key: String, from: Int, textLength: Int): List[String] = {
    val firstIndex = text.indexOf(key, from)
    if (firstIndex == -1) {
      if (from < textLength) {
        List(text.substring(from))
      } else {
        List()
      }
    } else {
      // the text till the separator should be kept in any case
      text.substring(from, firstIndex) :: fastSplitHelper(text, key, firstIndex + 1, textLength)
    }
  }

  def fastSplit(text: String, key: String): Seq[String] = {
    fastSplitHelper(text, key, 0, text.length)
  }
}