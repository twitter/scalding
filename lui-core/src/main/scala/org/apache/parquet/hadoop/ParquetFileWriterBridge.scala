package org.apache.parquet.hadoop

import java.util.{ List => JList }
object ParquetFileWriterBridge {
  def getGlobalMetaData(footers: JList[Footer], strict: Boolean = true) =
    ParquetFileWriter.getGlobalMetaData(footers, strict)
}