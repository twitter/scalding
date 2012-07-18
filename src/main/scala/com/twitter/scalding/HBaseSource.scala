package com.twitter.scalding

import cascading.tap.Tap
import cascading.tuple.Fields
import com.twitter.maple.hbase.HBaseScheme
import com.twitter.maple.hbase.HBaseTap
import cascading.tap.SinkMode
import cascading.scheme.Scheme
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.hbase.util.Bytes


abstract class HBaseSource extends Source {
  val quorumNames: String = "localhost"
  val tableName: String
  
  override def createTap(readOrWrite : AccessMode)(implicit mode : Mode) : Tap[_,_,_] = {
    val hBaseScheme = hdfsScheme match {
      case hbase: HBaseScheme=> hbase
      case _ => throw new ClassCastException("Failed casting from Scheme to HBaseScheme")
    }
    mode match {
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read => {
          new HBaseTap(quorumNames, tableName, hBaseScheme, SinkMode.KEEP)
        }
        case Write => {
          new HBaseTap(quorumNames, tableName, hBaseScheme, SinkMode.UPDATE)
        }
      }
      case _ => super.createTap(readOrWrite)(mode) 
    }
  }
}


trait HBaseSchemeSource extends Source {
  val keyFields: Fields
  val familyNames: Array[String]
  val valueFields: Array[Fields]
  override val hdfsScheme = new HBaseScheme(keyFields, familyNames, valueFields)
  .asInstanceOf[Scheme[JobConf,RecordReader[_,_],OutputCollector[_,_],_,_]]
}


class HBaseTable(override val quorumNames: String, 
    override val tableName: String, 
    override val keyFields: Fields, 
    override val familyNames: Array[String], 
    override val valueFields: Array[Fields]) 
extends HBaseSource with HBaseSchemeSource

