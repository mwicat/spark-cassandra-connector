package com.datastax.spark.connector

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.{CassandraSpannedJoinRDD, ValidRDDType}
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object RowPairRDDFunctions {
  def join[KX](
      left: Iterable[CassandraRow],
      right: Iterable[CassandraRow],
      keyFun: CassandraRow => KX): Iterable[(KX, (CassandraRow, CassandraRow))] = {
    val hash = right.groupBy(keyFun) withDefaultValue Seq()
    left.flatMap { row =>
      val key = keyFun(row)
      hash(key).map(right_row => (key, (row, right_row)))
    }
  }
}


class RowPairRDDFunctions[K, V](rdd: RDD[(K, Iterable[CassandraRow])]) extends Serializable {

  val sparkContext: SparkContext = rdd.sparkContext

  def joinWithPairedCassandraTable[KX](
    keyspaceName: String,
    tableName: String,
    keyFun: CassandraRow => KX,
    selectedColumns: ColumnSelector = AllColumns,
    joinColumns: ColumnSelector = PartitionKeyColumns)(
    implicit
    connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
    newType: ClassTag[CassandraRow],
    rrf: RowReaderFactory[CassandraRow],
    ev: ValidRDDType[CassandraRow],
    keyType: ClassTag[K],
    valueType: ClassTag[Iterable[CassandraRow]],
    currentType: ClassTag[(K,Iterable[CassandraRow])],
    rwf: RowWriterFactory[K]): RDD[(K, (Iterable[(KX, (CassandraRow, CassandraRow))]))] = {

    val span_rdd = new CassandraSpannedJoinRDD[K,Iterable[CassandraRow],CassandraRow](rdd, keyspaceName, tableName, connector, columnNames = selectedColumns, joinColumns = joinColumns)

    span_rdd.map {
        case (key, (rows_left, rows_right)) => (key, RowPairRDDFunctions.join(rows_left, rows_right, keyFun))
    }
  }

}
