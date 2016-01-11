package com.datastax.spark.connector

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class RowPairRDDFunctions[K, V](rdd: PairRDDFunctions[K, Iterable[CassandraRow]]) extends Serializable {

  val sparkContext: SparkContext = rdd.sparkContext

  def joinWithPairedCassandraTable[KX](
    keyspaceName: String,
    tableName: String,
    selectedColumns: ColumnSelector = AllColumns,
    joinColumns: ColumnSelector = PartitionKeyColumns,
    keyFun: CassandraRow => KX): RDD[(K, (Iterable[(KX, (CassandraRow, CassandraRow))]))] = {

    rdd
      .joinWithSpannedCassandraTable(keyspaceName, tableName, selectedColumns, joinColumns)
      .map {
        case (key, (rows_left, rows_right)) => (key, join(rows_left, rows_right, keyFun))
      }
  }

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
