package com.datastax.spark.connector

import com.datastax.spark.connector.rdd.SpannedByKeyRDD

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.rdd.{CassandraSpannedJoinRDD, ValidRDDType}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector.writer._

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


class PairRDDFunctions[K, V](rdd: RDD[(K, V)]) extends Serializable {

  val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Groups items with the same key, assuming the items with the same key are next to each other
   * in the collection. It does not perform shuffle, therefore it is much faster than using
   * much more universal Spark RDD `groupByKey`. For this method to be useful with Cassandra tables,
   * the key must represent a prefix of the primary key, containing at least the partition key of the
   * Cassandra table. */
  def spanByKey: RDD[(K, Seq[V])] =
    new SpannedByKeyRDD[K, V](rdd)

  def joinWithSpannedCassandraTable[R](
    keyspaceName: String,
    tableName: String,
    selectedColumns: ColumnSelector = AllColumns,
    joinColumns: ColumnSelector = PartitionKeyColumns)(
  implicit
    connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
    newType: ClassTag[R],
    rrf: RowReaderFactory[R],
    ev: ValidRDDType[R],
    keyType: ClassTag[K],
    valueType: ClassTag[V],
    currentType: ClassTag[(K,V)],
    rwf: RowWriterFactory[V]): CassandraSpannedJoinRDD[K,V,R] = {

    new CassandraSpannedJoinRDD[K,V,R](rdd, keyspaceName, tableName, connector, columnNames = selectedColumns, joinColumns = joinColumns)
  }

}
