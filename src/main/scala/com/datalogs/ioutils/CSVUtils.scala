/**
  * @author Joy Chakraborty <joychak1@gatech.edu>.
  */

package com.datalogs.ioutils

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD


object CSVUtils {

  def loadAnyCSVAsTable(spark: SparkSession, path: String, tableName: String): DataFrame = {
    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      //.option("inferSchema", "true")
      .option("parserLib", "UNIVOCITY") // <-- This is the configuration that solved the issue.
      .load(path)

    data.registerTempTable(tableName)
    data
  }

  def loadAnyCSVAsTable(spark: SparkSession, path: String): DataFrame = {
    loadAnyCSVAsTable(spark, path, inferTableNameFromPath(path))
  }

  private val pattern = "(\\w+)(\\.csv)?$".r.unanchored
  def inferTableNameFromPath(path: String) = path match {
    case pattern(filename, extension) => filename
    case _ => path
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  def saveAsSingleLocalFile(rdd: RDD[String], outFile: String) = {

    val file = "/tmp/sparkTempFile"

    FileUtil.fullyDelete(new File(file))
    rdd.saveAsTextFile(file)

    FileUtils.deleteQuietly(new File(outFile.replaceFirst("file:", "")))
    merge(file, outFile)

    FileUtil.fullyDelete(new File(file))
  }
}

