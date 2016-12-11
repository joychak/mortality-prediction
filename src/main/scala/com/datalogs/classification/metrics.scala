/**
  * @author Joy Chakraborty <joychak1@gatech.edu>.
  */

package com.datalogs.classification

import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics}
import org.apache.spark.rdd.RDD

/**
  * Created by joychak on 11/19/16.
  */
object metrics {

  def getMetrics(scoreAndLabels: RDD[(Double, Double)]): (Double, RDD[(Double, Double)]) = {
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    val roc = metrics.roc()

//    val otherMetrics = new MulticlassMetrics(scoreAndLabels)
//    val accuracy = otherMetrics.accuracy
//    val precision = otherMetrics.precision
//    val fMeasure = otherMetrics.fMeasure

    (auROC, roc)
  }


}
