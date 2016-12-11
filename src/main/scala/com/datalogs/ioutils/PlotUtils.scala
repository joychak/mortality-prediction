//package com.datalogs
//
//import org.apache.spark.rdd.RDD
//import org.sameersingh.scalaplot.jfreegraph.JFGraphPlotter
//import org.sameersingh.scalaplot.{MemXYSeries, XYChart, XYData}
//
///**
//  * Created by joychak on 11/19/16.
//  */
//object PlotUtils {
//
//  def plot(roc: RDD[(Double, Double)], auROC: Double, label: String): Unit = {
//
//    val xs = roc.map(_._1).collect()
//    val ys = roc.map(_._2).collect()
//
//    val series = new MemXYSeries(xs, ys)
//    val data = new XYData(series)
//    val chart = new XYChart(label, data)
//    val plotter = new JFGraphPlotter(chart)
//    plotter.gui()
//  }
//}
