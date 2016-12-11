/**
  * @author Joy Chakraborty <joychak1@gatech.edu>.
  */

package com.datalogs.classification

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, Normalizer => MLNormalizer, StandardScaler => MLStandardScaler}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.feature.{Normalizer, StandardScaler}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by joychak on 11/19/16.
  */
object models {

  /**
    * Run with 70-30 % split of the input dataset
    * @param spark
    * @param path
    * @return
    */
  def run(spark: SparkSession, path: String): RDD[(Double, Double)] = {

    val data = MLUtils.loadLibSVMFile(spark.sparkContext, path)

    val scaler = new StandardScaler().fit(data.map(x => x.features))
    val normalizer = new Normalizer()

    val scaledData = data
      .map(x => (x.label, scaler.transform(x.features)))
      //.map(x => (x._1, normalizer.transform(x._2)))
      .map(x => LabeledPoint(x._1, x._2))

    val splits = scaledData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    runSVM(training, test)
    //runLR(training, test)
  }

  /**
    * Run with different set of data for training and testing purpose
    * @param spark
    * @param trainingPath
    * @param testPath
    * @return
    */
  def run(spark: SparkSession, trainingPath: String, testPath: String): RDD[(Double, Double)] = {

    val trainingData = MLUtils.loadLibSVMFile(spark.sparkContext, trainingPath)
    val testingData = MLUtils.loadLibSVMFile(spark.sparkContext, testPath)

    //val normalizer = new Normalizer()

    val trainingScaler = new StandardScaler().fit(trainingData.map(x => x.features))
    val testingScaler = new StandardScaler().fit(testingData.map(x => x.features))

    val training = trainingData
      .map(x => (x.label, trainingScaler.transform(x.features)))
      //.map(x => (x._1, normalizer.transform(x._2)))
      .map(x => LabeledPoint(x._1, x._2))

    val test = testingData
      .map(x => (x.label, testingScaler.transform(x.features)))
      //.map(x => (x._1, normalizer.transform(x._2)))
      .map(x => LabeledPoint(x._1, x._2))

    //runSVM(training, test)
    runLR(training, test)
  }

  /**
    * Run SVM
    * @param training
    * @param test
    * @return
    */
  def runSVM(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): RDD[(Double, Double)]  = {
    // Run training algorithm to build the model
    val numIterations = 1000
    val model = SVMWithSGD
      .train(training, numIterations)
    //.setThreshold(0.5)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      //println(s"label: ${point.label}; score: $score%1.5f")
      (score, point.label)
    }
    scoreAndLabels
  }

  /**
    * Run Logistic regression
    * @param training
    * @param test
    * @return
    */
  def runLR(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): RDD[(Double, Double)]  = {

    val lr = new LogisticRegressionWithLBFGS()
    lr.optimizer.setNumIterations(10000).setRegParam(0.0001)

    val model = lr
      .setNumClasses(2)
      .run(training)
      .clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      //println(f"label: ${point.label}; score: $score%1.5f")
      (score, point.label)
    }
    scoreAndLabels
  }

  /** Other classification algorithms **/

  def runDT(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): RDD[(Double, Double)]  = {

    // Run training algorithm to build the model
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    scoreAndLabels
  }

  def runRF(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): RDD[(Double, Double)]  = {

    // Run training algorithm to build the model
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 10 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 10
    val maxBins = 32

    val model = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    scoreAndLabels
  }

  def runSparkml(spark: SparkSession, path: String): Double = {

    val rawData = spark.read.format("libsvm").load(path)

    val scaler = new MLStandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)
    val scalerModel = scaler.fit(rawData)
    val scaledData = scalerModel.transform(rawData)

    val normalizer = new MLNormalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)
   val normalizedData = normalizer.transform(scaledData)

    val splits = normalizedData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(normalizedData)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      //.setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(normalizedData)

    val lor = new LogisticRegression()
      .setFeaturesCol("indexedFeatures")
      .setLabelCol("indexedLabel")
      .setRegParam(0.001)
      .setElasticNetParam(0.0)
      .setMaxIter(100)
      .setTol(1E-6)
      .setFitIntercept(true)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lor, labelConverter))

    val model = pipeline.fit(training)

    // Make predictions.
    val predictions = model.transform(test)

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")
    val auROC = evaluator.evaluate(predictions)

    auROC
    //println(s"Area under ROC = ${auROC}")

//    val paramGrid = new ParamGridBuilder()
//      .addGrid(lor.regParam, Array(0.1, 0.01, 0.001, 0.0001))
//      .build()

//    val cv = new CrossValidator()
//      .setEstimator(pipeline)
//      .setEvaluator(evaluator)
//      .setEstimatorParamMaps(paramGrid)
//      .setNumFolds(5)
//
//
//    val cvModel = cv.fit(training)
//
//    val total = cvModel.transform(test)
//      .select("prediction").rdd
//      .map{case Row(prediction: Double) => prediction}
//      .collect()
//
//    val totalVal = total.sum
//    val totalCount = total.length

//    println(s"test count: ${test.count}")
//
//    println(s"totalVal: ${totalVal}, totalCount: ${totalCount}")
//    println(s"Area under ROC = ${totalVal/totalCount}")

  }
}
