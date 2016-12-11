/**
 * @author Joy Chakraborty <joychak1@gatech.edu>.
 */

package com.datalogs.main

import java.io.File

import com.datalogs.classification.{metrics, models}
import com.datalogs.datamodel.{Chart, IcuStays, Patient}
import com.datalogs.features.FeatureConstruction.MortalityType
import com.datalogs.features.{FeatureConstruction, FeatureUtils}
import com.datalogs.ioutils.LoadData
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop.ScallopConf

/**
  * Runtime configurations
  * @param args
  */
class RunConf(args: Array[String]) extends ScallopConf(args) {

  val csvPath = opt[String]("csv-dir", required = true)
  val featurePath = opt[String]("feature-dir", required = true)
  val testFeaturePath = opt[String]("test-feature-dir", required = false, default = Some(""))
  val outputPath = opt[String]("output-dir", required = true)
  val stopWordFile = opt[String]("stop-word-file", required = false, default = Some(""))
  val hasNoSaps = opt[Boolean]("no-saps-data", required = false, default = Some(false))
  val hasNoComorbidities = opt[Boolean]("no-comorbidities", required = false, default = Some(false))
  val hasNoEvents = opt[Boolean]("no-event-data", required = false, default = Some(false))
  val hasNoNotes = opt[Boolean]("no-note-text", required = false, default = Some(false))
  val phase = opt[String]("pipeline-stage", required = true).map(_.toInt)
  verify()
}

object Main {

  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new RunConf(args)

    val (sc, spark) = createContext

    conf.phase() match {
      case 0 => {
        createFeatures(spark, conf.csvPath(), conf.featurePath(), conf.testFeaturePath(), conf.stopWordFile(),
          !conf.hasNoSaps(), !conf.hasNoComorbidities(), !conf.hasNoEvents(), !conf.hasNoNotes())
        runMultiModels(spark, conf.featurePath(), conf.testFeaturePath(), conf.outputPath(),
          !conf.hasNoSaps(), !conf.hasNoComorbidities(), !conf.hasNoEvents(), !conf.hasNoNotes())
      }
      case 1 => createFeatures(spark, conf.csvPath(), conf.featurePath(), conf.testFeaturePath(), conf.stopWordFile(),
        !conf.hasNoSaps(), !conf.hasNoComorbidities(), !conf.hasNoEvents(), !conf.hasNoNotes())
      case 2 => runMultiModels(spark, conf.featurePath(), "", conf.outputPath(),
        !conf.hasNoSaps(), !conf.hasNoComorbidities(), !conf.hasNoEvents(), !conf.hasNoNotes())
      case 3 => {
        if (conf.testFeaturePath().isEmpty)
          println("Valid Test feature path is required !!!")
        else
          runMultiModels(spark, conf.featurePath(), conf.testFeaturePath(), conf.outputPath(),
            !conf.hasNoSaps(), !conf.hasNoComorbidities(), !conf.hasNoEvents(), !conf.hasNoNotes())
      }
    }
    sc.stop()
  }

  def createFeatures(spark: SparkSession, inputPath: String, featurePath: String, testFeaturePath: String,
                      stopWordFile: String, hasSaps: Boolean, hasComor: Boolean, hasEvents: Boolean, hasNotes: Boolean): Unit = {
    /** initialize loading of data */
    val (patient, icustays, saps, comorbidities, labResults, diagnostics, medications, notes, testPatientIds)
    = LoadData.loadRddRawData(spark, inputPath, testFeaturePath, hasSaps, hasComor, hasEvents, hasNotes)

    /** Constructing features independent of model timeframe **/
    val baseFeature = FeatureConstruction.constructBaseFeatureTuple(spark, patient)
    println(s"baseFeature count: ${baseFeature.count}")

    val sapsFeature = if (hasSaps) FeatureConstruction.constructSaps2FeatureTuple(spark, patient.rdd, saps) else null
    if (hasSaps) println(s"sapsFeature count: ${sapsFeature.count}")

    val comorbiditiesFeature = if (hasComor)
      FeatureConstruction.constructComorbiditiesFeatureTuple(spark, patient.rdd, comorbidities)
    else
      null
    if (hasComor) println(s"ComorbiditiesFeature count: ${comorbiditiesFeature.count}")

    /** Updating patient index date and label for ICU **/
    println(s"InICU ... ")
    val (patientInICU12Hr, patientInICU24Hr, patientInICU48Hr, patientInICURetro) =
      constructPatient(spark, patient, icustays, FeatureConstruction.InICU())

    /** Updating patient index date and label for 30 day post discharge **/
    println(s"In 30 days ... ")
    val (patientIn30d12Hr, patientIn30d24Hr, patientIn30d48Hr, patientIn30dRetro) =
      constructPatient(spark, patient, icustays, FeatureConstruction.In30Days())

    /** Updating patient index date and label for 1 year post discharge **/
    println(s"In 1 Year ... ")
    val (patientIn1Yr12Hr, patientIn1Yr24Hr, patientIn1Yr48Hr, patientIn1YrRetro) =
      constructPatient(spark, patient, icustays, FeatureConstruction.In1Year())

    /** Constructing notes for ICU stay period intervals **/
    val note12HrFeatures = if (hasNotes) FeatureConstruction.constructNoteFeatureTuple(spark, stopWordFile,
      patientInICU12Hr, notes) else null
    if (hasNotes) println(s"noteFeatures Count: ${note12HrFeatures.count}")

    val note24HrFeatures = if (hasNotes) FeatureConstruction.constructNoteFeatureTuple(spark, stopWordFile,
      patientInICU24Hr, notes) else null
    if (hasNotes) println(s"noteFeatures Count: ${note24HrFeatures.count}")

    val note48HrFeatures = if (hasNotes) FeatureConstruction.constructNoteFeatureTuple(spark, stopWordFile,
      patientInICU48Hr, notes) else null
    if (hasNotes)println(s"noteFeatures Count: ${note48HrFeatures.count}")

    val noteRetroFeatures = if (hasNotes) FeatureConstruction.constructNoteFeatureTuple(spark, stopWordFile,
      patientInICURetro, notes) else null
    if (hasNotes) println(s"noteFeatures Count: ${noteRetroFeatures.count}")
    //noteRetroFeatures.take(100).foreach(x => println(s"Patient: ${x._1._1}, Feature: ${x._1._2}, Value: ${x._2}"))

    /** Construct the dynamic feature and merge it with baseline and comorbidities **/

    /** In ICU **/
    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_inICU12Hr",
      stopWordFile, patientInICU12Hr, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, note12HrFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_inICU24Hr",
      stopWordFile, patientInICU24Hr, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, note24HrFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_inICU48Hr",
      stopWordFile, patientInICU48Hr, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, note48HrFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_inICURetro",
      stopWordFile, patientInICURetro, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, noteRetroFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    /** In 30 days post discharge **/
    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_in30d12Hr",
      stopWordFile, patientIn30d12Hr, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, note12HrFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_in30d24Hr",
      stopWordFile, patientIn30d24Hr, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, note24HrFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_in30d48Hr",
      stopWordFile, patientIn30d48Hr, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, note48HrFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_in30dRetro",
      stopWordFile, patientIn30dRetro, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, noteRetroFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    /** In 1 year post discharge **/
    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_in1Yr12Hr",
      stopWordFile, patientIn1Yr12Hr, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, note12HrFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_in1Yr24Hr",
      stopWordFile, patientIn1Yr24Hr, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, note24HrFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_in1Yr48Hr",
      stopWordFile, patientIn1Yr48Hr, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, note48HrFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)

    FeatureUtils.combineFeatures(spark, featurePath, testFeaturePath, "/svlight_in1YrRetro",
      stopWordFile, patientIn1YrRetro, testPatientIds,
      baseFeature, sapsFeature, comorbiditiesFeature, noteRetroFeatures, diagnostics, medications, labResults,
      hasSaps, hasComor, hasEvents, hasNotes)
  }

  def constructPatient(spark: SparkSession, patient: Dataset[Patient], icustays: Dataset[IcuStays],
                       mortType: MortalityType) : (RDD[Patient], RDD[Patient], RDD[Patient], RDD[Patient]) = {

    val patient12Hr = FeatureConstruction.constructPatientBasedOnMortalityPeriod(
      spark, patient, icustays, mortType, 12)
    println(s"12 Hr count: ${patient12Hr.filter(x=> x.isDead==1.0).count}")

    val patient24Hr = FeatureConstruction.constructPatientBasedOnMortalityPeriod(
      spark, patient, icustays, mortType, 24)
    println(s"24 Hr count: ${patient24Hr.filter(x=> x.isDead==1.0).count}")

    val patient48Hr = FeatureConstruction.constructPatientBasedOnMortalityPeriod(
      spark, patient, icustays, mortType, 48)
    println(s"48 Hr count: ${patient48Hr.filter(x=> x.isDead==1.0).count}")

    val patientRetro = FeatureConstruction.constructPatientBasedOnMortalityPeriod(
      spark, patient, icustays, mortType, 0)
    println(s"Retro count: ${patientRetro.filter(x=> x.isDead==1.0).count}")

    (patient12Hr, patient24Hr, patient48Hr, patientRetro)
  }

  def runMultiModels(spark: SparkSession, featurePath: String, testFeatutePath: String, outputPath: String,
                     hasSaps: Boolean, hasComor: Boolean, hasEvents: Boolean, hasNotes: Boolean): Unit = {

    /** In ICU **/
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "inICU12Hr", hasSaps, hasComor, hasEvents, hasNotes)
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "inICU24Hr", hasSaps, hasComor, hasEvents, hasNotes)
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "inICU48Hr", hasSaps, hasComor, hasEvents, hasNotes)
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "inICURetro", hasSaps, hasComor, hasEvents, hasNotes)

    /** In 30 days post discharge **/
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "in30d12Hr", hasSaps, hasComor, hasEvents, hasNotes)
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "in30d24Hr", hasSaps, hasComor, hasEvents, hasNotes)
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "in30d48Hr", hasSaps, hasComor, hasEvents, hasNotes)
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "in30dRetro", hasSaps, hasComor, hasEvents, hasNotes)

    /** In 1 year post discharge **/
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "in1Yr12Hr", hasSaps, hasComor, hasEvents, hasNotes)
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "in1Yr24Hr", hasSaps, hasComor, hasEvents, hasNotes)
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "in1Yr48Hr", hasSaps, hasComor, hasEvents, hasNotes)
    runMortalityModels(spark, featurePath, testFeatutePath, outputPath, "in1YrRetro", hasSaps, hasComor, hasEvents, hasNotes)

  }

  def runMortalityModels(spark: SparkSession, featurePath: String, testFeatutePath: String, outputPath: String,
                         modelLabel: String, hasSaps: Boolean, hasComor: Boolean, hasEvents: Boolean, hasNotes: Boolean) = {

    runOneModel(spark, "Running svlight_" + modelLabel + "_baseline ....",
      featurePath,  testFeatutePath, outputPath,  "svlight_" + modelLabel + "_baseline",
      modelLabel + " with baseline features - aucROC")

    if (hasComor) {
      runOneModel(spark, "Running svlight_" + modelLabel + "_baseline_comorbidities ....",
        featurePath, testFeatutePath, outputPath, "svlight_" + modelLabel + "_baseline_comorbidities",
        modelLabel + " with baseline + comorbidities features - aucROC")
    }

    if (hasEvents) {
      runOneModel(spark, "Running svlight_" + modelLabel + "_baseline_comorbidities_dynamic ....",
        featurePath, testFeatutePath, outputPath, "svlight_" + modelLabel + "_baseline_comorbidities_dynamic",
        modelLabel + " with baseline + comorbidities + dynamic - aucROC")
    }

    if (hasNotes) {
      runOneModel(spark, "Running svlight_" + modelLabel + "_baseline_comorbidities_notes ....",
        featurePath, testFeatutePath, outputPath, "svlight_" + modelLabel + "_baseline_comorbidities_notes",
        modelLabel + " with baseline + comorbidities + notes features - aucROC")
    }

    if (hasEvents && hasNotes) {
      runOneModel(spark, "Running svlight_" + modelLabel + "_all_features ....",
        featurePath, testFeatutePath, outputPath, "svlight_" + modelLabel + "_all_features",
        modelLabel + " with all features - aucROC")
    }
  }

  def runOneModel(spark: SparkSession, msgToPrint: String, featurePath: String, testFeatutePath: String, outputPath: String,
                  modelLabel: String, plotLabel: String): Unit = {

    import spark.sqlContext.implicits._

    if (testFeatutePath.isEmpty) {
      val results = models.run(spark, featurePath + "/" + modelLabel + "/part-*")
      val (aucROC, roc) = metrics.getMetrics(results)

      roc.saveAsTextFile(outputPath + "/" + modelLabel)
      //results.foreach { case (score, label) => if (label == 1) println(f"label: ${label}; score: $score%1.12f") }

      println("======================")
      println(msgToPrint)
      println("Area under ROC = " + aucROC)
      println("**********************")
      //    println("Accuracy       = " + accuracy)
      //    println("Precision      = " + precision)
      //    println("fMeasure       = " + fMeasure)

      //PlotUtils.plot(roc, aucROC, plotLabel + s": ${"%.3f".format(aucROC)}")
    }
    else {
      val results = models.run(spark, featurePath + "/" + modelLabel + "/part-*",
        testFeatutePath + "/" + modelLabel + "/part-*")

      FileUtils.deleteQuietly(new File(outputPath + "/" + modelLabel))

      results
        .map(x => (x._2.toInt, x._1))
        .sortBy(x => x._1)
        .toDF("SUBJECT_ID","InHospital_Expiry")
        .write.format("com.databricks.spark.csv").save(outputPath + "/" + modelLabel)
    }
  }

  def createContext: (SparkContext, SparkSession) = {
    val conf = new SparkConf()  //.setAppName(appName).setMaster(masterUrl).set("spark.executor.memory", "2g")
    (new SparkContext(conf), SparkSession.builder().config(conf).getOrCreate())
  }
}
