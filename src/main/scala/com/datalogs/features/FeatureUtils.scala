/**
  * @author Joy Chakraborty <joychak1@gatech.edu>.
  */

package com.datalogs.features

import java.io.File

import com.datalogs.datamodel._
import com.datalogs.features.FeatureConstruction.FeatureTuple
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by joychak on 11/30/16.
  */
object FeatureUtils {

  def combineFeatures(spark: SparkSession,
                      featureDir: String, testFeatureDir: String, pathLabel: String, stopWordFile: String,
                      patient: RDD[Patient], testPatientIds: Dataset[PatientId],
                      baseFeature: RDD[((String, String), Double)],
                      sapsFeature: RDD[((String, String), Double)],
                      comorbiditiesFeature: RDD[((String, String), Double)],
                      noteFeatures: RDD[((String, String), Double)],
                      diagnostics: Dataset[Diagnostic],
                      medications: Dataset[Medication],
                      labResults: Dataset[LabResult],
                      hasSaps: Boolean, hasComor: Boolean, hasEvents: Boolean, hasNotes: Boolean): Unit = {

    val featurePath = featureDir + pathLabel
    val testFeaturePath = if (testFeatureDir.isEmpty) "" else testFeatureDir + pathLabel

    val diagnosticFeature = if (hasEvents)
      FeatureConstruction.constructDiagnosticFeatureTuple(spark, patient, diagnostics) else null
    if (hasEvents) println(s"diagnosticFeature count: ${diagnosticFeature.count}")

    val medicationFeature = if (hasEvents)
      FeatureConstruction.constructMedicationFeatureTuple(spark, patient, medications) else null
    if (hasEvents) println(s"medicationFeature count: ${medicationFeature.count}")

    val labResultFeature = if (hasEvents)
      FeatureConstruction.constructLabFeatureTuple(spark, patient, labResults) else null
    if (hasEvents) println(s"labResultFeature count: ${labResultFeature.count}")

    //    val noteFeatures = FeatureConstruction.constructNoteFeatureTuple(spark, stopWordFile, patient, notes)
    //    println(s"noteFeatures Count: ${noteFeatures.count}")

    val baseline = if (hasSaps) baseFeature.union(sapsFeature) else baseFeature
    val dynamic = if (hasEvents) diagnosticFeature.union(medicationFeature).union(labResultFeature) else null

    val baselinePlusComorbidities = if (hasComor) baseline.union(comorbiditiesFeature) else baseline
    val baselinePlusComorbiditiesPlusDynamic = if (hasEvents)
      baselinePlusComorbidities.union(dynamic) else baselinePlusComorbidities

    val baselinePlusComorbiditiesPlusNotes = if (hasNotes)
      baselinePlusComorbidities.union(noteFeatures) else baselinePlusComorbidities
    val allFeatures = if (hasNotes)
      baselinePlusComorbiditiesPlusDynamic.union(noteFeatures) else baselinePlusComorbiditiesPlusDynamic

    val baselinePath = featurePath + "_baseline"
    val testBaselinePath = if (testFeaturePath.isEmpty) "" else testFeaturePath + "_baseline"
    println(s"${baselinePath} is starting !!!")
    FileUtils.deleteQuietly(new File(baselinePath))
    constructFeatuteVector(spark.sparkContext, patient, baseline, baselinePath,
      testPatientIds, testBaselinePath)
    println(s"${baselinePath} is finished !!!")

    if (hasComor) {
      val baselineComobiditiesPath = featurePath + "_baseline_comorbidities"
      val testBaselineComobiditiesPath = if (testFeaturePath.isEmpty) "" else testFeaturePath + "_baseline_comorbidities"
      println(s"${baselineComobiditiesPath} is starting !!!")
      FileUtils.deleteQuietly(new File(baselineComobiditiesPath))
      constructFeatuteVector(spark.sparkContext, patient, baselinePlusComorbidities, baselineComobiditiesPath,
        testPatientIds, testBaselineComobiditiesPath)
      println(s"${baselineComobiditiesPath} is finished !!!")
    }

    if (hasEvents) {
      val baselineComobiditiesDynamicPath = featurePath + "_baseline_comorbidities_dynamic"
      val testBaselineComobiditiesDynamicPath = if (testFeaturePath.isEmpty) "" else testFeaturePath + "_baseline_comorbidities_dynamic"
      println(s"${baselineComobiditiesDynamicPath} is staring !!!")
      FileUtils.deleteQuietly(new File(baselineComobiditiesDynamicPath))
      constructFeatuteVector(spark.sparkContext, patient, baselinePlusComorbiditiesPlusDynamic,
        baselineComobiditiesDynamicPath, testPatientIds, testBaselineComobiditiesDynamicPath)
      println(s"${baselineComobiditiesDynamicPath} is finished !!!")
    }

    if (hasNotes) {
      val baselineComorbiditiesNotesPath = featurePath + "_baseline_comorbidities_notes"
      val testBaselineComorbiditiesNotesPath = if (testFeaturePath.isEmpty) "" else testFeaturePath + "_baseline_comorbidities_notes"
      println(s"${baselineComorbiditiesNotesPath} is starting !!!")
      FileUtils.deleteQuietly(new File(baselineComorbiditiesNotesPath))
      constructFeatuteVector(spark.sparkContext, patient, baselinePlusComorbiditiesPlusNotes,
        baselineComorbiditiesNotesPath, testPatientIds, testBaselineComorbiditiesNotesPath)
      println(s"${baselineComorbiditiesNotesPath} is finished !!!")
    }

    if (hasEvents && hasNotes) {
      val allFeaturePath = featurePath + "_all_features"
      val testAllFeaturePath = if (testFeaturePath.isEmpty) "" else testFeaturePath + "_all_features"
      println(s"${allFeaturePath} is starting !!!")
      FileUtils.deleteQuietly(new File(allFeaturePath))
      constructFeatuteVector(spark.sparkContext, patient, allFeatures, allFeaturePath,
        testPatientIds, testAllFeaturePath)
      println(s"${allFeaturePath} is finished !!!")
    }
  }

  /**
    * Given a feature tuples RDD, construct features in vector
    * format for each patient. feature name should be mapped
    * to some index and convert to sparse feature format.
    *
    * @param sc SparkContext to run
    * @param feature RDD of input feature tuples
    * @return
    */
  def constructFeatuteVector(sc: SparkContext, patients: RDD[Patient], allFeature: RDD[FeatureTuple], featurePath: String,
                             testPatient: Dataset[PatientId], testFeaturePath: String): Unit = {

    /** save for later usage */
    //feature.cache()
    //patients.cache()

    val testPatientIds = if (testPatient != null) testPatient.rdd else null

    val featureMap = allFeature
      .map(_._1._2)
      .distinct
      .sortBy(x => x)
      .zipWithIndex
      .collectAsMap()

    val numFeature = featureMap.size
    println(s"Total Feature count: ${numFeature}")

    val patientMap = patients.map(x => (x.patientID, x.isDead)).collectAsMap()

    val scFeatureMap = sc.broadcast(featureMap)
    val scPatientMap = sc.broadcast(patientMap)

    val scTestPatientIds = if (testPatientIds != null)
      sc.broadcast(testPatientIds.map(_.patientID).sortBy(x => x.toInt).collect) else null

    val feature = if (testFeaturePath.isEmpty) allFeature else allFeature.filter(x => !scTestPatientIds.value.contains(x._1._1))

    val result = feature
      .map{case((patient, patientFeature), value) => (patient, (scFeatureMap.value(patientFeature), value))}
      .groupByKey
      .map { case (patient, indexedFeatures) =>

        val label = scPatientMap.value(patient)
        val featureVector = Vectors.sparse(numFeature, indexedFeatures.toList.map(x => (x._1.toInt, x._2)))
        LabeledPoint(label, featureVector)
      }
    MLUtils.saveAsLibSVMFile(result, featurePath)
    println(s"Result count: ${result.count}")

    if (!testFeaturePath.isEmpty) {
      val testResult = allFeature
        .filter(x => scTestPatientIds.value.contains(x._1._1))
        .map { case ((patient, patientFeature), value) => (patient, (scFeatureMap.value(patientFeature), value)) }
        .groupByKey
        .map { case (patient, indexedFeatures) =>

          val featureVector = Vectors.sparse(numFeature, indexedFeatures.toList.map(x => (x._1.toInt, x._2)))
          LabeledPoint(patient.toDouble, featureVector)
        }
      MLUtils.saveAsLibSVMFile(testResult, testFeaturePath)
      println(s"Test Result count: ${testResult.count}")
    }

    //patients.unpersist(true)
    //feature.unpersist(true)
  }
}
