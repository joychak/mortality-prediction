/**
  * @author Joy Chakraborty <joychak1@gatech.edu>.
  */

package com.datalogs.ioutils

import java.sql.Date
import java.text.SimpleDateFormat

import com.datalogs.datamodel._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by joychak on 11/30/16.
  */
object LoadData {
  def loadRddRawData(spark: SparkSession, inputPath: String, testFeatutePath: String,
                     hasSaps: Boolean, hasComor: Boolean, hasEvents: Boolean, hasNotes: Boolean): (Dataset[Patient],
    Dataset[IcuStays], Dataset[Saps2], Dataset[Comorbidities], Dataset[LabResult], Dataset[Diagnostic],
    Dataset[Medication], Dataset[Note], Dataset[PatientId]) = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val patient = loadPatientData(spark, inputPath, dateFormat)
    println(s"Patient count: ${patient.count}")

    val testPatientIds = if (!testFeatutePath.isEmpty) loadTestPatientIds(spark, inputPath, dateFormat) else null
    if (!testFeatutePath.isEmpty) println(s"Test Patient count: ${testPatientIds.count}")

    val icustay = loadICUData(spark, inputPath, dateFormat)
    println(s"ICU count: ${icustay.count}")

    val saps = if (hasSaps) loadSapsData(spark, inputPath) else null
    if (hasSaps) println(s"SAPS count: ${saps.count}")

    val comorbidities = if (hasComor) loadComorbiditiesData(spark, inputPath) else null
    if (hasComor) println(s"Comorbidities count: ${comorbidities.count}")

    val labResults = if (hasEvents) loadLabResultData(spark, inputPath, dateFormat) else null
    if (hasEvents) println(s"Lab result count: ${labResults.count}")

    val diagnostics = if (hasEvents) loadDiagnosticsData(spark, inputPath, dateFormat) else null
    if (hasEvents) println(s"Diagonstics count: ${diagnostics.count}")

    val medications = if (hasEvents) loadMedicationData(spark, inputPath, dateFormat) else null
    if (hasEvents) println(s"Medication count: ${medications.count}")

    //"sh scripts/formatnotes.sh data/NOTEEVENTS.csv data/NOTEEVENTS_temp.csv"!
    val notes = if (hasNotes) loadNotesData(spark, inputPath, dateFormat) else null
    if (hasNotes) println(s"Notes count: ${notes.count}")
    //"rm data/NOTEEVENTS_temp.csv"!

    //(patient, icustay, chart, saps, comorbidities, labResults, diagnostics, medications, notes)
    (patient, icustay, saps, comorbidities, labResults, diagnostics, medications, notes, testPatientIds)
  }

  def loadPatientData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[Patient] = {
    import spark.implicits._

    List(inputPath + "/PATIENTS.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "PATIENTS"))

    spark.sqlContext.sql(
      """
        |SELECT subject_id, gender, dob, dod, expire_flag
        |FROM PATIENTS
      """.stripMargin)
      .map(r => Patient(
        r(0).toString,
        if (r(1).toString.toLowerCase=="m") true else false,
        new Date(dateFormat.parse(r(2).toString).getTime),
        r(4).toString.toDouble,
        if (r(4).toString.toInt == 1 && r(3).toString.trim != "")
          new Date(dateFormat.parse(r(3).toString).getTime)
        else
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime),
        new Date(dateFormat.parse("1971-01-01 00:00:00").getTime),
        0.0
      ))
  }

  def loadTestPatientIds(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[PatientId] = {
    import spark.implicits._

    List(inputPath + "/icu_mortality_test_patients.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "TESTPATIENTS"))

    spark.sqlContext.sql(
      """
        |SELECT SUBJECT_ID
        |FROM TESTPATIENTS
      """.stripMargin)
      .map(r => PatientId(r(0).toString))
  }

  def loadICUData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[IcuStays] = {
    import spark.implicits._

    List(inputPath + "/ICUSTAYS.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "ICUSTAY"))

    spark.sqlContext.sql(
      """
        |SELECT subject_id, intime, outtime
        |FROM ICUSTAY
      """.stripMargin)
      .map(r => IcuStays(
        r(0).toString,
        if (r(1).toString.isEmpty)
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime)
        else
          new Date(dateFormat.parse(r(1).toString).getTime),
        if (r(2).toString.isEmpty)
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime)
        else
          new Date(dateFormat.parse(r(2).toString).getTime)
      ))
  }

  def loadChartData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[Chart] = {
    import spark.implicits._

    List(inputPath + "/CHARTEVENTS.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "CHARTEVENTS"))

    spark.sqlContext.sql(
      """
        |SELECT subject_id, charttime
        |FROM CHARTEVENTS
      """.stripMargin)
      .map(r => Chart(
        r(0).toString,
        new Date(dateFormat.parse(r(1).toString).getTime)
      ))
  }

  def loadSapsData(spark: SparkSession, inputPath: String): Dataset[Saps2] = {
    import spark.implicits._

    List(inputPath + "/SAPSII.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "SAPSII"))

    spark.sqlContext.sql(
      """
        |SELECT subject_id, hadm_id, icustay_id, sapsii, sapsii_prob, age_score,hr_score,sysbp_score,temp_score,
        |pao2fio2_score,uo_score,bun_score,wbc_score,potassium_score,sodium_score,bicarbonate_score,
        |bilirubin_score,gcs_score,comorbidity_score,admissiontype_score
        |FROM SAPSII
      """.stripMargin)
      .map(r => Saps2(
        r(0).toString,
        r(1).toString,
        r(2).toString,
        if (r(3).toString.isEmpty) 0.0 else r(3).toString.toDouble,
        if (r(4).toString.isEmpty) 0.0 else r(4).toString.toDouble,
        if (r(5).toString.isEmpty) 0.0 else r(5).toString.toDouble,
        if (r(6).toString.isEmpty) 0.0 else r(6).toString.toDouble,
        if (r(7).toString.isEmpty) 0.0 else r(7).toString.toDouble,
        if (r(8).toString.isEmpty) 0.0 else r(8).toString.toDouble,
        if (r(9).toString.isEmpty) 0.0 else r(9).toString.toDouble,
        if (r(10).toString.isEmpty) 0.0 else r(10).toString.toDouble,
        if (r(11).toString.isEmpty) 0.0 else r(11).toString.toDouble,
        if (r(12).toString.isEmpty) 0.0 else r(12).toString.toDouble,
        if (r(13).toString.isEmpty) 0.0 else r(13).toString.toDouble,
        if (r(14).toString.isEmpty) 0.0 else r(14).toString.toDouble,
        if (r(15).toString.isEmpty) 0.0 else r(15).toString.toDouble,
        if (r(16).toString.isEmpty) 0.0 else r(16).toString.toDouble,
        if (r(17).toString.isEmpty) 0.0 else r(17).toString.toDouble,
        if (r(18).toString.isEmpty) 0.0 else r(18).toString.toDouble,
        if (r(19).toString.isEmpty) 0.0 else r(19).toString.toDouble
      ))
  }

  def loadComorbiditiesData(spark: SparkSession, inputPath: String): Dataset[Comorbidities] = {
    import spark.implicits._

    List(inputPath + "/EHCOMORBIDITIES.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "EHCOMORBIDITIES"))

    spark.sqlContext.sql(
      """
        |SELECT subject_id,hadm_id,congestive_heart_failure,cardiac_arrhythmias,valvular_disease,
        |pulmonary_circulation,peripheral_vascular,hypertension,paralysis,other_neurological,
        |chronic_pulmonary,diabetes_uncomplicated,diabetes_complicated,hypothyroidism,renal_failure,
        |liver_disease,peptic_ulcer,aids,lymphoma,metastatic_cancer,solid_tumor,rheumatoid_arthritis,
        |coagulopathy,obesity,weight_loss,fluid_electrolyte,blood_loss_anemia,deficiency_anemias,
        |alcohol_abuse,drug_abuse,psychoses,depression
        |FROM EHCOMORBIDITIES
      """.stripMargin)
      .map(r => Comorbidities(
        r(0).toString,
        r(1).toString,
        if (r(2).toString.isEmpty) 0.0 else r(3).toString.toDouble,
        if (r(3).toString.isEmpty) 0.0 else r(3).toString.toDouble,
        if (r(4).toString.isEmpty) 0.0 else r(4).toString.toDouble,
        if (r(5).toString.isEmpty) 0.0 else r(5).toString.toDouble,
        if (r(6).toString.isEmpty) 0.0 else r(6).toString.toDouble,
        if (r(7).toString.isEmpty) 0.0 else r(7).toString.toDouble,
        if (r(8).toString.isEmpty) 0.0 else r(8).toString.toDouble,
        if (r(9).toString.isEmpty) 0.0 else r(9).toString.toDouble,
        if (r(10).toString.isEmpty) 0.0 else r(10).toString.toDouble,
        if (r(11).toString.isEmpty) 0.0 else r(11).toString.toDouble,
        if (r(12).toString.isEmpty) 0.0 else r(12).toString.toDouble,
        if (r(13).toString.isEmpty) 0.0 else r(13).toString.toDouble,
        if (r(14).toString.isEmpty) 0.0 else r(14).toString.toDouble,
        if (r(15).toString.isEmpty) 0.0 else r(15).toString.toDouble,
        if (r(16).toString.isEmpty) 0.0 else r(16).toString.toDouble,
        if (r(17).toString.isEmpty) 0.0 else r(17).toString.toDouble,
        if (r(18).toString.isEmpty) 0.0 else r(18).toString.toDouble,
        if (r(19).toString.isEmpty) 0.0 else r(19).toString.toDouble,
        if (r(20).toString.isEmpty) 0.0 else r(20).toString.toDouble,
        if (r(21).toString.isEmpty) 0.0 else r(21).toString.toDouble,
        if (r(22).toString.isEmpty) 0.0 else r(22).toString.toDouble,
        if (r(23).toString.isEmpty) 0.0 else r(23).toString.toDouble,
        if (r(24).toString.isEmpty) 0.0 else r(24).toString.toDouble,
        if (r(25).toString.isEmpty) 0.0 else r(25).toString.toDouble,
        if (r(26).toString.isEmpty) 0.0 else r(26).toString.toDouble,
        if (r(27).toString.isEmpty) 0.0 else r(27).toString.toDouble,
        if (r(28).toString.isEmpty) 0.0 else r(28).toString.toDouble,
        if (r(29).toString.isEmpty) 0.0 else r(29).toString.toDouble,
        if (r(30).toString.isEmpty) 0.0 else r(30).toString.toDouble,
        if (r(31).toString.isEmpty) 0.0 else r(31).toString.toDouble
      ))
  }

  def loadLabResultData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[LabResult] = {
    import spark.implicits._

    List(inputPath + "/LABEVENTS.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "LABEVENTS"))

    spark.sqlContext.sql(
      """
        |SELECT subject_id, hadm_id, charttime, itemid, valuenum
        |FROM LABEVENTS
      """.stripMargin)
      .map(r => LabResult(
        r(0).toString,
        r(1).toString,
        new Date(dateFormat.parse(r(2).toString).getTime),
        r(3).toString,
        if (!r(4).toString.isEmpty) r(4).toString.toDouble else 0.0
      ))
  }

  def loadDiagnosticsData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[Diagnostic] = {
    import spark.implicits._

    List(inputPath + "/DIAGNOSES_ICD.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "DIAGNOSES_ICD"))

    spark.sqlContext.sql(
      """
        |SELECT subject_id, hadm_id, icd9_code, seq_num
        |FROM DIAGNOSES_ICD
      """.stripMargin)
      .map(r => Diagnostic(
        r(0).toString,
        r(1).toString,
        new Date(dateFormat.parse("1900-01-01 00:00:00").getTime),
        r(2).toString,
        if (r(3).toString.isEmpty) 0 else r(3).toString.toInt
      ))
  }

  def loadMedicationData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[Medication] = {
    import spark.implicits._

    List(inputPath + "/PRESCRIPTIONS.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "PRESCRIPTIONS"))

    spark.sqlContext.sql(
      """
        |SELECT subject_id, hadm_id, startdate, enddate, drug
        |FROM PRESCRIPTIONS
      """.stripMargin)
      .map(r => Medication(
        r(0).toString,
        r(1).toString,
        if (!r(2).toString.isEmpty)
          new Date(dateFormat.parse(r(2).toString).getTime)
        else if (!r(3).toString.isEmpty)
          new Date(dateFormat.parse(r(3).toString).getTime)
        else
          new Date(dateFormat.parse("1900-01-01 00:00:00").getTime),
        r(3).toString
      ))
  }

  def loadNotesData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[Note] = {

    import spark.implicits._

    List(inputPath + "/NOTEEVENTS.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "NOTEEVENTS"))

    spark.sqlContext.sql(
      """
        |SELECT subject_id, hadm_id, chartdate, charttime, category, description, text
        |FROM NOTEEVENTS
      """.stripMargin)
      .filter(r => !r(4).toString.toLowerCase.contains("discharge summary"))
      .map(r => Note(
        r(0).toString,
        r(1).toString,
        if (r(3).toString.isEmpty) {
          val dateString = if (r(2).toString.trim.length == 10) r(2).toString.trim + " 00:00:00" else r(2).toString.trim
          new Date(dateFormat.parse(dateString).getTime)
        }
        else {
          val dateString = if (r(3).toString.trim.length == 10) r(3).toString.trim + " 00:00:00" else r(3).toString.trim
          new Date(dateFormat.parse(dateString).getTime) //dateFormat.parse(r(1).toString),
        },
        r(4).toString,
        r(5).toString,
        r(6).toString
      ))
  }
}
