/**
  * @author Joy Chakraborty <joychak1@gatech.edu>.
  */

package com.datalogs.features

import java.sql.Date

import com.datalogs.datamodel._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.{DistributedLDAModel, EMLDAOptimizer, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by joychak on 11/17/16.
  */
object FeatureConstruction {
  /**
    * ((patient-id, feature-name), feature-value)
    */
  type FeatureTuple = ((String, String), Double)

  val obervationWindow = 2000L

  abstract class MortalityType()
  case class InICU12Hr() extends MortalityType
  case class InICU24Hr() extends MortalityType
  case class InICU48Hr() extends MortalityType
  case class InICU() extends MortalityType
  case class In30Days() extends MortalityType
  case class In1Year() extends MortalityType

  def constructPatientBasedOnMortalityPeriod(spark: SparkSession, patient: Dataset[Patient], icustays: Dataset[IcuStays],
                                               mortType: MortalityType, predictionHr: Int) : RDD[Patient] ={

    val MILLISECONDS_IN_DAY = 24L * 60 * 60 * 1000
    val MILLISECONDS_IN_YEAR = MILLISECONDS_IN_DAY * 365L

    val icuStaysMap = icustays.rdd
      .map(x => (x.patientID, x))
      .reduceByKey((x, y) => if (x.outDate.getTime > y.outDate.getTime) x else y)
      .collectAsMap()

    val scICUStaysMap = spark.sparkContext.broadcast(icuStaysMap)

    patient.rdd
      .map(x => {
        if (scICUStaysMap.value.contains(x.patientID)) {
          val icuData = scICUStaysMap.value(x.patientID)

          val diedInThisPeriod = mortType match {
            case InICU() => x.dod.getTime >= icuData.inDate.getTime && x.dod.getTime <= icuData.outDate.getTime
            case In30Days() => x.dod.getTime > icuData.outDate.getTime &&
              (math.abs(x.dod.getTime - icuData.outDate.getTime) / MILLISECONDS_IN_DAY) <= 30
            case In1Year() => x.dod.getTime > icuData.outDate.getTime &&
              (math.abs(x.dod.getTime - icuData.outDate.getTime) / MILLISECONDS_IN_DAY) > 30 &&
              (math.abs(x.dod.getTime - icuData.outDate.getTime) / MILLISECONDS_IN_DAY) <= 365
          }
          val dead = if (x.isDead != 0 && diedInThisPeriod) 1.0 else 0.0

          val predictionTime = icuData.inDate.getTime + predictionHr * 3600 * 1000L
          val indexTime =
            if (predictionHr == 0) {
              //Retrospective model data
              if (dead == 1.0)
                x.dod.getTime
              else
                mortType match {
                  case InICU() => icuData.outDate.getTime
                  case In30Days() => icuData.outDate.getTime + 30 * MILLISECONDS_IN_DAY
                  case In1Year() => icuData.outDate.getTime + 365 * MILLISECONDS_IN_DAY
                }
            }
            else {
              //Hourly model data
              if (predictionTime < icuData.outDate.getTime && predictionTime < x.dod.getTime)
                predictionTime
              else if (icuData.outDate.getTime < x.dod.getTime)
                icuData.outDate.getTime
              else
                x.dod.getTime
            }

          Patient(x.patientID, x.isMale, x.dob, dead, x.dod, new Date(indexTime),
            math.abs(indexTime - x.dob.getTime) / MILLISECONDS_IN_YEAR)
        }
        else
          Patient(x.patientID, x.isMale, x.dob, 0.0, x.dod, x.indexDate,
            math.abs(x.indexDate.getTime - x.dob.getTime)/MILLISECONDS_IN_YEAR)
      })
      //.filter(x => x.indexDate.getTime > x.dod.getTime)
  }

  /**
    * Get the base feature
    *
    * @param spark
    * @param patient
    * @return
    */
  def constructBaseFeatureTuple(spark: SparkSession, patient: Dataset[Patient]): RDD[FeatureTuple] = {
    import spark.implicits._

    //val today = new Date(Calendar.getInstance().getTime.getTime)

    val age = patient.map(x => ((x.patientID, "patient_age"), x.age))
    val sex = patient.map(x => ((x.patientID, "patient_sex"), if (x.isMale) 1.0 else 0.0))
    age.rdd.union(sex.rdd)
    //sex.rdd
  }

  /**
    * Get the saps score
    *
    * @param spark
    * @param saps
    * @return
    */
  def constructSaps2FeatureTuple(spark: SparkSession, patients: RDD[Patient],
                                 saps: Dataset[Saps2]): RDD[FeatureTuple] = {
    import spark.implicits._

    val patientMap = patients.map(x => (x.patientID, x.indexDate)).collectAsMap()
    val scPatientMap = spark.sparkContext.broadcast(patientMap)

    val sapsUnique = saps.rdd
      .filter(x => scPatientMap.value.contains(x.patientID))
      .map(x => ((x.patientID, x.hadmID), x))
      .reduceByKey((x, y) => if (x.icuStayID.toDouble > y.icuStayID.toDouble) x else y)
      .map(_._2)
      .map(x => (x.patientID, x))
      .reduceByKey((x, y) => if (x.hadmID.toDouble > y.hadmID.toDouble) x else y)
      .map(_._2)

    val sapsScore = sapsUnique.map(x => ((x.patientID, "saps_score"), x.sapsScore))
    val scoreProbability = sapsUnique.map(x => ((x.patientID, "saps_probab"), x.scoreProbability))
    val ageScore = sapsUnique.map(x => ((x.patientID, "age_score"), x.ageScore))
    val hrScore = sapsUnique.map(x => ((x.patientID, "hr_score"), x.hrScore))
    val sysbpScore = sapsUnique.map(x => ((x.patientID, "sysbp_score"), x.sysbpScore))
    val tempScore = sapsUnique.map(x => ((x.patientID, "temp_score"), x.tempScore))
    val pao2fio2Score = sapsUnique.map(x => ((x.patientID, "pao2fio_score"), x.pao2fio2Score))
    val uoScore = sapsUnique.map(x => ((x.patientID, "uo_score"), x.uoScore))
    val bunScore = sapsUnique.map(x => ((x.patientID, "bun_score"), x.bunScore))
    val wbcScore = sapsUnique.map(x => ((x.patientID, "wbc_score"), x.wbcScore))
    val potassiumScore = sapsUnique.map(x => ((x.patientID, "potassium_score"), x.potassiumScore))
    val sodiumScore = sapsUnique.map(x => ((x.patientID, "sodium_score"), x.sodiumScore))
    val bicarbonateScore = sapsUnique.map(x => ((x.patientID, "bicarbonate_score"), x.bicarbonateScore))
    val bilirubinScore = sapsUnique.map(x => ((x.patientID, "bilirubin_score"), x.bilirubinScore))
    val gcsScore = sapsUnique.map(x => ((x.patientID, "gcs_score"), x.gcsScore))
    val comorbidityScore = sapsUnique.map(x => ((x.patientID, "comorbidity_score"), x.comorbidityScore))
    val admissiontypeScore = sapsUnique.map(x => ((x.patientID, "admissiontype_score"), x.admissiontypeScore))

    sapsScore.union(scoreProbability).union(ageScore).union(hrScore).union(sysbpScore).union(tempScore).union(
      pao2fio2Score).union(uoScore).union(bunScore).union(wbcScore).union(potassiumScore).union(sodiumScore).union(
      bicarbonateScore).union(bilirubinScore).union(gcsScore).union(comorbidityScore).union(admissiontypeScore)
  }

  /**
    * Get the saps score
    *
    * @param spark
    * @param saps
    * @return
    */
  def constructComorbiditiesFeatureTuple(spark: SparkSession, patients: RDD[Patient],
                                         comorbidities: Dataset[Comorbidities]): RDD[FeatureTuple] = {
    import spark.implicits._

    val patientMap = patients.map(x => (x.patientID, x.indexDate)).collectAsMap()
    val scPatientMap = spark.sparkContext.broadcast(patientMap)


    val comorbiditiesUnique = comorbidities.rdd
      .filter(x => scPatientMap.value.contains(x.patientID))
      .map(x => ((x.patientID, x.hadmID), x))
      .reduceByKey((x, y) => if (x.hadmID.toDouble > y.hadmID.toDouble) x else y)
      .map(_._2)
      .map(x => (x.patientID, x))
      .reduceByKey((x, y) => if (x.hadmID.toDouble > y.hadmID.toDouble) x else y)
      .map(_._2)

    val congestiveHeartFailure = comorbiditiesUnique.map(x => ((x.patientID, "com-congestiveHeartFailure"), x.congestiveHeartFailure))
    val cardiacArrhythmias = comorbiditiesUnique.map(x => ((x.patientID, "com-cardiacArrhythmias"), x.cardiacArrhythmias))
    val valvularDisease = comorbiditiesUnique.map(x => ((x.patientID, "com-valvularDisease"), x.valvularDisease))
    val pulmonaryCirculation = comorbiditiesUnique.map(x => ((x.patientID, "com-pulmonaryCirculation"), x.pulmonaryCirculation))
    val peripheralVascular = comorbiditiesUnique.map(x => ((x.patientID, "com-peripheralVascular"), x.peripheralVascular))
    val hypertension = comorbiditiesUnique.map(x => ((x.patientID, "com-hypertension"), x.hypertension))
    val paralysis = comorbiditiesUnique.map(x => ((x.patientID, "com-paralysis"), x.paralysis))
    val otherNeurological = comorbiditiesUnique.map(x => ((x.patientID, "com-otherNeurological"), x.otherNeurological))
    val chronicPulmonary = comorbiditiesUnique.map(x => ((x.patientID, "ccom-hronicPulmonary"), x.chronicPulmonary))
    val diabetesUncomplicated = comorbiditiesUnique.map(x => ((x.patientID, "com-diabetesUncomplicated"), x.diabetesUncomplicated))
    val diabetesComplicated = comorbiditiesUnique.map(x => ((x.patientID, "com-diabetesComplicated"), x.diabetesComplicated))
    val hypothyroidism = comorbiditiesUnique.map(x => ((x.patientID, "com-hypothyroidism"), x.hypothyroidism))
    val renalFailure = comorbiditiesUnique.map(x => ((x.patientID, "com-renalFailure"), x.renalFailure))
    val liverDisease = comorbiditiesUnique.map(x => ((x.patientID, "lcom-iverDisease"), x.liverDisease))
    val pepticUlcer = comorbiditiesUnique.map(x => ((x.patientID, "com-pepticUlcer"), x.pepticUlcer))
    val aids = comorbiditiesUnique.map(x => ((x.patientID, "com-aids"), x.aids))
    val lymphoma = comorbiditiesUnique.map(x => ((x.patientID, "com-lymphoma"), x.lymphoma))
    val metastaticCancer = comorbiditiesUnique.map(x => ((x.patientID, "com-metastaticCancer"), x.metastaticCancer))
    val solidTumor = comorbiditiesUnique.map(x => ((x.patientID, "com-solidTumor"), x.solidTumor))
    val rheumatoidArthritis = comorbiditiesUnique.map(x => ((x.patientID, "com-rheumatoidArthritis"), x.rheumatoidArthritis))
    val coagulopathy = comorbiditiesUnique.map(x => ((x.patientID, "com-coagulopathy"), x.coagulopathy))
    val obesity = comorbiditiesUnique.map(x => ((x.patientID, "com-obesity"), x.obesity))
    val weightLoss = comorbiditiesUnique.map(x => ((x.patientID, "weightLoss"), x.weightLoss))
    val fluidElectrolyte = comorbiditiesUnique.map(x => ((x.patientID, "com-fluidElectrolyte"), x.fluidElectrolyte))
    val bloodLossAnemia = comorbiditiesUnique.map(x => ((x.patientID, "com-bloodLossAnemia"), x.bloodLossAnemia))
    val deficiencyAnemias = comorbiditiesUnique.map(x => ((x.patientID, "com-deficiencyAnemias"), x.deficiencyAnemias))
    val alcoholAbuse = comorbiditiesUnique.map(x => ((x.patientID, "com-alcoholAbuse"), x.alcoholAbuse))
    val drugAbuse = comorbiditiesUnique.map(x => ((x.patientID, "com-drugAbuse"), x.drugAbuse))
    val psychoses = comorbiditiesUnique.map(x => ((x.patientID, "com-psychoses"), x.psychoses))
    val depression = comorbiditiesUnique.map(x => ((x.patientID, "com-depression"), x.depression))

    congestiveHeartFailure.union(cardiacArrhythmias).union(valvularDisease).union(pulmonaryCirculation).union(
      peripheralVascular).union(hypertension).union(paralysis).union(otherNeurological).union(chronicPulmonary)
      .union(diabetesComplicated).union(diabetesUncomplicated).union(hypothyroidism).union(renalFailure)
      .union(liverDisease).union(pepticUlcer).union(aids).union(lymphoma).union(metastaticCancer).union(solidTumor)
      .union(rheumatoidArthritis).union(coagulopathy).union(obesity).union(weightLoss).union(fluidElectrolyte)
      .union(bloodLossAnemia).union(deficiencyAnemias).union(alcoholAbuse).union(drugAbuse).union(psychoses)
      .union(depression)
  }

  /**
    * Aggregate feature tuples from diagnostic with COUNT aggregation,
    *
    * @param diagnostic RDD of diagnostic
    * @return RDD of feature tuples
    */
  def constructDiagnosticFeatureTuple(spark: SparkSession, patients: RDD[Patient],
                                      diagnostic: Dataset[Diagnostic]): RDD[FeatureTuple] = {
    import spark.implicits._

    val patientMap = patients.map(x => (x.patientID, x.indexDate)).collectAsMap()
    val scPatientMap = spark.sparkContext.broadcast(patientMap)

    diagnostic
      .filter(x => scPatientMap.value.contains(x.patientID) && x.icd9code.length>0)
      .filter(x => x.date.getTime <= scPatientMap.value(x.patientID).getTime)
      //      .filter(x => (scPatientMap.value(x.patientID).getTime - x.date.getTime) <= obervationWindow &&
      //            (scPatientMap.value(x.patientID).getTime - x.date.getTime) >= 0)
      .map(x => ((x.patientID, "DIAG-" + x.icd9code), 1.0)).rdd.reduceByKey(_+_).map(x => x)
  }

  /**
    * Aggregate feature tuples from medication with COUNT aggregation,
    *
    * @param medication RDD of medication
    * @return RDD of feature tuples
    */
  def constructMedicationFeatureTuple(spark: SparkSession, patients: RDD[Patient],
                                      medication: Dataset[Medication]): RDD[FeatureTuple] = {
    import spark.implicits._

    val patientMap = patients.map(x => (x.patientID, x.indexDate)).collectAsMap()
    val scPatientMap = spark.sparkContext.broadcast(patientMap)

    medication
      .filter(x => scPatientMap.value.contains(x.patientID) && x.medicine.length>0)
      .filter(x => x.date.getTime <= scPatientMap.value(x.patientID).getTime)
      //      .filter(x => (scPatientMap.value(x.patientID).getTime - x.date.getTime) <= obervationWindow &&
      //              (scPatientMap.value(x.patientID).getTime - x.date.getTime) >= 0)
      .map(x => ((x.patientID, "MED-" + x.medicine), 1.0)).rdd.reduceByKey(_+_).map(x => x)
  }

  /**
    * Aggregate feature tuples from lab result, using AVERAGE aggregation
    *
    * @param labResult RDD of lab result
    * @return RDD of feature tuples
    */
  def constructLabFeatureTuple(spark: SparkSession, patients: RDD[Patient],
                               labResult: Dataset[LabResult]): RDD[FeatureTuple] = {
    import spark.implicits._

    val patientMap = patients.map(x => (x.patientID, x.indexDate)).collectAsMap()
    val scPatientMap = spark.sparkContext.broadcast(patientMap)

    labResult.rdd
      .filter(x => scPatientMap.value.contains(x.patientID) && x.labName.length>0)
      .filter(x => x.date.getTime <= scPatientMap.value(x.patientID).getTime)
      //      .filter(x => (scPatientMap.value(x.patientID).getTime - x.date.getTime) <= obervationWindow &&
      //                    (scPatientMap.value(x.patientID).getTime - x.date.getTime) >= 0)
      .map(x => ((x.patientID, "LAB-" + x.labName), (x.value, 1.0)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1/x._2._2))
  }

  /**
    * Get the Notes using LDA
    *
    * @param spark
    * @param notes
    * @return
    */
  def constructNoteFeatureTuple(spark: SparkSession, stopWordFile: String, patients: RDD[Patient],
                                notes: Dataset[Note]): RDD[FeatureTuple] = {

    val patientMap = patients.map(x => (x.patientID, x.indexDate)).collectAsMap()
    val scPatientMap = spark.sparkContext.broadcast(patientMap)

    val filteredNotes = notes.rdd
      .filter(x => scPatientMap.value.contains(x.patientID) && x.text.length>0)
      .filter(x => x.chartDate.getTime <= scPatientMap.value(x.patientID).getTime)

    val (documents, vocabArray, actualNumTokens) = LDAHelper.preprocess(
      spark, filteredNotes, stopWordFile)

    val patientDocs = documents.map(x => (x._1, x._2)).zipWithIndex.map(_.swap) //.collectAsMap()

    //patientDocs.foreach(x => println(s"Patient: ${x._2._1} - Date/Time: ${x._2._2}"))

    val corpus = patientDocs.map(x => (x._1, x._2._2))

    val lda = new LDA().setOptimizer(new EMLDAOptimizer).setK(50).setMaxIterations(25)

    val ldaModel = lda.run(corpus)

    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
//    println(s"50 topics:")
//    topics.zipWithIndex.foreach { case (topic, i) =>
//      println(s"TOPIC $i")
//      topic.foreach { case (term, weight) =>
//        println(s"$term\t$weight")
//      }
//      println()
//    }

    val patientIdMap = patientDocs.map(x => (x._1, x._2._1)).collectAsMap
    val scPatientIdMap = spark.sparkContext.broadcast(patientIdMap)

    val distLdaModel = ldaModel.asInstanceOf[DistributedLDAModel]
    val topTopicsForDoc = distLdaModel.topTopicsPerDocument(50)

    //    println(s"Document => Top topics")
    //    topTopicsForDoc.take(10).foreach{case(docId, topics, weights) =>
    //      val doc = scPatientIdMap.value(docId)
    //      println(s"Document-id: ${doc}")
    //      for (topic <- topics.indices) {
    //        println(s"Topic: ${topics(topic)}, Weight: ${weights(topic)}")
    //      }
    //    }

    topTopicsForDoc.map { case (docId, topics, weights) =>
      topics.map(topic => ((scPatientIdMap.value(docId), "topic-"+topic.toString), weights(topic)))
    }.flatMap(x => x)
  }
}