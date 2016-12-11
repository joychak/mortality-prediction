/**
  * @author Joy Chakraborty <joychak1@gatech.edu>.
  */

package com.datalogs.features

import java.sql.Date

import com.datalogs.datamodel.Note
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by joychak on 11/16/16.
  */
object LDAHelper {

  def preprocess(spark: SparkSession, feature: RDD[Note], stopwordFile: String)
  : (RDD[(String, Vector)], Array[String], Long) = {

    import spark.implicits._

    //val vocabSize = 2900000

    //val initialrdd = feature.map(x => (x.patientID, x.chartDate, x.text))
    val initialrdd = feature
      .map(x => (x.patientID, x.text))
      .reduceByKey((x, y) => x + " " + y)

    val rdd = initialrdd.map(x => (x._1, LDAHelper.filterSpecialCharacters(x._2)))
    val df = rdd.toDF("patientId", "docs")

    val sc = spark.sparkContext
    val customizedStopWords: Array[String] = if (stopwordFile.isEmpty) {
      Array.empty[String]
    } else {
      val stopWordText = sc.textFile(stopwordFile).collect()
      stopWordText.flatMap(_.stripMargin.split(",")).map(_.trim).distinct
    }

    println("Custom stop words: " + customizedStopWords.length)

    //Tokenizing using the RegexTokenizer
    val tokenizer = new RegexTokenizer().setInputCol("docs").setOutputCol("rawTokens")

    //Removing the Stop-words using the Stop Words remover
    val stopWordsRemover = new StopWordsRemover().setInputCol("rawTokens").setOutputCol("tokens")
    stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ customizedStopWords)

    //Converting the Tokens into the CountVector
    val countVectorizer = new CountVectorizer()
      //.setVocabSize(vocabSize)
      .setInputCol("tokens")
      .setOutputCol("features")

    //Setting up the pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

    val model = pipeline.fit(df)
    val documents = model
      .transform(df)
      .select("patientId", "features")
      .rdd.map {
      case Row(patientId: String, features: MLVector)
      => (patientId, Vectors.fromML(features))
    } //.zipWithIndex().map(_.swap)

    println("Document count: " + documents.count)

    (documents,
      model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary,  // vocabulary
      documents.map(_._2.numActives).sum().toLong                     // total token count
      )
  }

  def filterSpecialCharacters(document: String) = {

    document.replaceAll( """[! @ # $ % ^ & * ( ) \[ \] . \\ / _ { } + - − , " ' ~ ; : ` ? = > < --]""", " ")
      //.replaceAll("""\.\s""", " ")
      .replaceAll("""\w*\d\w*""", " ")        //replace digits
      .replaceAll("""\s[A-Z,a-z]\s""", " ")   //replace single digit char
      .replaceAll("""\s[A-Z,a-z]\s""", " ")   //replace consecutive single digit char
  }
}
