/**
  * @author Joy Chakraborty <joychak1@gatech.edu>.
  */

package edu.gatech.cse8803.datamodel

import java.sql.Date

/** Data Model Classes **/

case class Patient(patientID: String, isMale: Boolean, dob: Date, isDead: Double, dod: Date, indexDate: Date, age: Double)

case class PatientId(patientID: String)

case class Chart(patientID: String, lastChartDate: Date)

case class IcuStays(patientID: String, inDate: Date, outDate: Date)

case class Saps2(patientID: String, hadmID: String, icuStayID: String, sapsScore: Double, scoreProbability: Double, ageScore: Double,
                 hrScore: Double, sysbpScore: Double, tempScore: Double, pao2fio2Score: Double, uoScore: Double,
                 bunScore: Double, wbcScore: Double, potassiumScore: Double, sodiumScore: Double,
                 bicarbonateScore: Double, bilirubinScore: Double, gcsScore: Double, comorbidityScore:
                 Double, admissiontypeScore: Double)

case class Comorbidities(patientID: String, hadmID: String, congestiveHeartFailure: Double,
                         cardiacArrhythmias: Double, valvularDisease: Double, pulmonaryCirculation: Double,
                         peripheralVascular: Double, hypertension: Double, paralysis: Double, otherNeurological: Double,
                         chronicPulmonary: Double, diabetesUncomplicated: Double, diabetesComplicated: Double,
                         hypothyroidism: Double, renalFailure: Double, liverDisease: Double, pepticUlcer: Double,
                         aids: Double, lymphoma: Double, metastaticCancer: Double, solidTumor: Double,
                         rheumatoidArthritis: Double, coagulopathy: Double, obesity: Double, weightLoss: Double,
                         fluidElectrolyte: Double, bloodLossAnemia: Double, deficiencyAnemias: Double,
                         alcoholAbuse: Double, drugAbuse: Double, psychoses: Double, depression: Double)

case class LabResult(patientID: String, hadmID: String, date: Date, labName: String, value: Double)

case class Diagnostic(patientID: String, hadmID: String, date: Date, icd9code: String, sequence: Int)

case class Medication(patientID: String, hadmID: String, date: Date, medicine: String)

case class Note(patientID: String, hadmID: String, chartDate: Date, category: String, description: String, text: String)

