package org.kenyahmis.prepvisits;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.kenyahmis.core.DatabaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class LoadPrepVisits {

    private static final Logger logger = LoggerFactory.getLogger(LoadPrepVisits.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load PrEP Visits");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadPrepVisits.sql";
        String sourceQuery;
        InputStream inputStream = LoadPrepVisits.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load prep visits from file");
        }
        logger.info("Loading source prep visits");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.prepcentral.url"))
                .option("driver", rtConfig.get("spark.prepcentral.driver"))
                .option("user", rtConfig.get("spark.prepcentral.user"))
                .option("password", rtConfig.get("spark.prepcentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.prepcentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());

        sourceDf = sourceDf
                .withColumn("ClinicalNotes", when(col("ClinicalNotes").equalTo(""), null)
                        .otherwise(col("ClinicalNotes")))
                .withColumn("TreatedForHepC", when(col("TreatedForHepC").equalTo(""), null)
                        .otherwise(col("TreatedForHepC")))
                .withColumn("VaccinationForHepCStarted", when(col("VaccinationForHepCStarted").equalTo(""), null)
                        .otherwise(col("VaccinationForHepCStarted")))
                .withColumn("VaccinationForHepBStarted", when(col("VaccinationForHepBStarted").equalTo(""), null)
                        .otherwise(col("VaccinationForHepBStarted")))
                .withColumn("HepatitisCPositiveResult", when(col("HepatitisCPositiveResult").equalTo(""), null)
                        .otherwise(col("HepatitisCPositiveResult")))
                .withColumn("HepatitisBPositiveResult", when(col("HepatitisBPositiveResult").equalTo(""), null)
                        .otherwise(col("HepatitisBPositiveResult")))
                .withColumn("Reasonfornotgivingnextappointment", when(col("Reasonfornotgivingnextappointment").equalTo(""), null)
                        .otherwise(col("Reasonfornotgivingnextappointment")))
                .withColumn("Tobegivennextappointment", when(col("Tobegivennextappointment").equalTo(""), null)
                        .otherwise(col("Tobegivennextappointment")))
                .withColumn("CondomsIssued", when(col("CondomsIssued").equalTo(""), null)
                        .otherwise(col("CondomsIssued")))
                .withColumn("RegimenPrescribed", when(col("RegimenPrescribed").equalTo(""), null)
                        .otherwise(col("RegimenPrescribed")))
                .withColumn("PrepPrescribed", when(col("PrepPrescribed").equalTo(""), null)
                        .otherwise(col("PrepPrescribed")))
                .withColumn("PrepTreatmentPlan", when(col("PrepTreatmentPlan").equalTo(""), null)
                        .otherwise(col("PrepTreatmentPlan")))
                .withColumn("ContraindicationsPrep", when(col("ContraindicationsPrep").equalTo(""), null)
                        .otherwise(col("ContraindicationsPrep")))
                .withColumn("SymptomsAcuteHIV", when(col("SymptomsAcuteHIV").equalTo(""), null)
                        .otherwise(col("SymptomsAcuteHIV")))
                .withColumn("AdherenceReasons", when(col("AdherenceReasons").equalTo(""), null)
                        .otherwise(col("AdherenceReasons")))
                .withColumn("AdherenceOutcome", when(col("AdherenceOutcome").equalTo(""), null)
                        .otherwise(col("AdherenceOutcome")))
                .withColumn("AdherenceDone", when(col("AdherenceDone").equalTo(""), null)
                        .otherwise(col("AdherenceDone")))
                .withColumn("FPMethods", when(col("FPMethods").equalTo(""), null)
                        .otherwise(col("FPMethods")))
                .withColumn("FamilyPlanningStatus", when(col("FamilyPlanningStatus").equalTo(""), null)
                        .otherwise(col("FamilyPlanningStatus")))
                .withColumn("Breastfeeding", when(col("Breastfeeding").equalTo(""), null)
                        .otherwise(col("Breastfeeding")))
                .withColumn("BirthDefects", when(col("BirthDefects").equalTo(""), null)
                        .otherwise(col("BirthDefects")))
                .withColumn("PregnancyOutcome", when(col("PregnancyOutcome").equalTo(""), null)
                        .otherwise(col("PregnancyOutcome")))
                .withColumn("PregnancyEndDate", when(col("PregnancyEndDate").equalTo(""), null)
                        .otherwise(col("PregnancyEndDate")))
                .withColumn("PregnancyEnded", when(col("PregnancyEnded").equalTo(""), null)
                        .otherwise(col("PregnancyEnded")))
                .withColumn("PregnancyPlanned", when(col("PregnancyPlanned").equalTo(""), null)
                        .otherwise(col("PregnancyPlanned")))
                .withColumn("PlanningToGetPregnant", when(col("PlanningToGetPregnant").equalTo(""), null)
                        .otherwise(col("PlanningToGetPregnant")))
                .withColumn("EDD", when(col("EDD").equalTo(""), null)
                        .otherwise(col("EDD")))
                .withColumn("PregnantAtThisVisit", when(col("PregnantAtThisVisit").equalTo(""), null)
                        .otherwise(col("PregnantAtThisVisit")))
                .withColumn("MenopausalStatus", when(col("MenopausalStatus").equalTo(""), null)
                        .otherwise(col("MenopausalStatus")))
                .withColumn("LMP", when(col("LMP").equalTo(""), null)
                        .otherwise(col("LMP")))
                .withColumn("VMMCReferral", when(col("VMMCReferral").equalTo(""), null)
                        .otherwise(col("VMMCReferral")))
                .withColumn("Circumcised", when(col("Circumcised").equalTo(""), null)
                        .otherwise(col("Circumcised")))
                .withColumn("STITreated", when(col("STITreated").equalTo(""), null)
                        .otherwise(col("STITreated")))
                .withColumn("STISymptoms", when(col("STISymptoms").equalTo(""), null)
                        .otherwise(col("STISymptoms")))
                .withColumn("STIScreening", when(col("STIScreening").equalTo(""), null)
                        .otherwise(col("STIScreening")))
                .withColumn("BMI", when(col("BMI").equalTo(""), null)
                        .otherwise(col("BMI")))
                .withColumn("Height", when(col("Height").equalTo(""), null)
                        .otherwise(col("Height")))
                .withColumn("Weight", when(col("Weight").equalTo(""), null)
                        .otherwise(col("Weight")))
                .withColumn("BloodPressure", when(col("BloodPressure").equalTo(""), null)
                        .otherwise(col("BloodPressure")))
                .withColumn("MonthsPrescribed", when(col("MonthsPrescribed").gt(lit(12)), null)
                        .otherwise(col("MonthsPrescribed")))
                .withColumn("VisitDate",
                        when(col("VisitDate").equalTo("")
                                .or(col("VisitDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))), null)
                                .otherwise(col("VisitDate")))
                .withColumn("NextAppointment",
                        when(col("NextAppointment").equalTo("")
                                .or(col("NextAppointment").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))), null)
                                .otherwise(col("NextAppointment")))
                .withColumn("TreatedForHepB", when(col("TreatedForHepB").equalTo(""), null)
                        .otherwise(col("TreatedForHepB")));

        logger.info("Loading target prep visits");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.PrEP_Visits")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_prep_visits");
        sourceDf.createOrReplaceTempView("source_prep_visits");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_prep_visits s LEFT ANTI JOIN " +
                "target_prep_visits t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode AND t.VisitID <=> s.VisitID");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("PrepNumberHash", upper(sha2(col("PrepNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String columnList = "RefId,Created,PatientPk,SiteCode,Emr,Project,Processed,QueueId,Status,StatusDate,DateExtracted,FacilityId,FacilityName,PrepNumber,HtsNumber," +
                "VisitDate,VisitID,BloodPressure,Temperature,Weight,Height,BMI,STIScreening,STISymptoms,STITreated,Circumcised,VMMCReferral,LMP,MenopausalStatus," +
                "PregnantAtThisVisit,EDD,PlanningToGetPregnant,PregnancyPlanned,PregnancyEnded,PregnancyEndDate,PregnancyOutcome,BirthDefects,Breastfeeding,FamilyPlanningStatus," +
                "FPMethods,AdherenceDone,AdherenceOutcome,AdherenceReasons,SymptomsAcuteHIV,ContraindicationsPrep,PrepTreatmentPlan,PrepPrescribed,RegimenPrescribed,MonthsPrescribed," +
                "CondomsIssued,Tobegivennextappointment,Reasonfornotgivingnextappointment,HepatitisBPositiveResult,HepatitisCPositiveResult,VaccinationForHepBStarted,TreatedForHepB," +
                "VaccinationForHepCStarted,TreatedForHepC,NextAppointment,ClinicalNotes,Date_Created,Date_Last_Modified,RecordUUID";

        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));
        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.PrEP_Visits")
                .mode(SaveMode.Append)
                .save();

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PrepNumber", "PrepNumberHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.PrEP_Visits", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
