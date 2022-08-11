package com.example.sk

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object SparkInstance {

  val config: Config = ConfigFactory.load("config-template")

  /**
    * @return SparkSession
    */
  def getSparkInstance: SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(config.getString("SPARK_APP_NAME"))
      .master(config.getString("SPARK_HOST"))
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.session.timeZone", config.getString("SPARK_TIMEZONE"))
      .config("spark.debug.maxToStringFields", "1000")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("spark.sql.codegen.wholeStage", "false")
      .config("spark.executor.heartbeatInterval","1000000")
      .config("spark.network.timeout","1200000")
      .getOrCreate()
    val sc = spark.sparkContext
        sc.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", config.getString("S3_ACCESS_KEY"))
        sc.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", config.getString("S3_Secret_KEY"))
        sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        sc.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
        sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com")    
    spark
  }
}
