package com.example.sk

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

object SparkKafkaMain {
  val config: Config = ConfigFactory.load("config-template")
  val infoLogger: Logger = LoggerFactory.getLogger(SparkKafkaMain.getClass)
  var value: String ="""{"com.janio.b2c.janiobackned":{"""
  var ro: Int = 0

  def main(args: Array[String]): Unit = {
    val spark = SparkInstance.getSparkInstance
    infoLogger.info("Starting Job...")

    spark.read.json(config.getString("S3_KAFKA_CHECKPOINT"))
      .createOrReplaceTempView("checkpoint")
    val checkpoint = spark.sql("select topic,partition,max(offset) as offset from checkpoint group by topic,partition").toDF()

    var co = 0

    checkpoint.foreach { row =>
      ro += 1
      row.toSeq.foreach { col =>
        co += 1
        if (co == 2) {
          value +=""""""" + col +"""":"""
        }
        if (co == 3) {
          value += col
        }
      }
      if (ro != 4)
        value += ","
    }
    value +="""}}"""

    infoLogger.info("Kafka starting offset: " + value)
    println(value)

    /**
      * Creating dataframe from kafka
      */
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("KAFKA_BOOTSTRAP_SERVERS"))
      .option("subscribe", config.getString("KAFKA_TOPIC"))
      .option("startingOffsets", value)
      .load()

    /**
     * <START>
     * LOGIC GOES HERE FOR (WHAT TO DO WITH KAFKA DATA ?)
     */

    /**
     * <END>
     */

    /**
      * Going to delete old kafka checkpoint and will save new kafka checkpoint
      * */
    infoLogger.info("Going to delete kafka checkpoint from s3")
    FileSystem.get(new URI(config.getString("S3_URI")), SparkInstance.getSparkInstance.sparkContext.hadoopConfiguration)
      .delete(new Path(config.getString("S3_KAFKA_CHECKPOINT")), true)
    infoLogger.info("Successfully deleted kafka checkpoint from s3")
    df.createOrReplaceTempView("checkpoint")
    spark.sql("select topic,partition,max(offset) as offset from checkpoint group by topic,partition")
      .coalesce(1).write.mode("append").json(config.getString("S3_KAFKA_CHECKPOINT"))
    infoLogger.info("Successfully written kafka checkpoint from s3")
  }

}


