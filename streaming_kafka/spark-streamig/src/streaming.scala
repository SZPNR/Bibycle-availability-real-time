import org.apache.spark.sql.SparkSession
import org.apache.spark

object Streaming {
  def main(agrs: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark Streaming With Scala and Kafka")
      .master("spark://spark:7077")
      .getOrCreate()
    
    println("created SparkSession")

    import spark.implicits._
    

    spark.sparkContext.setLogLevel("ERROR")

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "AKIAV6RXMNQVQQD7YMO2")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "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")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")



    // access key AKIAV6RXMNQVQQD7YMO2
    // secret key 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

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "test-topic")
      .load()

    val rawDF = df.selectExpr("cast (value as string) as json")
      .select(from_json($"json", schema).as("data"))
      .select("data.*")

    println(rawDF)

    
    println("Commence streaming")


    val writeToS3Query = rawDF.writeStream
                          .format("parquet")
                          .outputMode("append")
                          .option("path","s3a://velib-streaming-weather/streaming-velib/")
                          .start()

    query.awaitTermination()
  }
}
