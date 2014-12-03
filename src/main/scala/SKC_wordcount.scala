import org.apache.spark._
import org.apache.spark.streaming._
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.kafka._

val appName = "SKC_wordcount"
val master = "spark://plamb-ubuntu:7077"

val conf = new SparkConf(true)
  .setAppName(appName)
  .setMaster(master)
  .set("spark.cassandra.connection.host", "127.0.0.1")
  .set("spark.executor.extraClassPath", "/home/plamb/Coding/Web_Analytics_POC/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/spark-cassandra-connector-assembly-1.1.1-SNAPSHOT.jar")
val ssc = new StreamingContext(conf, Seconds(1))
