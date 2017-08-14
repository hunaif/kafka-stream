/**
  * Created by muhammed on 12/8/17.
  */

import org.apache.spark._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
import _root_.kafka.serializer.StringDecoder
import services.ProcessData



object KafkaStreamExample {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("kafka-stream-example").setMaster("local[*]")
    StreamingLog.setStreamingLogLevels()

    //The cores should be greater than one as minimum one will be dedicated for the receiver

    val ssc = new StreamingContext(conf, Seconds(5))
    val brokers = "" // comma separated brokerlist
    val topics = "Phenom_Track_1_Topic"  //comma separated topic name
    // Create context with 2 second batch interval

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
//    val lines = messages.map(_._2)
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)


    val keyValueDStream = messages.map {
      case (key, value) => {
        ProcessData.createKeyValuePair(value)
      }
    }

    val aggregatedByRefNumDStream = keyValueDStream.reduceByKey((agg,x) => (agg+x))

    //Sort the Dstream based on the Freq count
    val sortedDStream = aggregatedByRefNumDStream.transform(x => x.sortBy(_._2))

    //As of now printing top 5
    //Can be persisted to DB or filesystem
    sortedDStream.print(5)

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }


}
