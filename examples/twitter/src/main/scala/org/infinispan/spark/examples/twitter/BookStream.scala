package org.infinispan.spark.examples.twitter

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.event.ClientEvent.Type
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.examples.twitter.Sample.getSparkConf
import org.infinispan.spark.stream.InfinispanInputDStream

import scala.language.existentials
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object BookStream {

   def populateBook(host: String) = {
      val cache = new RemoteCacheManager(
         new ConfigurationBuilder().addServer().host(host).build
      ).getCache("exampleCache").asInstanceOf[RemoteCache[Int, Book]]
      Future {
         val c = new AtomicInteger(0)
         val executor = Executors.newScheduledThreadPool(1)
         executor.scheduleAtFixedRate(new Runnable {
            override def run() = {
               val id = c.incrementAndGet()
               println(s"Adding book $id")
               cache.put(id, new Book(s"author-$id", "2015", s"title-$id"))
            }
         }, 0, 100, TimeUnit.MILLISECONDS)
      }
   }

   def main(args: Array[String]) {
      val infinispanHost = args(0)

      Logger.getLogger("org").setLevel(Level.WARN)

      // Begin by defining a new Spark configuration and creating a Spark context from this.
      val conf = getSparkConf("example-book-Stream")
      // val conf = new SparkConf().setAppName("example-RDD-scala")
      val sc = new SparkContext(conf)

      // Create the Properties instance, containing the JBoss Data Grid node and cache name.
      val properties = new Properties
      properties.put("infinispan.client.hotrod.server_list", infinispanHost + ":11222")
      properties.put("infinispan.rdd.cacheName", "exampleCache")


     populateBook(infinispanHost)

      // RDDs will be emitted every 1 second
      val ssc = new StreamingContext(sc, Seconds(1))

      val stream = new InfinispanInputDStream[String, Book](ssc, StorageLevel.MEMORY_ONLY, properties)

      // Filter only created entries
      val createdBooksRDD = stream.filter { case (_, _, t) => t == Type.CLIENT_CACHE_ENTRY_CREATED }

      // Aggregates Books on each RDD in an interval
      val windowedRDD: DStream[Long] = createdBooksRDD.count().reduceByWindow(_ + _, Seconds(30), Seconds(10))

      // Print counts per RDD
      windowedRDD.foreachRDD { rdd => println(rdd.reduce(_ + _)) }

      // Start the processing
      ssc.start()
      ssc.awaitTermination()

   }

}

