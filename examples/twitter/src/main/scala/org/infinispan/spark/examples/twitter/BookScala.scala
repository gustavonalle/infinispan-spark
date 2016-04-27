package org.infinispan.spark.examples.twitter

import java.util.Properties
import org.infinispan.spark._
import org.infinispan.spark.examples.twitter.Sample.getSparkConf
import org.infinispan.spark.rdd.InfinispanRDD

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object BookScala {

   def main(args: Array[String]) {
      val infinispanHost = args(0)

      // Begin by defining a new Spark configuration and creating a Spark context from this.
      val conf = getSparkConf("example-RDD")
      // val conf = new SparkConf().setAppName("example-RDD-scala")
      val sc = new SparkContext(conf)

      // Create the Properties instance, containing the JBoss Data Grid node and cache name.
      val properties = new Properties
      properties.put("infinispan.client.hotrod.server_list", infinispanHost + ":11222")
      properties.put("infinispan.rdd.cacheName", "exampleCache")

      // Create an RDD of Books
      val bookOne = new Book("Linux Bible", "Negus, Chris", "2015")
      val bookTwo = new Book("Java 8 in Action", "Urma, Raoul-Gabriel", "2014")

      val sampleBookRDD = sc.parallelize(Seq(bookOne,bookTwo))
      val pairsRDD = sampleBookRDD.zipWithIndex().map(_.swap)


      // Write the Key/Value RDD to the Data Grid
      pairsRDD.writeToInfinispan(properties)

      // Create an RDD from the DataGrid cache
      val exampleRDD = new InfinispanRDD[Integer, Book](sc, properties)

      val booksRDD = exampleRDD.values

      println(booksRDD.count)

      // Create a SQLContext, register a data frame and table
      val sqlContext = new SQLContext(sc)
      val dataFrame = sqlContext.createDataFrame(booksRDD, classOf[Book])
      dataFrame.registerTempTable("books")

      // Run the Query, collect and print results
      val rows = sqlContext.sql("SELECT author, count(*) as a from books WHERE author != 'N/A' GROUP BY author ORDER BY a desc").collect()

      println(rows(0))

   }

}

