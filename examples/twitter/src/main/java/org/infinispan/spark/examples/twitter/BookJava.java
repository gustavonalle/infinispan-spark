package org.infinispan.spark.examples.twitter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.infinispan.spark.rdd.InfinispanJavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BookJava {

   public static void main(String[] args) throws Exception {
      String master = args[0];

      // Begin by defining a new Spark configuration and creating a Spark context from this.
      SparkConf conf = Sample.getSparkConf("BookJava");
      JavaSparkContext jsc = new JavaSparkContext(conf);

      // Create the Properties instance, containing the JBoss Data Grid node and cache name.
      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", master + ":11222");
      properties.put("infinispan.rdd.cacheName", "exampleCache");

      // Create an RDD of Books
      JavaRDD<Book> books = jsc.parallelize(Arrays.asList(new Book("George R. Martin", "2000", "A feast for crows"), new Book("Stephen King", "1996", "IT")));
      JavaPairRDD<Long, Book> bookMap = books.zipWithIndex().mapToPair(Tuple2::swap);

      // Write the JavaPair RDD to the Data Grid
      InfinispanJavaRDD.write(bookMap, properties);

      // Create an RDD from the DataGrid
      JavaPairRDD<Integer, Book> exampleRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, properties);

      JavaRDD<Book> booksRDD = exampleRDD.values();

      System.out.println(booksRDD.count());

      // Create a SQLContext, registering the data frame and table
      SQLContext sqlContext = new SQLContext(jsc);
      DataFrame dataFrame = sqlContext.createDataFrame(booksRDD, Book.class);
      dataFrame.registerTempTable("books");

       // Run the Query and collect results
      List<Row> rows = sqlContext.sql("SELECT author, count(*) as a from books WHERE author != 'N/A' GROUP BY author ORDER BY a desc").collectAsList();

      System.out.println(rows.get(0));
   }

}