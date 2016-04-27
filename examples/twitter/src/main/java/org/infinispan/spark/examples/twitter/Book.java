package org.infinispan.spark.examples.twitter;

import java.io.Serializable;

public class Book implements Serializable {

   private String author;
   private String year;
   private String title;

   public Book(String author, String year, String title) {
      this.author = author;
      this.year = year;
      this.title = title;
   }

   public String getAuthor() {
      return author;
   }

   public String getYear() {
      return year;
   }

   public String getTitle() {
      return title;
   }
}
