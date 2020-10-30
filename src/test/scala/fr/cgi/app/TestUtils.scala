package fr.lactalis.services

import java.sql.Timestamp

import org.junit.Test
import fr.lactalis.tools.Utils._
import fr.lactalis.tools.Formate._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType}


class TestUtils  {

  @Test
  def testGetPathDate(): Unit ={
    val result = getPathDate("2019/06/03")
    assert ("year=2019/month=06/day=03".equals(result))
  }

  @Test
  def testGDate(): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Evaluation")
      .config(new SparkConf().setMaster("local[*]"))
      .getOrCreate()

    val moviesDF = sparkSession.read.option("header", "true").option("delimiter", ",").csv("src/test/resources/movies.csv")
    val ratingsDF = sparkSession.read.option("header", "true").option("delimiter", ",").csv("src/test/resources/ratings.csv")
    val creditsDF = sparkSession.read.option("header", "true").option("delimiter", ",").csv("src/test/resources/credits.csv")

    //var moviesRatingsDF = moviesDF.join(ratingsDF, moviesDF("id") === ratingsDF("movieId"), "left")

  //  moviesRatingsDF.show()

  //  moviesRatingsDF = moviesRatingsDF.drop("belongs_to_collection", "homepage", "imdb_id", "original_language", "popularity", "poster_path", "production_companies", "production_countries", "release_date", "revenue", "runtime", "spoken_languages", "status", "tagline", "title", "video", "vote_average", "vote_count")
    //moviesRatingsDF = moviesRatingsDF.withColumnRenamed("original_title", "title")

    //moviesDF.show()
  //  ratingsDF.show()

    var englishMoviesDF = moviesDF.filter(moviesDF("original_language") === "en")

    //englishMoviesDF.filter("rating is not null").sort(desc("rating")).show(5)
    //frenchMoviesDF.filter("rating is not null").sort(desc("rating")).show(6)

   // englishMoviesDF.sort("rating").show()
   var moviesRatingsDF = englishMoviesDF.join(ratingsDF, englishMoviesDF("id") === ratingsDF("movieId"), "left")

    showLinesAndSchema(moviesRatingsDF)
    moviesRatingsDF = moviesRatingsDF.drop("belongs_to_collection", "homepage", "imdb_id", "original_language", "popularity", "poster_path", "production_companies", "production_countries", "release_date", "revenue", "runtime", "spoken_languages", "status", "tagline", "title", "video", "vote_average", "vote_count").withColumnRenamed("original_title", "title")


    moviesRatingsDF = moviesRatingsDF.orderBy(desc("rating"))
    showLinesAndSchema(moviesRatingsDF)

   // moviesRatingsDF.write.csv("src/test/resources/MovieswithRatings.csv")

    val moviesAndCredits = moviesRatingsDF.join(creditsDF, moviesRatingsDF("id") === creditsDF("id"), "left")

    moviesAndCredits.select("cast").where(moviesAndCredits("title") === "Jumanji").show(false)

    moviesAndCredits.createOrReplaceTempView("moviesAndCredits")

    val sqlDF = sparkSession.sql("SELECT * FROM moviesAndCredits").show(false)


    assert(1 == 1)

  }

  def showLinesAndSchema(dataframe: DataFrame) : Unit = {
    dataframe.printSchema()
    dataframe.show(5)
  }

  @Test
  def testSlidingMonh() : Unit ={
    val result = slidingMonth("2018/05", 1)
    val array = Array("year=2018/month=05","year=2018/month=04")
    assert(array.sameElements(result))
  }


  @Test
  def testGetPathDate2(): Unit ={
    val result = getPathDate("2019/06")
    assert ("year=2019/month=06".equals(result))
  }

  @Test
  def testSetPathDate(): Unit ={
    val result = setPathDate("2019/01/14")
    assert (result == "year=2019/month=01/day=2019-01-14/")
  }

  @Test
  def testConvertDate(): Unit ={
    assert("2019-01-10 00:14:30.0".equals(convertStringToTimestamp("20190110001430").toString) &&
      "2019-01-10 00:00:00.0".equals(convertStringToTimestamp("20190110").toString)
      //"0002-11-30 00:00:00.0".equals(convertDate("R454").toString)
    )
  }


  @Test
  def  testConvertDouble(): Unit ={
    assert("1.0".equals(convertDouble("1.0").toString) &&
      "1.0".equals(convertDouble("1,0").toString) &&
      "1234.01".equals(convertDouble("1234,01").toString)
    )
  }
}
