package fr.cgi.app

class MainTest  {

  @Test
  def mainTest(): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Evaluation")
      .config(new SparkConf().setMaster("local[*]"))
      .getOrCreate()

	// 1 - )
    val moviesDF = sparkSession.read.option("header", "true").option("delimiter", ",").csv("src/test/resources/movies.csv")
    val ratingsDF = sparkSession.read.option("header", "true").option("delimiter", ",").csv("src/test/resources/ratings.csv")
    val creditsDF = sparkSession.read.option("header", "true").option("delimiter", ",").csv("src/test/resources/credits.csv")

	// 2 - )
	moviesDF.show()
	ratingsDF.show()
	creditsDF.show()
	
	// 3 - )
    
	var englishMoviesDF = moviesDF.filter(moviesDF("original_language") === "en")
	
	// 4 - )
    var moviesRatingsDF = englishMoviesDF.join(ratingsDF, englishMoviesDF("id") === ratingsDF("movieId"), "left")

    // 5 - ) 6 - )
    moviesRatingsDF = moviesRatingsDF.drop("belongs_to_collection", "homepage", "imdb_id", "original_language", "popularity", "poster_path", "production_companies", "production_countries", "release_date", "revenue", "runtime", "spoken_languages", "status", "tagline", "title", "video", "vote_average", "vote_count").withColumnRenamed("original_title", "title")

    // 7 - )
    showLinesAndSchema(moviesRatingsDF)

	// 8 - )
	moviesRatingsDF = moviesRatingsDF.orderBy(desc("rating"))
	moviesRatingsDF.write.csv("src/test/resources/MovieswithRatings")

	// 9 - ) 
    val moviesAndCredits = moviesRatingsDF.join(creditsDF, moviesRatingsDF("id") === creditsDF("id"), "left")

	// 9.1 - ) 
    moviesAndCredits.select("cast").where(moviesAndCredits("title") === "Jumanji").show(false)

	// 9.2 - ) 
    moviesAndCredits.createOrReplaceTempView("moviesAndCredits")

    val sqlDF = sparkSession.sql("SELECT * FROM moviesAndCredits WHERE title = 'Jumanji'").show(false)

	// 10 - ) 'Main idea'
    moviesAndCredits.select(to_date(from_unixtime(moviesAndCredits("timestamp")))).show(2)
  }

  def showLinesAndSchema(dataframe: DataFrame) : Unit = {
    dataframe.printSchema()
    dataframe.show(5)
  }

}
