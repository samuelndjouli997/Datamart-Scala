package dc.paris.integration

//package dev.draft

import org.jsoup.Jsoup
import org.apache.spark.sql.SparkSession
import scala.jdk.CollectionConverters.CollectionHasAsScala
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.spark.sql.DataFrame


object Main extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Data Integration")
    .master("local[*]")
    .config("fs.s3a.access.key", "7D6skp7yIzatGvMuzXVL") // A renseigner
    .config("fs.s3a.secret.key", "yeDJJZHTtC7DRpXai997n60OzrOLDtq7RfbIhCTI") // A renseigner
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "1000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()

  def getYellowTripDataLinks(year: Int, months: List[Int]): List[String] = {
    val url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    val doc = Jsoup.connect(url).get()

    val formattedMonths = months.map(m => f"$m%02d")
    val expectedFragments = formattedMonths.map(m => s"yellow_tripdata_${year}-$m.parquet")

    val links = doc.select("a").asScala

    links.flatMap { link =>
      val href = link.attr("href")
      expectedFragments.find(fragment => href.contains(fragment)).map(_ => href)
    }.toList
  }

  def downloadFile(fileUrl: String, destinationPath: String): Unit = {
    val url = new URL(fileUrl)
    val inputStream = url.openStream()
    val outputPath = Paths.get(destinationPath)

    Files.createDirectories(outputPath.getParent) // Crée le dossier s'il n'existe pas
    Files.copy(inputStream, outputPath, StandardCopyOption.REPLACE_EXISTING)
    inputStream.close()
  }

  val year = 2024
  val months = List(10, 11, 12)
  val links = getYellowTripDataLinks(year, months)

  for (link <- links) {
    val fileName = link.split("/").last
    val localPath = s"data/raw/$fileName"

    println(s"Téléchargement de $link vers $localPath ...")
    downloadFile(link, localPath)
    println(s"$fileName téléchargé avec succès dans data/raw/")
  }

  for (month <- months) {
    val formattedMonth = f"$month%02d"
    val fileName = s"yellow_tripdata_${year}-$formattedMonth.parquet"
    val s3Path = s"s3a://nyc-taxi/$fileName"

    try {
      val df = spark.read.parquet(s3Path)

      // Optionnel : tu peux sélectionner certaines colonnes ou filtrer
      // val cleanedDf = df.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count")

      // Écriture dans la base PostgreSQL
      writeToPostgres(df, s"yellow_tripdata_${year}_$formattedMonth")

    } catch {
      case e: Exception =>
        println(s"❌❌❌❌❌❌❌❌ Erreur pour $fileName : ${e.getMessage}")
    }
  }

  def writeToPostgres(df: DataFrame, tableName: String): Unit = {
    val jdbcUrl = "jdbc:postgresql://localhost:15432/postgres"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "postgres")
    connectionProperties.setProperty("password", "admin")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    df.write
      .mode("overwrite") // ou append si tu veux ajouter à chaque fois
      .jdbc(jdbcUrl, tableName, connectionProperties)

    println(s"✅✅✅✅✅✅✅✅✅✅✅ Données écrites dans la table $tableName avec succès.")
  }


}