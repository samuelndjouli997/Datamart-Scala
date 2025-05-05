lazy val root = (project in file(".")).
  settings(
    name := "spark_data_integration",
    version := "0.0.1",
    scalaVersion := "2.13.15",
    libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.5"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"

/*
libraryDependencies += "org.scalameta" %% "munit" % "1.1.0"
*/
libraryDependencies += "org.scalameta" %% "scalameta" % "4.13.4"
libraryDependencies += "org.jsoup" % "jsoup" % "1.18.3"
libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "3.1.2"
libraryDependencies += "org.seleniumhq.selenium" % "selenium-java" % "4.29.0"