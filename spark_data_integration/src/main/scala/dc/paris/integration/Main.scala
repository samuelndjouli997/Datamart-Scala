package dc.paris.integration

import org.apache.spark.sql.SparkSession

import java.io.File



object Main extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Data Integration")
    .master("local[*]")
    .config("fs.s3a.access.key", "feYIrRkftrx645QsECKw") // A renseigner
    .config("fs.s3a.secret.key", "2RDLb99JSmwd8Zj1JcNPvK6t2LbaQnHSDp1cctRW") // A renseigner
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "1000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()
}
