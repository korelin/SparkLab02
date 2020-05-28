import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.log4j.{Logger, Level}

case class Histogram(hist_film: Seq[Int], hist_all: Seq[Int])

object Lab02 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
  //val lines = Source.fromFile("file.json").getLines.toList
  //lines.take(20).foreach(println)
  val df = spark.read.json("file.json")//.json("file.json")
  import spark.implicits._
  val film = df.filter('id === 49).select('name)
  film.show()
  val corpus = df.filter('lang === "en").select(col("*"))
  corpus.show()
  val tokenizer = new Tokenizer().setInputCol("desc").setOutputCol("words")
  val wordsData = tokenizer.transform(corpus)
  val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
  val featurizedData = hashingTF.transform(wordsData)
  featurizedData.show()
  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val idfModel = idf.fit(featurizedData)
  val rescaledData = idfModel.transform(featurizedData)
  rescaledData.select("desc","features").show()
  spark.stop
}

//  def orderBy(x:Map[Int,Int]):Seq[Int]= x.toSeq.sortBy(_._1).map(_._2)
//
//  val data = Source.fromFile("u.data")
//  val records = (for (line <- data.getLines) yield (line.split("\t")(1) toInt, line.split("\t")(2) toInt)) toList
//  val itemsRaiting = records groupBy(_._1) //first group by
//  val allItemsRaiting = for ((k,v) <- itemsRaiting) yield (k, v.groupBy(_._2).mapValues(_.size)) //second group by
//  val totalRaitings = (allItemsRaiting toList) flatMap(_._2)
//  val total = totalRaitings groupBy(_._1) mapValues(_.map(_._2).sum) //third group by
//  val hist = Histogram(orderBy(allItemsRaiting(64)), orderBy(total))
//  new PrintWriter("lab01.json") { write(hist.asJson.toString); close() }