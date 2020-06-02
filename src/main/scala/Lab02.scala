import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

object UdfUtils extends Serializable {
  def calcNorm(vectorA: SparseVector): Double = {
    var norm = 0.0
    for (i <-  vectorA.indices){ norm += vectorA(i)*vectorA(i) }
    math.sqrt(norm)
  }
  val calcNormDF = udf[Double,SparseVector](calcNorm)

  def cosineSimilarity(vectorA: SparseVector, vectorB:SparseVector,normASqrt:Double,normBSqrt:Double) :(Double) = {
    var dotProduct = 0.0
    for (i <-  vectorA.indices){ dotProduct += vectorA(i) * vectorB(i) }
    val div = (normASqrt * normBSqrt)
    if( div == 0 ) (0)
    else (dotProduct / div)
  }
  val calcCosineUDF = udf[Double,SparseVector,SparseVector,Double,Double](cosineSimilarity)
}

object Analizer {
  def getFeatures(df:DataFrame)(implicit spark:SparkSession): DataFrame = {
    import spark.implicits._
    val corpus = df.select('id, 'name, regexp_replace('desc,lit("""[\p{Punct}]"""), lit(" ")).alias("desc"))
    val tokenizer = new Tokenizer()
      .setInputCol("desc")
      .setOutputCol("wordsR")
    val wordsDataRaw = tokenizer.transform(corpus)
    val remover = new StopWordsRemover()
      .setInputCol("wordsR")
      .setOutputCol("words")
    val wordsData = remover.transform(wordsDataRaw)
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(10000)
    val featurizedData = hashingTF.transform(wordsData)
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    val normalized = rescaledData.withColumn("norm", UdfUtils.calcNormDF(col("features")))
    normalized
  }

  def lookUp(id:Int, df:DataFrame)(implicit spark:SparkSession) = {
    import spark.implicits._
    val filmLang = df.filter('id === id).select('lang)
        .collect().map(r => r(0)).toList.head.toString
    val corpus = df.filter('lang === filmLang)
    val featuredCorpus = getFeatures(corpus)
    def query = featuredCorpus.filter('id === id)//df.filter('id === id)
    val featuredQuery = query//getFeatures(query)
      .withColumnRenamed("features", "features2")
      .withColumnRenamed("norm", "norm2")
      .drop("name")
      .drop("id")
    val cross = featuredQuery.crossJoin(featuredCorpus)
    val cosine = cross.withColumn("similarity", UdfUtils.calcCosineUDF(col("features"), col("features2"), col("norm"), col("norm2")))
    val similars = cosine.sort(desc("similarity"),'name,'id).select('id).limit(11).collect().takeRight(10).toList
    similars.map(r => r(0))
  }
}

object Lab02 extends App{
  def listToString(l:List[Any])={
    "[\n    " + l.mkString(",\n    ") + "\n  ]"
  }
  def mapToString(m:Map[Int, List[Any]])={
    val key = m.keys.head
    "\n  \"" + key + "\" : " + listToString(m(key))
  }
  override def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("spark-test").setMaster("local")
      val sc = new SparkContext(conf)
      implicit val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
      val df = spark.read.json("DO_record_per_line.json")//.json("DO_record_per_line.json")//.json("file.json")args(0)
      val ids = args(0).split(",").map(r => r.toInt) //"23325,15072,24506,3879,1067,17019"args(1)
      val results = ids.map(id => Map(id -> Analizer.lookUp(id, df)))
      //results.foreach(println)
      //println("{" + results.map(r => mapToString(r)).mkString(",") + "\n}")
      new PrintWriter("lab02.json") { write("{" + results.map(r => mapToString(r)).mkString(",") + "\n}"); close() }
      spark.stop
    }
}


