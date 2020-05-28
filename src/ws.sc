import scala.io.Source

def orderBy(x:Map[Int,Int]):Seq[Int]= x.toSeq.sortBy(_._1).map(_._2)

val data = Source.fromFile("C:\\Users\\Ivan\\IdeaProjects\\SparkLab01\\u.data")
val records = (for (line <- data.getLines) yield (line.split("\t")(1) toInt, line.split("\t")(2) toInt)) toList
val itemsRaiting = records groupBy(_._1) //first group by
val allItemsRaiting = for ((k,v) <- itemsRaiting) yield (k, v.groupBy(_._2).mapValues(_.size)) //second group by
val totalRaitings = (allItemsRaiting toList) flatMap(_._2)
val total = totalRaitings groupBy(_._1) mapValues(_.map(_._2).sum) //third group by