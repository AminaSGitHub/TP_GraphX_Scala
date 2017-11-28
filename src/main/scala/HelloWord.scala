import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object HelloWord {
  def main(args: Array[String]) : Unit = {
    println("hello world !")
    val conf = new SparkConf()
      .setAppName("Stats Graph")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger //import de org.apache.log4j//
    rootLogger.setLevel(Level.ERROR)    // pour éviter spark trop verbeux
   case class User(name: String, age : Int, job : String, sex : String)

    val nodes : RDD[(VertexId, User)] =
      sc.textFile("./src/main/ressources/users.txt")
        .filter(!_.startsWith("#"))
        .map { line =>
          val row = line split '\t'
           (row(0).toLong, User(row(1).toString, row(2).toInt, row(3).toString, row(4)))
        }
    val links : RDD[Edge[String]] =
      sc.textFile("./src/main/ressources/relationships.txt")
        .filter(!_.startsWith("#"))
        .map { line =>
          val row = line split '\t'
          Edge(row(0).toLong, row(1).toLong, row(2).toString)
      }


  }
}

