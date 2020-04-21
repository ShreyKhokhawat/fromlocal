import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext


object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    //Read the input file to a dataframe

    val file1 = sc.textFile("textfile1.txt")
    val file2 = sc.textFile("textfile2.txt")
    val count = file1.flatMap(x => x.split("|")).map(x => (x, 1)).reduceByKey(_ + _)
    
    count.collect.foreach(println)
/*    val kf1 = file1.map(x => x.split("|")).map(x => (x(0), x))
    val kf2 = file2.map(x => x.split("|")).map(x => (x(0), x))

    val word = file1.map(x => x.split("|")).foreach(println)
*/  
    
  val abcd=file1.map(x=>x.split("\\|")).map(x=>List(x(0),x(1),x(2)))
  .foreach(println)
  
  }

}
