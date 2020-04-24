import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object readJSON {
  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().appName("DFExample").master("local").getOrCreate()

    //Read the input file to a dataframe

    val df = sc.read.json("people.json")
    df.show

   df.groupBy("age").count().show()
  }
}
