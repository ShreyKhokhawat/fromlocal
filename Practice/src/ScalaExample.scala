/*import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object ScalaExample {
  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder()
      .appName("ScalaExample")
      .master("local").getOrCreate()

    //Read the input file to a dataframe

    val df1 = sc.read.format(source = "csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("test.csv")

    //df1.printSchema()

    val df2 = df1.columns.foreach(println)
    println("Hello Worlds")

    val df = sc.range(500).toDF("number")
    df.select(df.col("number") +1).show(false)
  
  val account = df1.account(id, name, balance)

    case class customer(cust_id: Int, store_id: String, balance: Float)
    case class account(customer_id)
    
  }

}
*/