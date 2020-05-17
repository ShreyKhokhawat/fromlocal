
// Each library has its significance, I have commented when it's used
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.Pivot
import org.apache.spark.sql.functions._

object RWA {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RWA").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    System.setProperty("hadoop.home.dir", "c:/hadoop/");

    val FlatModel = sc.textFile("Flat_Model.txt")
    val FlatModel_Header = "Report|Header_Nm|Header_Desc"
    val FlatModelSchema = StructType(FlatModel_Header.split("\\|").map(fieldName => StructField(fieldName, StringType, true)))
    val FlatModelRDD = FlatModel.map(_.split("\\|")).map(x => Row(x(0), x(1), x(2)))

    val FlatModelDF = sqlContext.createDataFrame(FlatModelRDD, FlatModelSchema)

    val RWA = sc.textFile("RWA.txt")
    val RWA_Header = "Report|Header_Nm|Header_Value"
    val RWASchema = StructType(RWA_Header.split("\\|").map(fieldName => StructField(fieldName, StringType, true)))
    val RWARDD = RWA.map(_.split("\\|")).map(x => Row(x(0), x(1), x(2)))

    val RWADF = sqlContext.createDataFrame(RWARDD, RWASchema)

    val JoinedRDD = RWADF.join(FlatModelDF, (RWADF("Report") === FlatModelDF("Report")) and RWADF("Header_Nm") === FlatModelDF("Header_nm"), "inner")
      .select(FlatModelDF("Report"), FlatModelDF("Header_Nm"), FlatModelDF("Header_Desc"), RWADF("Header_Value"))
      .toDF("Report", "Header_Nm", "Header_Desc", "Header_Value")

    val result = JoinedRDD.groupBy("Report")
      .pivot("Header_Desc")
      .agg(expr("coalesce(first(Header_Value),\"NA\")"))
      .show()

    FlatModelRDD.foreach(println)
    RWARDD.foreach(println)
    sc.stop()
  }
}