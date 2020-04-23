
// Each library has its significance, I have commented when it's used
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row

object DFExample {

	Logger.getLogger("org").setLevel(Level.ERROR)
	val conf = new SparkConf().setAppName("textfileReader").setMaster("local")
	val sc = new SparkContext(conf)

	val sqlContext = new SQLContext(sc)
	def main (args:Array[String]): Unit = {
			System.setProperty("hadoop.home.dir", "c:/hadoop/");
			val squidString = sc.textFile("squid.txt")
					val squidHeader = "time duration client_add result_code bytes req_method url user hierarchy_code type"

					val schema = StructType(squidHeader.split(" ")
							.map(fieldName => StructField(fieldName,StringType, true)))

					val rowRDD = squidString.map(_.split(" ")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5) , x(6) , x(7) , x(8), x(9)))
					val squidDF = sqlContext.createDataFrame(rowRDD, schema)

					squidDF.registerTempTable("squid")

					val allrecords = sqlContext.sql("select * from squid")

					allrecords.show(5,false)
					allrecords.write.mode(SaveMode.Overwrite).saveAsTable("allrecords")
					allrecords.printSchema()

					// Something like this for date, integer and string conversion

					// To have multiline sql use triple quotes
					val transformedData = sqlContext.sql("""  -- multiline sql
							select from_unixtime(time) as time, -- you can apply to_date
							cast(duration as int) as duration,  -- casting to integer
							cast (req_method as string) as req_method from allrecords  -- casting to string just to explain
							where type like '%application%' -- filtering
							""")

					// To print schema after transformation, you can see new fields data types
					transformedData.printSchema()
					transformedData.show()

					sc.stop()
	}
}