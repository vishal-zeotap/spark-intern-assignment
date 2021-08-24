import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row


object Driver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val schemaText = spark.sparkContext.wholeTextFiles("./src/main/resources/outputSchema.avsc").collect()(0)._2
    val inputPath1 = "test_data/sample1"
    val inputPath2 = "test_data/sample2"
    val inputPath3 = "test_data/sample3"
    val inputPath4 = "test_data/sample4"


    val df_4 = spark.read.option("avroSchema", schemaText).avro(inputPath1)
    val df2 = spark.read.option("avroSchema", schemaText).avro(inputPath2)
    val df3 = spark.read.option("avroSchema", schemaText).avro(inputPath3)
    val df_212 = df2.union(df3);
    val df_316 = spark.read.option("avroSchema", schemaText).avro(inputPath4)

    val priority: Map[String, List[Int]] = Map(
      ("Gender", List(4, 212, 316)),
      ("Device", List(212, 4, 316)),
      ("Age", List(316, 4, 212)),
      ("Zipcode", List(4, 212, 316))
    )

    def age_group_udf: UserDefinedFunction = udf((age: Int) => (
      if (age <= 18) 18
      else if (age > 18 && age <= 25) 25
      else if (age > 25 && age <= 35) 35
      else if (age > 35 && age <= 45) 45
      else if (age > 45 && age <= 55) 55
      else if (age > 55 && age <= 65) 65
      else 75
      ))

    //        val df = inputDf.orderBy("Identifier","DpId")

    val df = df_4.join(df_212.select("Identifier"), "Identifier").join(df_316.select("Identifier"), "Identifier")

    val overlaps = df_4.join(df_316.select("Identifier"), "Identifier").count()
    val id_count = df.count()
    val zip_count = df.groupBy("Zipcode").count()
    val device_count = df.groupBy("Device").count()
    val age_count = df.groupBy("Age").count()

    val withAgeGroup = df.withColumn("Age_Group", age_group_udf(col("Age")))
    val age_count_new = withAgeGroup.groupBy("Age_Group").count()
    val gender_count_new = withAgeGroup.groupBy("Gender").count()

    withAgeGroup.show();

  }
}


//{
//    "gender": [
//    4,
//    212,
//    316
//    ],
//    "age": [
//    212,
//    4,
//    316
//    ],
//    "zipcode": [
//    316,
//    212,
//    4
//    ]
//}