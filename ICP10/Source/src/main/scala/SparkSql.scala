import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.SparkSession

object SparkSql {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("groupProject")
      .getOrCreate()

    val SQLContext = spark.sqlContext

    val filepath = "survey.csv"

    //  1.	Import the dataset and create data frames directly on import.
    val df = SQLContext.read.option("header", true).csv(filepath)

    df.show(20)
    df.createOrReplaceTempView("survey")

    df.write.mode("overwrite").option("header","true").csv("output")

    // 3. Check for Duplicate records in the dataset.
    val distinctDF = df.distinct()
    println(distinctDF.count()+"some"+ df.count())

    val Texas_df = df.filter("state LIKE 'TX'")
    Texas_df.createOrReplaceTempView("texsurvey")
    val Illinois_df = df.filter("state LIKE 'IL'")
    Illinois_df.createOrReplaceTempView("illisurvey")


    // 4. Apply Union operation on the dataset and order the output by Country
    // Name alphabetically.
    Texas_df.show(20)
    Illinois_df.show(20)

    SQLContext.sql("SELECT * FROM texsurvey " +
    "UNION " +
    "SELECT * FROM illisurvey ORDER BY COUNTRY").show(20)


    //5. Use Groupby Query based on treatment.
    SQLContext.sql("SELECT treatment, count(*) FROM survey GROUP BY treatment").show(20)


    // Part – 2

    // 1. Apply the basic queries related to Joins and aggregate functions (at least 2)

    val femaleDF = df.filter("Gender LIKE 'f%' OR Gender LIKE 'F%' ")
    val maleDF   = df.filter("Gender LIKE 'm%' OR Gender LIKE 'M%' ")

    val df1 = femaleDF.select("Age" ,"Country","Gender","state","family_history")
    val df2 = maleDF.select("Age" ,"Country","Gender","state", "benefits")
    val jointdf = df1.join(df2, df1("Country") === df2("Country"), "inner")
    jointdf.show(false)

    val udf = df2.union(df1)
    val uniondf =udf.withColumn("Age", udf.col("Age").cast(DataTypes.IntegerType))
    uniondf.orderBy("Country").show(200)

    uniondf.groupBy("Country").count().show()
    uniondf.groupBy("Country").mean("Age").show()

    // 2. Write a query to fetch 13th Row in the dataset.
    println(df.take(13).last)



  }

}
