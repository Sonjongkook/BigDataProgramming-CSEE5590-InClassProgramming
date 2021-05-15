import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame



object ICP13 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Graph Frames")
      .config("spark.master", "local")
      .getOrCreate()

    val trip_data = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("201508_trip_data.csv")

    val station_data = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("201508_station_data.csv")


    val input = station_data.select("name", "landmark", "lat", "long", "dockcount").withColumnRenamed("name", "id")
    input.show()
    val output = trip_data.select("Start Station", "End Station", "Duration").withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst").withColumnRenamed("Duration", "relationship")
    output.show()


    val g = GraphFrame(input, output)

    // trianglecount
    println("Triangle count")
    val TC = g.triangleCount.run()
    TC.select("id", "count").show()
    println("Triangle count")

    // Shortest Path
    val SP = g.shortestPaths.landmarks(Seq("San Jose Civic Center", "Market at 4th")).run
    println("shortest path")
    SP.orderBy("id").show(10, false)

    // Pagerank
    val PR = g.pageRank.resetProbability(0.15).maxIter(10).run()
    println("Pagerank for vertices")
    PR.vertices.show()
    println("Pagerank for edges")
    PR.edges.show()

    // BFS
    val BFS = g.bfs.fromExpr("id = 'Mezes Park'").toExpr("dockcount < 15").run()
    println("BFS")
    BFS.show(truncate = false)

    //Save vertices and edges
    g.vertices.write.csv("vertices")
    g.edges.write.csv("edges")


  }
}
