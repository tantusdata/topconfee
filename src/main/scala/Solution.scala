import java.util.UUID

import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object Solution {

  def main(args: Array[String]) {
    //TODO: set desired output directory
    val output: String = "/tmp/output/"
    //TODO: change this to your input directory
    val input: String = "/Users/marcinszymaniuk/tmp/input2/"

    val conf = new SparkConf()
      .setAppName("Demo")
      //TODO: change "1" to "4" to see what will be the difference (if any)
      .setMaster("local[4]")
    val spark = new SparkContext(conf)
    val fileSystem = FileSystem.get(spark.hadoopConfiguration)
    fileSystem.delete(new Path(output), true)


    val rdd = spark.textFile(input)
    rdd
      .map(mLine(_))
      .saveAsTextFile(output)
  }

  /*
  Our buisness logic - mapping from input csv line to output csv line
   */
  def mLine(line: String) = {
    val parsed = new CSVParser(',').parseLine(line)
    s"${UUID.randomUUID()},$parsed(0),$parsed(3)"
  }

}

