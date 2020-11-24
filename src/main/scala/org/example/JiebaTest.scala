import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

object JiebaTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd=sc.textFile("/home/hadoop/hadoop/test/jieba.txt")
      .map { x =>
        println(x)
        println("orzorz")
        val str = if (x.length > 0)
          new JiebaSegmenter().sentenceProcess(x)
        str.toString
      }.top(10).foreach(str => {
      val list = str.split(", ")
      println(list.mkString(" "))
    })
  }

}