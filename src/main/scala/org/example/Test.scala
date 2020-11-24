
import com.alibaba.fastjson.{JSON, JSONObject}
import com.huaban.analysis.jieba.JiebaSegmenter
import com.mongodb.casbah.Imports.MongoClientURI
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Test {
  def main(args: Array[String]) {
    /*val jars = Array("target/untitled3-1.0-SNAPSHOT.jar")
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("spark://hadoopmaster:7077")
      .setJars(jars)*/
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    lazy val uri = MongoClientURI("mongodb://localhost:27017/")
    lazy val mongoClient = MongoClient(uri)
    lazy val db = mongoClient("streaming")

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("/home/hadoop/hadoop/testCheckpoint")
    val sc = ssc.sparkContext
    val files = ssc.textFileStream("hdfs://172.19.241.194:9000/user/hadoop/")
    //val files = ssc.textFileStream("/home/hadoop/hadoop/out/")
    val lines = files.flatMap(_.split(System.getProperty("line.separator")))

    //filter listened files as userInfo and commentInfo
    val userInfo = lines.filter(rdd => {
      val obj = JSON.parseObject(rdd)
      try{
        val content = obj.getString("hometeam")
        if (content != null){
          true
        }else{
          false
        }
      }catch{
        case ex:Exception =>{
          false
        }
      }
    })
    val commentInfo = lines.filter(rdd => {
      val obj = JSON.parseObject(rdd)
      try{
        val content = obj.getString("content")
        if (content != null){
          true
        }else{
          false
        }
      }catch{
        case ex:Exception =>{
          false
        }
      }
    })

    val commentTime = commentInfo.map(rdd =>{
      val obj = JSON.parseObject(rdd)
      val time = obj.getString("created_at")
      val timeArray = time.split(" ")
      val date = timeArray(0)
      val hour = timeArray(1).substring(0,2)
      val hourInt = (hour.toInt) / 2 * 2
      ((date, hourInt.toString), 1)
    })

    val hometeam = userInfo.map(rdd => {
      val obj = JSON.parseObject(rdd)
      (obj.getString("hometeam"), 1)
    })

    val hometeamUsers = userInfo.map(rdd => {
      val obj = JSON.parseObject(rdd)
      val hometeam = obj.getString("hometeam")
      val username = obj.getString("username")
      val followerNumber = obj.getString("followers_total")
      val replyNumber = obj.getString("reply_total")
      (hometeam, (username, followerNumber.toInt, replyNumber.toInt))
    })

    val top5FollowedUsers = hometeamUsers.groupByKey().flatMap(line => {
      val topItem = line._2.toArray.sortBy(_._2)(Ordering[Int].reverse).take(5)
      val res = topItem.map(x => (line._1, x))
      res
    })

    top5FollowedUsers.foreachRDD(rdd => {
      rdd.foreach(item => {
        val team = item._1
        val username = item._2._1
        val followerNumber = item._2._2

        /*val uri = MongoClientURI("mongodb://localhost:27017/")
        val mongoClient = MongoClient(uri)
        val db = mongoClient("streaming")*/
        val coll_top5 = db("top5followers")

        val existedTop = coll_top5.find(MongoDBObject("team" -> team))
        val size = existedTop.count()

        if (size < 5){
          coll_top5.insert(MongoDBObject("team" -> team, "username" -> username, "followers" -> followerNumber))
        }else{
          var map:Map[String, Int] = Map()
          existedTop.foreach(e => {
            val existUsername = e.get("username").toString
            val existFollowers = e.get("followers").toString.toInt
            map += (existUsername -> existFollowers)
          })
          map += (username -> followerNumber)
          val seq = map.toSeq.sortBy(_._2)(Ordering[Int].reverse)
          coll_top5.remove(MongoDBObject("team" -> team))
          val maxIndex = if (seq.size <= 5)  seq.size - 1 else 4
          try{
            for(i <- 0 to maxIndex){
              coll_top5.insert(MongoDBObject("team" -> team, "username" -> seq(i)._1, "followers" -> seq(i)._2))
            }
          }catch{
            case ex:Exception =>{
              println(size + " " + team)
              println(seq)
              ex.printStackTrace()
            }
          }
        }
      })
    })

    val top5ReplyUsers = hometeamUsers.groupByKey().flatMap(line => {
      val topItem = line._2.toArray.sortBy(_._3)(Ordering[Int].reverse).take(5)
      val res = topItem.map(x => (line._1, x))
      res
    })

    top5ReplyUsers.foreachRDD(rdd => {
      rdd.foreach(item => {
        val team = item._1
        val username = item._2._1
        val replyNumber = item._2._2

        /*val uri = MongoClientURI("mongodb://localhost:27017/")
        val mongoClient = MongoClient(uri)
        val db = mongoClient("streaming")*/
        val coll_top5 = db("top5Replys")

        val existedTop = coll_top5.find(MongoDBObject("team" -> team))
        val size = existedTop.count()

        if (size < 5){
          coll_top5.insert(MongoDBObject("team" -> team, "username" -> username, "replys" -> replyNumber))
        }else{
          var map:Map[String, Int] = Map()
          existedTop.foreach(e => {
            val existUsername = e.get("username").toString
            val existReplys = e.get("replys").toString.toInt
            map += (existUsername -> existReplys)
          })
          map += (username -> replyNumber)
          val seq = map.toSeq.sortBy(_._2)(Ordering[Int].reverse)
          coll_top5.remove(MongoDBObject("team" -> team))
          val maxIndex = if (seq.size <= 5)  seq.size - 1 else 4
          try{
            for(i <- 0 to maxIndex){
              coll_top5.insert(MongoDBObject("team" -> team, "username" -> seq(i)._1, "replys" -> seq(i)._2))
            }
          } catch{
          case ex:Exception =>{
            println(size + " " + team)
            println(seq)
            ex.printStackTrace()
          }
        }
        }
      })
    })

    val region = userInfo.map(rdd => {
      val obj = JSON.parseObject(rdd)
      (obj.getString("region"), 1)
    })
    val gender = userInfo.map(rdd => {
      val obj = JSON.parseObject(rdd)
      (obj.getString("gender"), 1)
    })
    val username = userInfo.map(rdd => {
      val obj = JSON.parseObject(rdd)
      val name = obj.getString("username")
      val name_jieba = new JiebaSegmenter().sentenceProcess(name)
      val str = name_jieba.toString
      str.substring(1, str.length - 1)
    })
    val word = username.flatMap(str => str.split(", ")).map(word => (word, 1))

    //accumulate result in each batch
    val commentTimeStatistic = commentTime.updateStateByKey((seq:Seq[Int], option:Option[Int]) => {
      val sum = seq.sum
      Some(sum + option.getOrElse(0))
    })
    val hometeamStatistic = hometeam.updateStateByKey((seq:Seq[Int], option:Option[Int]) => {
      val sum = seq.sum
      Some(sum + option.getOrElse(0))
    })
    val regionStatistic = region.updateStateByKey((seq:Seq[Int], option:Option[Int]) => {
      val sum = seq.sum
      Some(sum + option.getOrElse(0))
    })
    val genderStatistic = gender.updateStateByKey((seq:Seq[Int], option:Option[Int]) => {
      val sum = seq.sum
      Some(sum + option.getOrElse(0))
    })
    val wordStatistic = word.updateStateByKey((seq:Seq[Int], option:Option[Int]) => {
      val sum = seq.sum
      Some(sum + option.getOrElse(0))
    })


    //connect to mongodb & save statics
    commentTimeStatistic.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        /*val uri = MongoClientURI("mongodb://localhost:27017/")
        val mongoClient = MongoClient(uri)
        val db = mongoClient("streaming")*/
        val coll_time = db("time")
        partitionOfRecords.foreach(record => {
          val time = record._1
          val date = time._1
          val hour = time._2
          val number = record._2
          val existTimeStatistic = coll_time.find(MongoDBObject("date" -> date))
          if (existTimeStatistic.size > 0){
            val old = existTimeStatistic.one()
            val str = old.get("array").toString
            val oldTotal = old.get("total").toString
            val array = str.substring(1, str.length - 1).split(",")
            val numberArray = for (e <- array) yield e.trim.toInt
            val index = hour.toInt / 2
            val total = oldTotal.toInt - numberArray(index) + number
            numberArray(index) = number
            coll_time.update(old, MongoDBObject("date" -> date, "total" -> total, "array" -> numberArray))
          }else{
            val numberArray = new Array[Int](12)
            numberArray(hour.toInt / 2) = number
            coll_time.insert(MongoDBObject("date" -> date, "total" -> number, "array" -> numberArray))
          }
        })
      })
    })

    hometeamStatistic.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        /*val uri = MongoClientURI("mongodb://localhost:27017/")
        val mongoClient = MongoClient(uri)
        val db = mongoClient("streaming")*/
        val coll_hometeam = db("hometeam")
        partitionOfRecords.foreach(record => {
          val hometeam = record._1
          val number = record._2
          val existHometeamStatistic = coll_hometeam.find(MongoDBObject("hometeam" -> hometeam))
          if (existHometeamStatistic.size > 0){
            val old = existHometeamStatistic.one()
            coll_hometeam.update(old, MongoDBObject("hometeam" -> hometeam, "number" -> number))
          }else{
            coll_hometeam.insert(MongoDBObject("hometeam" -> hometeam, "number" -> number))
          }
        })
      })
    })

    regionStatistic.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        /*val uri = MongoClientURI("mongodb://localhost:27017/")
        val mongoClient = MongoClient(uri)
        val db = mongoClient("streaming")*/
        val coll_region = db("region")
        partitionOfRecords.foreach(record => {
          val region = record._1
          val number = record._2
          val existRegionStatistic = coll_region.find(MongoDBObject("region" -> region))
          if (existRegionStatistic.size > 0){
            val old = existRegionStatistic.one()
            coll_region.update(old, MongoDBObject("region" -> region, "number" -> number))
          }else{
            coll_region.insert(MongoDBObject("region" -> region, "number" -> number))
          }
        })
      })
    })
    genderStatistic.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        /*val uri = MongoClientURI("mongodb://localhost:27017/")
        val mongoClient = MongoClient(uri)
        val db = mongoClient("streaming")*/
        val coll_gender = db("gender")
        partitionOfRecords.foreach(record => {
          val gender = record._1
          val number = record._2
          val existGenderStatistic = coll_gender.find(MongoDBObject("gender" -> gender))
          if (existGenderStatistic.size > 0){
            val old = existGenderStatistic.one()
            coll_gender.update(old, MongoDBObject("gender" -> gender, "number" -> number))
          }else{
            coll_gender.insert(MongoDBObject("gender" -> gender, "word" -> number))
          }
        })
      })
    })
    wordStatistic.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        /*val uri = MongoClientURI("mongodb://localhost:27017/")
        val mongoClient = MongoClient(uri)
        val db = mongoClient("streaming")*/
        val coll_word = db("word")
        partitionOfRecords.foreach(record => {
          val word = record._1
          if (word.length > 1){
            val number = record._2
            val existWordStatistic = coll_word.find(MongoDBObject("word" -> word))
            if (existWordStatistic.size > 0){
              val old = existWordStatistic.one()
              coll_word.update(old, MongoDBObject("word" -> word, "number" -> number))
            }else{
              coll_word.insert(MongoDBObject("word" -> word, "number" -> number))
            }
          }
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
