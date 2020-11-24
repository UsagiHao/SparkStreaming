package org.example

import java.io.FileInputStream

import com.alibaba.fastjson.JSON
import com.mongodb.casbah.Imports.MongoClientURI
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.commons.io.IOUtils

object JsonTest {
  def main(args: Array[String]): Unit = {
    /*val input = new FileInputStream("/home/hadoop/Downloads/comments1.json")
    val inputString = IOUtils.toString(input)
    val temp = JSON.parseArray(inputString)
    println(temp)*/
    val uri = MongoClientURI("mongodb://localhost:27017/")
    val mongoClient = MongoClient(uri)
    val db = mongoClient("streaming")
    val coll = db("hometeam")
    val a = MongoDBObject("hometeam"->"FCB", "number"->1)
    coll.insert(a)
    val allDocs = coll.find()
    allDocs.foreach(println)


  }

}
