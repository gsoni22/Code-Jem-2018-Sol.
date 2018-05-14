// Databricks notebook source
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType, LongType}
import spark.implicits._
import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel._
//This is one test from github to azure databricks
// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/")

// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp-secondary.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
val df = spark.read.parquet("wasbs://cdcvillx170@strgt000000mp-secondary.blob.core.windows.net/viacom90days/mixpanel_event_app/").select("distinct_id","date_stamp_ist").distinct()

// COMMAND ----------

spark.conf.set("spark.driver.maxResultSize", "12g")
spark.conf.set("sspark.memory.fraction", "0.7")

// COMMAND ----------

var newdf= df.distinct.groupBy("date_stamp_ist").agg(collect_list("distinct_id").as("id_list"))

// COMMAND ----------

newdf.persist(MEMORY_AND_DISK_SER)
newdf.count()

// COMMAND ----------

val start="2017-12-01"
val end="2018-02-28"
val startDate=LocalDate.parse(start)
val endDate=LocalDate.parse(end)
val count= ChronoUnit.DAYS.between(startDate, endDate)+1
var dateArray=(0 until count.toInt).map(startDate.plusDays(_))

// COMMAND ----------

val schema= StructType(
    StructField("startDate", StringType, true) ::
    StructField("endDate", StringType, true) ::
    StructField("count", LongType, true) :: Nil)
var res=spark.createDataFrame(sc.emptyRDD[Row], schema)

// COMMAND ----------

def computeFun(d1: String, d2: String, df: org.apache.spark.sql.DataFrame ) : Long ={
  val c=df.filter((df("date_stamp_ist") >= d1) and (df("date_stamp_ist") <= d2))
          .select(explode($"id_list")).distinct.count()
  c
}

// COMMAND ----------

for (outer<-0 until dateArray.length){
  for (inner <- outer until dateArray.length){
    var c1=computeFun(dateArray(outer).toString, dateArray(inner).toString, newdf)
    var r: Seq[(String, String, Long)]=Seq(((dateArray(outer).toString, dateArray(inner).toString, c1)))
    var myrow=Row.fromSeq(r)
    res=res.union(r.toDF).persist(MEMORY_AND_DISK_SER)
  }
}

// COMMAND ----------

res.show

// COMMAND ----------

res.write.mode(SaveMode.Append).saveAsTable("distinctid_count_90_days_tbl")

// COMMAND ----------

//TWO

// COMMAND ----------

val start1="2017-12-01"
val end1="2017-12-20"
val startDate1=LocalDate.parse(start1)
val endDate1=LocalDate.parse(end1)
val count1= ChronoUnit.DAYS.between(startDate1, endDate1)+1
var r1=(0 until count1.toInt).map(startDate1.plusDays(_))

// COMMAND ----------

val schema= StructType(
    StructField("startDate", StringType, true) ::
    StructField("endDate", StringType, true) ::
    StructField("count", LongType, true) :: Nil)
var res1=spark.createDataFrame(sc.emptyRDD[Row], schema)

// COMMAND ----------

var inner=0
var w=0
var count2=0
for (w<-0 until dateArray.length){
  for (inner <- 0 until r1.length){
      //println(r1(inner), dateArray(w))
      var c2=computeFun(r1(inner).toString, dateArray(w).toString, newdf)
      var r: Seq[(String, String, Long)]=Seq(((r1(inner).toString, dateArray(w).toString, c2)))
      var myrow=Row.fromSeq(r)
      res1=res1.union(r.toDF)
 }
}

// COMMAND ----------



// COMMAND ----------

res1.cache()
res1.show()

// COMMAND ----------

res1.write.mode(SaveMode.Append).saveAsTable("distinctid_count_90_days_tbl")

// COMMAND ----------

val test=spark.read.table("distinctid_count_90_days_tbl").select("startDate").distinct.count()

// COMMAND ----------

import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType, LongType}
import spark.implicits._
import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel._

// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")

// COMMAND ----------

val dataDF = spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/").select("distinct_id","date_stamp_ist","event").distinct()

// COMMAND ----------

when($"os".isNull,"Others").otherwise($"os").alias("operating_system"))).drop("os")

// COMMAND ----------

val df=dataDF.select($"*", when($"event" isin("App Launched","App Opened", "App Access"),1).otherwise(0).alias("visit_num"), 
                                    when($"event" isin("mediaReady"),1).otherwise(0).alias("view_num"))
val df1=df.groupBy("distinct_id","date_stamp_ist").agg(sum($"visit_num").alias("visit_count"),
                                                       sum($"view_num").alias("view_count"))

// COMMAND ----------

val visit_view_df=df1.withColumn("visit_flag", when($"visit_count">0,1).otherwise(0)).withColumn("viewer_flag", when($"view_count">0,1).otherwise(0))
                .withColumn("date_snap", to_date($"date_stamp_ist")).drop("date_stamp_ist").drop("visit_count").drop("view_count")

// COMMAND ----------

visit_view_df.cache()
visit_view_df.count()

// COMMAND ----------

visit_view_df.createOrReplaceTempView("visit_view")

// COMMAND ----------

display(visit_view_df)

// COMMAND ----------

visit_view_df.groupBy("distinct_id").agg(collect_list($"date_snap"))

// COMMAND ----------

display(res19)

// COMMAND ----------

display(df1)

// COMMAND ----------

df1.count

// COMMAND ----------

val table=spark.read.table("num_visitors_viewers").withColumn("visitor_count",when($"visitors_count".isNull,0).otherwise($"visitors_count")).withColumn("viewer_count",when($"viewers_count".isNull,0).otherwise($"viewers_count")).drop("visitors_count").drop("viewers_count")

val dataDF=table.withColumn("visit_flag", when($"visitor_count">0,1).otherwise(0)).withColumn("viewer_flag", when($"viewer_count">0,1).otherwise(0))
                .withColumn("date_snap", to_date($"date_stamp_ist")).drop("date_stamp_ist").drop("visitor_count").drop("viewer_count").drop("distinct_id")

dataDF.cache
dataDF.count
dataDF.createOrReplaceTempView("visit_view")

// COMMAND ----------

val res=spark.sql("select * from visit_view where date_snap >=cast('2017-12-01' as date) and date_snap <= cast('2018-02-28' as date)").groupBy($"date_snap").agg(sum($"visit_flag"),sum($"viewer_flag"))

// COMMAND ----------

display(res)

// COMMAND ----------

visit_view_df.unpersist()

// COMMAND ----------

val byID= Window.partitionBy('distinct_id)
 val partitionedID=visit_view_df.withColumn("counts", count("date_snap") over byID)
 partitionedID.cache
 partitionedID.count
 partitionedID.createOrReplaceTempView("data_view")

// COMMAND ----------

partitionedID.unpersist

// COMMAND ----------

display(df1)

// COMMAND ----------

import org.apache.spark.sql.types.DateType
import java.text.SimpleDateFormat;
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import java.util.Calendar
import java.time.LocalDate 
import java.time.format.DateTimeFormatter

var ls4= new scala.collection.mutable.ArrayBuffer[List[String]]()
 val byID= Window.partitionBy('distinct_id)
 val partitionedID=visit_view_df.withColumn("counts", count("date_snap") over byID)
 partitionedID.cache
 partitionedID.count
 partitionedID.createOrReplaceTempView("visit_view")
val c1 = Calendar.getInstance();
val c2 = Calendar.getInstance();
def computeDistinctIDFunc(from:String,to:String)={ 
 
var startDate = from
val formatter =new SimpleDateFormat("yyyy-MM-dd");
val oldDate = formatter.parse(startDate)
val toDate = to
val newDate = formatter.parse(toDate)
val numberOfDays=((newDate.getTime-oldDate.getTime)/(1000*60*60*24)).toInt


for (i<-1 to numberOfDays){
    
  val initialDate=startDate
  val fromDate = formatter.parse(initialDate)
  var counter=0
  for (j<-i+1 to numberOfDays){
//     val endDay=fromDate.plusDays(1).getDayOfMonth.toInt
    //println(i,j)
    c2.setTime(fromDate)
    c2.add(Calendar.DATE,counter+1)
  
    val endDate=formatter.format(c2.getTime())
//     val endDateQuery=formatter.format(endDate)
   println(initialDate,endDate)
  //  val distinctCount=spark.sql(s"""Select `sum(visit_flag)`, `sum(viewer_flag)` from visit_view where date_snap >="$initialDate" and date_snap<="$endDate"   """).groupBy('distinct_id).agg(count('distinct_id)).filter($"count(distinct_id)">=1).count()
    val sumDF=spark.sql(s"""select distinct_id, visit_flag, viewer_flag from visit_view where date_snap >="$initialDate" and date_snap<="$endDate" """).groupBy("distinct_id").agg(sum($"visit_flag"),sum($"viewer_flag"))
val visitor_count=sumDF.filter($"sum(visit_flag)">=1).count()
val viewer_count=sumDF.filter($"sum(viewer_flag)">=1).count()
    
    ls4+=List(initialDate,endDate,visitor_count.toString(),viewer_count.toString())
    counter=counter+1 
  }
c1.setTime(fromDate)
c1.add(Calendar.DATE,1)
startDate=formatter.format(c1.getTime())
}
}

// COMMAND ----------

val sumDF=spark.sql("select distinct_id, visit_flag, viewer_flag from visit_view where date_snap >="$initialDate" and date_snap<="$endDate" """).groupBy("distinct_id").agg(sum($"visit_flag"),sum($"viewer_flag"))
val visitor_count=sumDF.filter($"sum(visit_flag)">=1).count()
val viewer_count=sumDF.filter($"sum(viewer_flag)">=1).count()

// COMMAND ----------

spark.sql(s"""Select * from data_view where date_snap >=cast('2017-12-01' as date) and date_snap<=cast('2017-12-02' as date)""").groupBy("distinct_id").agg(sum($"visit_flag"),sum($"viewer_flag")).filter($"sum(visit_flag)">=1 or $"sum(viewer_flag)" >=1)

// COMMAND ----------

res48.select(col("sum(visit_flag)")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)

// COMMAND ----------

res48.agg(sum("sum(visit_flag)").cast("long")).first.getLong(0)

// COMMAND ----------

res48.show

// COMMAND ----------

res40.show

// COMMAND ----------

dbutils.fs.rm("/tmp",true)

// COMMAND ----------

computeDistinctIDFunc("2017-12-01","2018-03-01")

// COMMAND ----------

ls4

// COMMAND ----------

visit_view_df.show

// COMMAND ----------

val df=sc.parallelize(ls4).toDF("value_arr")


val result= df.select($"value_arr".getItem(0).alias("startDate"),$"value_arr".getItem(1).alias("endDate"),$"value_arr".getItem(2).alias("visitor_count"),$"value_arr".getItem(3).alias("viewer_count"))

// COMMAND ----------

result.show

// COMMAND ----------

result.write.saveAsTable("distinct_visitor_viewer_count_range")

// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")

// COMMAND ----------

result.write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacomOutput/visitor_viewer_count_range")

// COMMAND ----------

display(result)

// COMMAND ----------

val vvdf=spark.read.table("distinct_visitor_viewer_datewise_count")

// COMMAND ----------

display(vvdf)

// COMMAND ----------

// Set the configration of spark to get the data with get the accesss key
spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
//spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
import spark.implicits._
import org.apache.spark.sql.functions._
// val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom18/*")
//val customColSelection=loadData.select('distinct_id,'date_stamp_ist,'date_stamp_ist.alias("date_snap"))
//val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/")
val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/")
// val customColSelection1= loadData.select('distinct_id,'date_stamp_ist,'event').repartition(500)
// customColSelection1.cache
// customColSelection1.count

// COMMAND ----------

import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType, DateType}
val test = loadData.select($"date_stamp_ist",$"distinct_id",$"first_app_launch_date".cast(DateType)).filter($"date_stamp_ist"===$"first_app_launch_date")

// COMMAND ----------

val new_dws=spark.read.table("final_final_duration_watched_sec_correct")
//when ($"event" ==="mediaReady"agg(countDistinct($"distinct_id"))),when($"event" ==="App Install"agg(countDistinct($"distinct_id")))

// COMMAND ----------

display(new_dws)

// COMMAND ----------

dws.count

// COMMAND ----------

new_dws.count

// COMMAND ----------

val dws=spark.read.table("DURATION_WATCHED_SECS")
val aaa= loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"duration",$"app_version",$"duration_seconds",$"media_source",$"os",$"user_type", $"first_app_launch_date".cast(DateType))

//val nnn= aaa.join(new_dws,usingColumns=Seq("distinct_id","date_stamp_ist"),joinType="outer")

val mmmr= nnn.select($"distinct_id",$"date_stamp_ist",$"event",$"duration",$"app_version",$"duration_seconds",$"media_source",$"os",$"user_type", $"first_app_launch_date".cast(DateType),$"sum(duration)",when($"os".isNull,"others").otherwise($"os").alias("operatingSystem"), when($"sum(duration)"/60 >=30,"Y").otherwise("N"). alias("daily_power_user"),when($"sum(duration)"/60 >= 30 and $"sum(duration)"/60 < 40,">=30Mins").when($"sum(duration)"/60 >=40 and $"sum(duration)"/60 < 50,">=40Mins").when($"sum(duration)"/60 >= 50 and $"sum(duration)"/60 < 80,">=50 Mins").when($"sum(duration)"/60 >= 80,"80 Min").otherwise("N").alias("duration_power_user_flag"), when($"media_source"==="Organic","Organic").otherwise("Inorganic").alias("Lead") ,when($"user_type" =!= "null" && $"user_type"=!= "Guest","REGISTERED").otherwise("NOT Register").alias("Status"),when($"date_stamp_ist" === $"first_app_launch_date".cast(DateType),"NEW USER").otherwise("Old User").alias("New or Not"),when($"event" isin ("App Launched","App Access","App Opened"),1).alias("visitor"),when($"event"==="mediaReady",0).alias("viewer"),when($"event" === "App Install"),"App Install").drop("event").drop("duration").drop("duration_flag") .drop("app_version"). drop("user_type").drop("first_app_launch_date").drop("distinct_id").drop("duration_seconds").drop("media_source").drop("os"). groupBy("date_stamp_ist","operatingSystem","daily_power_user","duration_power_user_flag","Lead","Status","New or Not").count

// COMMAND ----------

mmmr.cache

// COMMAND ----------

display(mmmr)

// COMMAND ----------

val visitorCount = loadData.filter($"event"==="App Launched" || $"event"==="App Access" || $"event"==="App Opened" )
                  .select($"date_stamp_ist".cast(DateType)).groupBy("date_stamp_ist").count
val viewerCount= loadData.filter($"event"==="mediaReady").select($"date_stamp_ist").groupBy("date_stamp_ist").count
val app_ins= loadData.filter($"event" === "App Install").select($"date_stamp_ist").groupBy("date_stamp_ist").count

// COMMAND ----------

//Logic of Duration_watched_seconds
val testsecp=loadData.select($"distinct_id",$"date_stamp_ist",$"app_version",$"duration",$"duration_seconds",$"event",when($"app_version" isin ("47","1.2.16","1.2.21") and ($"duration">0 and $"duration"<36000) and $"event"==="Video Watched",1).when(!($"app_version"isin("47","1.2.16","1.2.21")) and ($"duration_seconds">0 and $"duration_seconds"<36000) and $"event"==="Video Watched",0).otherwise(-1).alias( "duration_flag"),when($"event"==="mediaReady",1).otherwise(0).alias("views")). groupBy("date_stamp_ist","duration_flag","views").
agg(when($"views"===1,countDistinct($"distinct_id")),when($"duration_flag"===1,sum($"duration")).when($"duration_flag"===0,sum($"duration_seconds")).when($"duration_flag"===(-1),0).alias("TSV"))
//.drop("app_version").drop("duration").drop("duration_seconds")

//In the block of aggrigation block
//

// COMMAND ----------

display(testsecp)

// COMMAND ----------

val fff=testsecp.select($"*").groupBy("date_stamp_ist").agg(sum($"TSV")/sum($"CASE WHEN (views = 1) THEN count(DISTINCT distinct_id) END")).withColumnRenamed("(sum(TSV) / sum(CASE WHEN (views = 1) THEN count(DISTINCT distinct_id) END))","TSV")

// COMMAND ----------

display(fff)

// COMMAND ----------

fff.coalesce(1).write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/day_before_after_lunch")

// COMMAND ----------

fff.agg(sum("TSV")).show

// COMMAND ----------

testsecp.cache
testsecp.count

// COMMAND ----------

fff.cache
fff.count

// COMMAND ----------

val mmmr_w2= loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"media_source",$"os",$"user_type", $"first_app_launch_date".cast(DateType), 
                            when($"os".isNull,"others").otherwise($"os").alias("operatingSystem"),
                            when($"media_source"==="Organic","Organic").otherwise("Inorganic").alias("Lead"),
                            when($"user_type".isNotNull  && $"user_type"=!= "Guest","REGISTERED").otherwise("NOT Register").alias("Status"),
                            when($"date_stamp_ist" === $"first_app_launch_date".cast(DateType),"NEW USER").otherwise("Old User").alias("New or Not"),
                            when($"event" isin ("App Launched","App Access","App Opened"),1).otherwise(0).alias("visits"),
                            when($"event"==="mediaReady",1).otherwise(0).alias("views"),
                            when($"event" === "App Install",1).otherwise(0) .alias("App Installs"))
.drop("event").drop("first_app_launch_date").drop("media_source").drop("os").drop("user_type"). groupBy("date_stamp_ist","operatingSystem","Lead","Status","New or Not","visits","views","App Installs").
agg(when($"visits"===1,countDistinct("distinct_id")),when($"views"===1,countDistinct("distinct_id")),when($"App Installs"===1,countDistinct ("distinct_id"))).
drop("visits").drop("views").drop("App Installs").drop("distinct_id")
mmmr_w2.cache

// COMMAND ----------

display(mmmr_w2)

// COMMAND ----------

mmmr_w2.withColumnRenamed("CASE WHEN (visits = 1) THEN count(DISTINCT distinct_id) END","visits").withColumnRenamed("CASE WHEN (views = 1) THEN count(DISTINCT distinct_id) END","views").withColumnRenamed("CASE WHEN (App Installs = 1) THEN count(DISTINCT distinct_id) END","App Installs")

// COMMAND ----------

mmmr_w2.coalesce(1).write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/it's_final_0000")

// COMMAND ----------

//val dws=spark.read.table("DURATION_WATCHED_SECS")
//val aaa= loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"duration",$"app_version",$"duration_seconds",$"media_source",$"os",$"user_type", $"first_app_launch_date".cast(DateType))

//val nnn= aaa.join(new_dws,usingColumns=Seq("distinct_id","date_stamp_ist"),joinType="outer")

val mmmr_w= loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"media_source",$"os",$"user_type", $"first_app_launch_date".cast(DateType), 
                            when($"os".isNull,"others").otherwise($"os").alias("operatingSystem"),
                            when($"media_source"==="Organic","Organic").otherwise("Inorganic").alias("Lead"),
                            when($"user_type".isNotNull  && $"user_type"=!= "Guest","REGISTERED").otherwise("NOT Register").alias("Status"),
                            when($"date_stamp_ist" === $"first_app_launch_date".cast(DateType),"NEW USER").otherwise("Old User").alias("New or Not"),
                            when($"event" isin ("App Launched","App Access","App Opened"),1).otherwise(0).alias("visits"),
                            when($"event"==="mediaReady",1).otherwise(0).alias("views"),
                            when($"event" === "App Install",1).otherwise(0) .alias("App Installs"))
.drop("event").drop("first_app_launch_date").drop("distinct_id").drop("media_source").drop("os").drop("user_type"). groupBy("date_stamp_ist","operatingSystem","Lead","Status","New or Not","visits","views","App Installs"). agg(sum($"visits"),sum($"views"),sum($"App Installs")).
drop("visits").drop("views").drop("App Installs")

// COMMAND ----------

display(mmmr_w)

// COMMAND ----------

mmmr_w.filter($"date_stamp_ist"==="2017-12-01").count

// COMMAND ----------

mmmr_w.cache
mmmr_w.count

// COMMAND ----------

loadData.filter($"user_type".isNotNull).count

// COMMAND ----------

loadData.filter($"date_stamp_ist"==="2017-12-22" and $"os"==="Android" and $"media_source"==="Organic" and ($"user_type" =!= "null" && $"user_type"=!= "Guest") and  !($"date_stamp_ist" === $"first_app_launch_date".cast(DateType)) and ($"event" isin ("App Launched","App Access","App Opened")) and ($"event" =!= "App Install")).count

// COMMAND ----------

loadData.fileter($"date_stamp_ist" ==="2017-12-21" and $"os" === "iOS"  and )

// COMMAND ----------

loadData.filtetr 

// COMMAND ----------

mmmr_w.coalesce(1).write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/day_before_after_lunch")

// COMMAND ----------

mmmr_w.withColumnRenamed("New or Not","new_or_not").withColumnRenamed("sum(visits)","visitors").withColumnRenamed("sum(views)","Viewers").withColumnRenamed("sum(App Installs)","Install")

// COMMAND ----------

res200.write.saveAsTable("all_the_best")

// COMMAND ----------

loadData.filter($"date_stamp_ist"==="2018-01-07" and $"media_source"==="Organic" and $"os"==="Android" and $"event"==="mediaReady" and ($"user_type" =!= "null" && $"user_type"=!= "Guest") and $"date_stamp_ist" === $"first_app_launch_date".cast(DateType)).count

// COMMAND ----------

loadData.filter($"date_stamp_ist"==="2018-01-07" and $"media_source"==="Organic" and $"os"==="Android" and $"event"==="App Install" and ($"user_type" =!= "null" && $"user_type"=!= "Guest") and $"date_stamp_ist" === $"first_app_launch_date".cast(DateType)).count

// COMMAND ----------

mmmr_w.agg(sum($"count")).show

// COMMAND ----------

val aajdekho = mmmr_new.select($"*", when($"event" ==="mediaReady",countDistinct($"distinct_id")),when($"event" ==="App Install"countDistinct($"distinct_id")),when($"event" isin ("App Launched", "App Opened", "App Access"),countDistinct($"distinct_id")))

// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
val rawData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/")

// COMMAND ----------

verification("2018-01-12","2018-01-12",rawData)

// COMMAND ----------

import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._

def verification(from: String, to: String, raw_data: org.apache.spark.sql.DataFrame): Boolean ={
  
//   val visitorCount = raw_data.filter($"event"==="App Launched" || $"event"==="App Access" || $"event"==="App Opened" )
//                       .select($"distinct_id",$"date_stamp_ist".cast(DateType))
//                       .where(s"""date_stamp_ist >=cast("$from" as date) and date_stamp_ist <= cast("$to" as date)""")
//                       .drop("date_stamp_ist")
//                       .distinct()
//                       .cache()
//                       .count()
//                       .toLong
//    val visitor_count=spark.sql(s"""select visitor_count from distinct_visitor_viewer_count_range where startDate =="$from" and endDate =="$to" """).first.getString(0).toLong
//   if(visitorCount==visitor_count)
//       return true
//   else return false
  
  
//   val viewerCount=raw_data.filter($"event"==="mediaReady").select($"distinct_id",$"date_stamp_ist".cast(DateType))
//                     .where(s"""date_stamp_ist >=cast("$from" as date) and date_stamp_ist <= cast("$to" as date)""")
//                     .drop("date_stamp_ist")
//                     .distinct()
//                     .cache()
//                     .count()
//                     .toLong 
//   val viewer_count=spark.sql(s"""select viewer_count from distinct_visitor_viewer_count_range where startDate =="$from" and endDate =="$to" """).first.getString(0).toLong
//   if(viewerCount==viewer_count)
//       return true
//   else return false
  
  val durationSecs=spark.read.table("duration_watched_secs").select($"distinct_id",$"date_stamp_ist".cast(DateType),$"sum_duration")
  val powerUser= raw_data.select($"distinct_id",$"date_stamp_ist".cast(DateType)).filter($"event"==="mediaReady")
                         .join(durationSecs, usingColumns=Seq("distinct_id","date_stamp_ist"), joinType="inner")
                            .where(s"""date_stamp_ist >=cast("$from" as date) and date_stamp_ist <= cast("$to" as date)""")
                            .filter($"sum_duration">=1800)
                            .select($"distinct_id")
                            .distinct()
                            .cache()
                            .count()
  val power_user=spark.sql(s"""select power_user_count from power_user_count_range where startDate =="$from" and endDate =="$to" """).first.getString(0).toLong
  if (power_user == powerUser)
    return true
  else return false
}

// COMMAND ----------

val visitor_count=spark.sql(s"""select visitor_count from distinct_visitor_viewer_count_range where startDate =="2017-12-01" and endDate =="2017-12-02" """).first.getString(0).toLong

// COMMAND ----------

val testDF1=loadData.select($"date_stamp_ist",$"os",$"media_source",when($"date_stamp_ist"==="2018-01-05").when($"os"==="Android"). when($"media_source"==="Organic")

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
val byID= Window.partitionBy("date_stamp_ist")
 val partitionedID=mmmr.withColumn("counts", sum("count") over byID)

// COMMAND ----------

val leadsDF=mmmr.select($"*", $"date_stamp_ist".cast(DateType).alias("date_snap")).drop($"date_stamp_ist")

// COMMAND ----------

leadsDF.createOrReplaceTempView("leads_view")
val sumDF=spark.sql(s"""select date_snap,operatingSystem, daily_power_user, duration_power_user_flag, Lead, Status, `New or Not`, count  from leads_view  where date_snap >=cast('2017-12-01' as date) and date_snap<=cast('2017-12-02' as date) """).groupBy($"operatingSystem",$"daily_power_user",$"Lead",$"Status",$"New or Not").agg(sum($"count")).withColumn("startDate",lit("date")).withColumn("endDate",lit("endDate"))

// COMMAND ----------

sumDF.count

// COMMAND ----------

import org.apache.spark.sql.types._
import java.text.SimpleDateFormat;
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import java.util.Calendar
import java.time.LocalDate 
import java.time.format.DateTimeFormatter

val schema= StructType(
    StructField("startDate", StringType, true) ::
    StructField("endDate", StringType, true) ::
    StructField("operatingSystem", StringType, true) ::
  StructField("daily_power_user", StringType, true) ::
  StructField("Lead", StringType, true) ::
  StructField("Status", StringType, true) ::
  StructField("New_or_Not", StringType, true) ::
  StructField("sum(count)", LongType, true) ::Nil)
var res=spark.createDataFrame(sc.emptyRDD[Row], schema)

var lead_list= new scala.collection.mutable.ArrayBuffer[List[String]]()
 val byID= Window.partitionBy("date_snap")
 val partitionedID=leadsDF.withColumn("counts", sum("count") over byID)
 partitionedID.cache
 partitionedID.count
 partitionedID.createOrReplaceTempView("leads_view")
val c1 = Calendar.getInstance();
val c2 = Calendar.getInstance();
def computeDistinctIDFunc(from:String,to:String)={ 
 
var startDate = from
val formatter =new SimpleDateFormat("yyyy-MM-dd");
val oldDate = formatter.parse(startDate)
val toDate = to
val newDate = formatter.parse(toDate)
val numberOfDays=((newDate.getTime-oldDate.getTime)/(1000*60*60*24)).toInt


for (i<-1 to numberOfDays){
    
  val initialDate=startDate
  val fromDate = formatter.parse(initialDate)
  var counter=0
  for (j<-i+1 to numberOfDays){
//     val endDay=fromDate.plusDays(1).getDayOfMonth.toInt
    //println(i,j)
    c2.setTime(fromDate)
    c2.add(Calendar.DATE,counter+1)
  
    val endDate=formatter.format(c2.getTime())
//     val endDateQuery=formatter.format(endDate)
   println(initialDate,endDate)
  //  val distinctCount=spark.sql(s"""Select `sum(visit_flag)`, `sum(viewer_flag)` from visit_view where date_snap >="$initialDate" and date_snap<="$endDate"   """).groupBy('distinct_id).agg(count('distinct_id)).filter($"count(distinct_id)">=1).count()
    val sumDF=spark.sql(s"""select date_snap,operatingSystem, daily_power_user, duration_power_user_flag, Lead, Status, `New or Not`, count  from leads_view where date_snap >="$initialDate" and date_snap<="$endDate" """).groupBy($"operatingSystem",$"daily_power_user",$"Lead",$"Status",$"New or Not").agg(sum($"count")).withColumn("startDate",lit(initialDate)).withColumn("endDate",lit(endDate))

    res=res.union(sumDF)
    //ls4+=List(initialDate,endDate,visitor_count.toString(),viewer_count.toString())
    counter=counter+1 
  }
c1.setTime(fromDate)
c1.add(Calendar.DATE,1)
startDate=formatter.format(c1.getTime())
}
}

// COMMAND ----------

computeDistinctIDFunc("2017-12-01","2018-02-28")

// COMMAND ----------

val chkvis=loadData.filter($"event"==="mediaReady").select("date_stamp_ist","distinct_id").groupBy("date_stamp_ist").agg(countDistinct("distinct_id"))

// COMMAND ----------

chkvis.show

// COMMAND ----------

chkvis.cache
chkvis.count

// COMMAND ----------

val chkdws=loadData.filter($"event"==="Video Watched").select($"date_stamp_ist",$"app_version",$"duration",$"duration_seconds",
  when($"app_version" isin ("47","1.2.16","1.2.21") and ($"duration">0 and $"duration"<36000) ,1).
  when(!($"app_version"isin("47","1.2.16","1.2.21")) and ($"duration_seconds">0 and $"duration_seconds"<36000) ,0).
  otherwise(-1).
  alias( "duration_flag")).
  groupBy("date_stamp_ist","duration_flag").agg(when($"duration_flag"===1,sum("duration")).when($"duration_flag"===0,sum("duration_seconds")).otherwise(0).alias("check_dws")).drop("duration_flag")

// COMMAND ----------

chkdws.show

// COMMAND ----------

chkdws.cache
chkdws.count

// COMMAND ----------

val chkdws1=chkdws.groupBy("date_stamp_ist").agg(sum($"check_dws"))

// COMMAND ----------

chkdws1.show

// COMMAND ----------

val final_raw_tsv=chkvis.join(chkdws1,"date_stamp_ist")

// COMMAND ----------

display(final_raw_tsv)

// COMMAND ----------

display(mmmr_w.filter( $"date_stamp_ist".cast(DateType)<="2017-12-02"))

// COMMAND ----------

val ff_tsv=final_raw_tsv.withColumn("TSV",$"sum(check_dws)"/$"count(DISTINCT distinct_id)").drop("count(DISTINCT distinct_id)").drop("sum(check_dws)")

// COMMAND ----------






ff_tsv.agg(sum("TSV")).show

// COMMAND ----------

import scala.collection.JavaConversions._

// COMMAND ----------

// Final Testing of the Records of All the values with their sum of Distinct of Distinct_id

// COMMAND ----------

val fi=loadData.filter($"event"isin("App Launched","App Opened", "App Access")).select("distinct_id").groupBy("distinct_id").count

// COMMAND ----------

fi.count

// COMMAND ----------

