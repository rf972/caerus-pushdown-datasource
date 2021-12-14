This is a new Spark V2 datasource, which has support for
pushdown of filter, project and aggregate.
This data source can operate against either HDFS or S3.

The best way to get started with this datasource is to check out the demo detailed here:<BR>
https://github.com/open-infrastructure-labs/caerus-dike/blob/master/README.md
That repo can build and run dockers, which will fully demonstrate this datasource.
That repo will bring up Spark with this datasource and with an S3 server, and an HDFS server.

How to use
=============
Build using: sbt package

The datasource requires the following below jars and packages
- pushdown-datasource_2.12-0.1.0.jar 
- ndp-hdfs-1.0.jar
- AWS Java SDK
- Hadoop version, compatible with Spark.

For working examples of this datasource, use the below caerus-dike repo to lauch Spark.  
One example of this is here:<BR>
https://github.com/open-infrastructure-labs/caerus-dike/blob/master/spark/examples/scala/run_example.sh

Or just follow the README.md to run a demo.<BR>
https://github.com/open-infrastructure-labs/caerus-dike/blob/master/README.md

Use of datasource
=================
Working examples that use this datasource can be found here:<BR>
https://github.com/open-infrastructure-labs/caerus-dike/blob/master/spark/examples/scala/

The data source can be used with the following parameters to the spark session's read command.

```
spark.read
     .format("com.github.datasource")
     .schema(schema)
     .option("format", "tbl")
     .load("ndphdfs://hdfs-server/table.tbl")
```

Using datasource with spark-shell
==================================
To try the datasource out with the spark-shell, first run 

 ```
./start.sh
 ```
 This brings up spark and the hdfs server.
     
Next, start spark shell with the following options.
     
```
docker exec -it sparkmaster spark-shell --master local --jars /dikeHDFS/client/ndp-hdfs/target/ndp-hdfs-1.0.jar,/build/extra_jars/*,/pushdown-datasource/target/scala-2.12/pushdown-datasource_2.12-0.1.0.jar
```
This will start the spark shell inside of our docker for Spark.

Below is an example test to paste into the spark-shell.  This test runs TPC-H Q6 with both NDP (Pushdown enabled), and Spark.

```
sc.setLogLevel("INFO")
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

case class Lineitem(
  l_orderkey: Long,
  l_partkey: Long,
  l_suppkey: Long,
  l_linenumber: Long,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

val schemaLineitem = ScalaReflection.schemaFor[Lineitem].dataType.asInstanceOf[StructType]  
val dfLineitemNdp = {
     spark.read.format("com.github.datasource")
        .option("format", "csv")
        .option("outputFormat", "csv")
        .option("header", "true")
        .schema(schemaLineitem)
        .load("ndphdfs://dikehdfs/tpch-test-csv/lineitem.csv")}
val dfQ06Ndp = {dfLineitemNdp.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05 && $"l_discount" <= 0.07 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))}
var startTime = System.currentTimeMillis()
dfQ06Ndp.show()
val endTime = System.currentTimeMillis()
println("Processed in %.3f sec\n".format((endTime - startTime) / 1000.0))
     
val dfLineitemSpark = {
     spark.read.format("csv")
        .option("header", "true")
        .schema(schemaLineitem)
        .load("hdfs://dikehdfs:9000/tpch-test-csv/lineitem.csv")}
val dfQ06Spark = {dfLineitemSpark.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05 && $"l_discount" <= 0.07 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))}
var startTime = System.currentTimeMillis()
dfQ06Spark.show()
val endTime = System.currentTimeMillis()
println("Processed in %.3f sec\n".format((endTime - startTime) / 1000.0))
```     

The first line enables tracing, which allows us to see pushdown occuring in the traces.

```     
21/11/15 18:38:14 INFO V2ScanRelationPushDown: 
Pushing operators to class com.github.datasource.PushdownBatchTable
Pushed Filters: IsNotNull(l_shipdate), GreaterThanOrEqual(l_shipdate,1994-01-01), LessThan(l_shipdate,1995-01-01), GreaterThanOrEqual(l_discount,0.05), LessThanOrEqual(l_discount,0.07), LessThan(l_quantity,24.0)
Post-Scan Filters: 
         
21/11/15 18:38:14 INFO PushdownScanBuilder: pruneColumns StructType(StructField(l_extendedprice,DoubleType,false), StructField(l_discount,DoubleType,false))
21/11/15 18:38:15 INFO V2ScanRelationPushDown: 
Output: l_extendedprice#5, l_discount#6
21/11/15 18:38:16 INFO HdfsStore: SQL Query (readable): SELECT l_extendedprice,l_discount FROM S3Object WHERE l_shipdate IS NOT NULL AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24.0     
```
     
The final trace above shows that pushdown is occuring for filter and project.
     
Supported Protocols
====================
The datasource supports either S3 or HDFS.

For HDFS, we would provide a Spark Session load of something like this:

```
.load("ndphdfs://hdfs-server/table.tbl")
```

Note that the ndphdfs is provided by ndp-hdfs-1.0.jar, the ndp-hdfs client.

For S3, we would provide a Spark Session load of something like this:

```
.load("s3a://s3-server/table.tbl")
```
Other options
=============

We support options to disable pushdown of either filter, project and aggregate.
These can be disabled individually or in any combination.
By default all three pushdowns are enabled.

To disable all pushdowns:

```
spark.read
     .format("com.github.datasource")
     .schema(schema)
     .option("format", "tbl")
     .option("DisableFilterPush", "")
     .option("DisableProjectPush", "")
     .option("DisableAggregatePush", "")
     .load("ndphdfs://hdfs-server/table.tbl")
```

To disable just aggregate pushdown:

```
spark.read
     .format("com.github.datasource")
     .schema(schema)
     .option("format", "tbl")
     .option("DisableAggregatePush", "")
     .load("ndphdfs://hdfs-server/table.tbl")
```

Supported formats
=================
Currently, only .tbl, a pipe (|) deliminated format is supported.  This is the format used by the TPCH benchmark.<BR>
We are actively working on adding csv and parquet support.

Datasource compatibilty
========================
In the case of S3, this data source is AWS S3 compatible.
It is also compatible with the S3 server included here:<BR>
https://github.com/open-infrastructure-labs/caerus-dikeCS

In the case of HDFS, this data source is compatible with the
HDFS server with proxy configuration located here:<BR>
https://github.com/open-infrastructure-labs/caerus-dikeHDFS

Credits
========
We based some of the initial S3 code on minio's spark select.<BR>
https://github.com/minio/spark-select

