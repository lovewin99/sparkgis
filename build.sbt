name := "sparkgis"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "maven Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-assembly_2.10" % "1.3.0-cdh5.4.0" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.3.0-cdh5.4.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.0-cdh5.4.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.0-cdh5.4.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.6.0-cdh5.4.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0-cdh5.4.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test",
  "redis.clients" % "jedis" % "2.1.0",
  "net.debasishg" % "redisclient_2.10" % "2.12",
  "cn.guoyukun.jdbc" % "oracle-ojdbc6" % "11.2.0.3.0"
)

//resolvers += "Nexus Repository" at "http://101.251.236.34:8081/nexus/content/groups/scalasbt/"

//resolvers += Resolver.url("cloudera", url("https://repository.cloudera.com/artifactory/cloudera-repos/."))
//
//resolvers += Resolver.url("MavenOfficial", url("http://repo1.maven.org/maven2"))
//
//resolvers += Resolver.url("springside", url("http://springside.googlecode.com/svn/repository"))
//
//resolvers += Resolver.url("jboss", url("http://repository.jboss.org/nexus/content/groups/public-jboss"))