name := "sparkgis"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test" ,
  "redis.clients" % "jedis" % "2.1.0",
  "net.debasishg" % "redisclient_2.10" % "2.12",
  "org.apache.spark" % "spark-assembly_2.10" % "1.0.1" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.1.0"
)

resolvers += Resolver.url("cloudera", url("https://repository.cloudera.com/artifactory/cloudera-repos/."))

resolvers += Resolver.url("MavenOfficial", url("http://repo1.maven.org/maven2"))

resolvers += Resolver.url("springside", url("http://springside.googlecode.com/svn/repository"))

resolvers += Resolver.url("jboss", url("http://repository.jboss.org/nexus/content/groups/public-jboss"))