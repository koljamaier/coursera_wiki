name := course.value + "-" + assignment.value

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation")

courseId := "e8VseYIYEeWxQQoymFg8zQ"

resolvers += Resolver.sonatypeRepo("releases")

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % Test

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "compile",
  "org.apache.spark" %% "spark-streaming" % "1.2.0" % "compile",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.2.0"
)


// include the common dir
commonSourcePackages += "common"
