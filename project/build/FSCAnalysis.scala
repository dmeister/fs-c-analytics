import sbt._
import java.io.File

class FSCAnalysisProject(info: ProjectInfo) extends DefaultProject(info)
{  
  def getFSCJars() : PathFinder = {
    var fscPath = System.getenv().get("FSC_ROOT")
    if (fscPath == null) {
      fscPath = "../fs-c"
    }
    val jars = descendents(Path.fromFile(new File(fscPath)), "*.jar")
    return jars
  }
  override def dependencyPath = "lib"
  override def unmanagedClasspath = super.unmanagedClasspath +++ getFSCJars()
  override def mainSourceRoots = super.mainSourceRoots +++ ("src" / "main" / "gen")
  override def mainClass = Some("de.pc2.dedup.analysis.Main")
    
  val argot = "org.clapper" %% "argot" % "0.3.1"
  val scalatest = "org.scalatest" % "scalatest_2.9.0" % "1.6.1"
  val guava = "com.google.guava" % "guava" % "13.0.1"
  val gson = "com.google.code.gson" % "gson" % "1.7.1"  
  val log4j =  "log4j" % "log4j" % "1.2.16"
  val commonscodec = "commons-codec" % "commons-codec" % "1.6"
  val commonsCollections = "commons-collections" % "commons-collections" % "3.1"
}
