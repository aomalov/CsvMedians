package demo.examples

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.ExecutionContextExecutor

package object csv_statistics {

  implicit val system: ActorSystem = ActorSystem("QuickStart")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  def getListOfFilesFromResourceFolder(dir: String,ext:String):List[String] = {
    val path = getClass.getResource(dir)
    val d = new File(path.getPath)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.filter(f=>f.getName.endsWith(ext)).map(_.getName)
    }
    else List.empty
  }

  def getGradeByIdx(grades:Seq[(Int,Int)],pos:Int):Int = {
    var runningPos:Int=0
    for (elem <- grades) {
      if(pos>=runningPos && pos<=runningPos+elem._2)
        return elem._1
      else runningPos+=elem._2
    }
    0
  }

}
