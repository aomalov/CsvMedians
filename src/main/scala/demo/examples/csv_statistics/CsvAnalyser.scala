package demo.examples.csv_statistics

import java.io.{File, PrintWriter}

import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future
import scala.io.{Source => IOSource}

object CsvAnalyser extends App {

  //the MAPPER job - does everything locally - end sends result to fold/reduce
  def analyseRegion(fileName: String, folderName: String) = {
    //TODO - add CSV library to verify file structure

    //file name contains region number as SECOND part
    val region = fileName.split("-")(1)

    val gradesHistogram = IOSource.fromInputStream(getClass.getResourceAsStream(s"$folderName/$fileName")).getLines()
      .map(_.split(",")(1).toInt) //Assumption: Grade is an INTEGER
      //get histogram on every distinct grage
      .foldLeft(Map.empty[Int, Int] withDefaultValue 0) {
      case (histogram, elem) => histogram ++ Map(elem -> (histogram(elem) + 1))
    }
    //infer average, bucketing on 10s
    val rangeHistogramm = gradesHistogram.foldLeft(Map.empty[String, Int] withDefaultValue 0) {
      case (histogram, (grade, count)) =>
        val range: Int = (grade - 1) / 10
        val key = s"${range * 10}-${range * 10 + 10}"
        histogram ++ Map(key -> (histogram(key) + count))
    }

    //Writing local mapper results
    val pw = new PrintWriter(new File(s"results/${region}_ranges.csv"))
    rangeHistogramm.toSeq.sortBy(_._1)
      .foreach {
        case (range, count) => pw.println(s"$range,$count")
      }
    pw.close()

    //TODO - can return more natural Future from Source(filestream).runFold
    Future.successful(region, gradesHistogram)
  }


  val par = ConfigFactory.load().getInt("input.csv-files.parallelism")

  Source(getListOfFilesFromResourceFolder("/csv", "csv"))
    //MAPPER task - can be sent to remote host as actor, or to TCP socket (both supported by AKKA)
    //Here and now - runs locally - but shows multithreaded approach - with unurdered results being collected by reducer
    .mapAsyncUnordered(par)(analyseRegion(_, "/csv"))
    //REDUCER task - receiving (sum and count by Region) as a Map
    .runFold((Map.empty[String, (Int, Int)], Map.empty[Int, Int] withDefaultValue 0)) {
    case ((statistics, medians), (region, grades)) =>
      val (sum, cnt) = grades.foldLeft((0, 0)) {
        case ((s, c), (grade, count)) => (s + grade * count, c + count)
      }
      val mediansUpd = grades.foldLeft(medians) {
        case (med, (grd, count)) => med ++ Map(grd -> (med(grd) + count))
      }
      (statistics ++ Map(region -> (sum, cnt)), mediansUpd)
  }
    //RESULTS output
    .map {
      case (stats, medians) =>
        println("Statistics for each region")
        stats.foreach {
          case (region, (sum, count)) => println(s"Region [$region], AVG grade ${sum / count}")
        }

        val (globalSum, globalCount) = stats.foldLeft((0, 0)) {
          case ((s, c), (_, (sumRegion, cntRegion))) => (s + sumRegion, c + cntRegion)
        }
        println(s"Global average grade ${globalSum / globalCount}")

        val sortedGrades= medians.toSeq.sortBy(_._1)
        val globalMedian=globalCount % 2 match {
          case 1 =>
            getGradeByIdx(sortedGrades,globalCount/2)
          case 0 =>
            (getGradeByIdx(sortedGrades,globalCount/2-1)+getGradeByIdx(sortedGrades,globalCount/2))/2
        }
        println(s"Global MEDIAN $globalMedian")

        system.terminate()
    }


}
