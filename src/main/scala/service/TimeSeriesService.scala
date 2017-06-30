package service

import java.io.File
import java.math.RoundingMode
import java.text.NumberFormat
import java.util.Locale

import models.{Measurement, MeasurementAnalysis}
import output.Output

import scala.collection.immutable.Queue
import scala.io.Source
import scala.util.{Failure, Success, Try}

class TimeSeriesService(T:Int = 60) extends Output {

  val formatter = NumberFormat.getInstance(Locale.US)
  formatter.setMaximumFractionDigits(5)
  formatter.setRoundingMode(RoundingMode.HALF_UP)

  def analysisInTimeSlide(filePath: String) = {
    val fileIterator = Source.fromFile(filePath).getLines
    analyzeMeasurements(fileIterator)
  }

  def analyzeMeasurements(fileIterator: Iterator[String]) = {

    printHeader

    if(fileIterator.hasNext) {
      val firstAnalysisRowO: Option[MeasurementAnalysis] = getNextMeasurementAnalysis(fileIterator)
      firstAnalysisRowO.map(firstAnalysisRow => {
        val slidingWindowMeasurementQueue: Queue[MeasurementAnalysis] = Queue.empty[MeasurementAnalysis]
        analyzeR(slidingWindowMeasurementQueue.enqueue(firstAnalysisRow), fileIterator)
      })
    }
  }

  private def printHeader: Unit = {
    print( """T          V       N RS      MinV    MaxV""")
    print( """--------------------------------------------""")
  }

  private def getNextMeasurementAnalysis(fileIterator: Iterator[String]): Option[MeasurementAnalysis] = {
    if(fileIterator.hasNext) {
      val line = fileIterator.next()
      val measurementO: Option[Measurement] = parseMeasurement(line)
      measurementO match {
        case Some(measurement) => Some(populateMeasurementAnalysisFromMeasurement(measurement))
        case None => getNextMeasurementAnalysis(fileIterator)
      }
    } else None
  }

  private def parseMeasurement(line: String): Option[Measurement] = {
    val lineEntries: Array[String] = if(line.contains(" ")) line.split("\\s+")
    else line.split("\t")
    val measurementTry = Try(Measurement(lineEntries(0).toLong, lineEntries(1).toDouble))
    measurementTry match {
      case Success(measurement) => Some(measurement)
      case Failure(_) => None
    }
  }

  def analyzeR(bufferQueue: Queue[MeasurementAnalysis], fileIterator: Iterator[String]):Unit = {

    val (currentMeasurement, slidingWindowMeasurementQueue) = bufferQueue.dequeue
    
    val timeStampValueWithWindow = currentMeasurement.timeStamp + T

    val updatedQueueWithCurrentMeasurement: Queue[MeasurementAnalysis] = updateQueueWithCurrentMeasurement(currentMeasurement, slidingWindowMeasurementQueue)

    val bufferQueueWithNewMeasurements = addNewMeasurementsWithinSlidingRange(updatedQueueWithCurrentMeasurement, fileIterator, timeStampValueWithWindow, currentMeasurement)

    val measurementBufferForNextIteration: Queue[MeasurementAnalysis] = addNextEntryToBufferIfBufferIsEmpty(bufferQueueWithNewMeasurements, fileIterator)

    print(s"${currentMeasurement.timeStamp} ${formatter.format(currentMeasurement.priceRatio)} ${currentMeasurement.count} ${formatter.format(currentMeasurement.rs)} ${formatter.format(currentMeasurement.minV)} ${formatter.format(currentMeasurement.maxV)}")

    if(measurementBufferForNextIteration.nonEmpty)
      analyzeR(measurementBufferForNextIteration, fileIterator)

  }

  def updateQueueWithCurrentMeasurement(currentEntry: MeasurementAnalysis, bufferQueue: Queue[MeasurementAnalysis]): Queue[MeasurementAnalysis] = {
    val timeStampValueWithWindow = currentEntry.timeStamp + T

    def getMinimumValue(entry: MeasurementAnalysis): Double = {
      if (entry.minV > currentEntry.priceRatio)
        currentEntry.priceRatio
      else entry.minV
    }

    def getMaxValue(entry: MeasurementAnalysis): Double = {
      if (entry.maxV < currentEntry.priceRatio)
        currentEntry.priceRatio
      else entry.maxV
    }

    bufferQueue.foldLeft(Queue.empty[MeasurementAnalysis])((acc, entry) => {
      if (entry.timeStamp < timeStampValueWithWindow) {
        val newMinV = getMinimumValue(entry)
        val newMaxV = getMaxValue(entry)
        val newMeasurement = entry.copy(count = entry.count + 1, rs = entry.rs + currentEntry.priceRatio, minV = newMinV, maxV = newMaxV)
        acc.enqueue(newMeasurement)
      }
      else acc.enqueue(entry)
    })
  }

  def addNewMeasurementsWithinSlidingRange(bufferQueue: Queue[MeasurementAnalysis], file: Iterator[String], timeStampValueWithWindow:Long, currentEntry:MeasurementAnalysis):(Queue[MeasurementAnalysis]) = {
    if(file.hasNext) {
      val nextMeasurementAnalysisO: Option[MeasurementAnalysis] = getNextMeasurementAnalysis(file)
      nextMeasurementAnalysisO match {
        case Some(measurementAnalysis) => {
          if (measurementAnalysis.timeStamp <= timeStampValueWithWindow) {
            val (newMinV, newMaxV) = if (measurementAnalysis.priceRatio > currentEntry.priceRatio)
              (currentEntry.priceRatio, measurementAnalysis.priceRatio)
            else (measurementAnalysis.priceRatio, currentEntry.priceRatio)
            val analysisRow = MeasurementAnalysis(measurementAnalysis.timeStamp, measurementAnalysis.priceRatio, 2, measurementAnalysis.priceRatio + currentEntry.priceRatio, newMinV, newMaxV)
            addNewMeasurementsWithinSlidingRange(bufferQueue.enqueue(analysisRow), file, timeStampValueWithWindow, currentEntry)
          } else {
            bufferQueue.enqueue(measurementAnalysis)
          }
        }
        case None => bufferQueue
      }
    } else {
      bufferQueue
    }
  }

  def addNextEntryToBufferIfBufferIsEmpty(bufferQueueWithNewMeasurements: Queue[MeasurementAnalysis], fileIterator: Iterator[String]): Queue[MeasurementAnalysis] = {

    def isBufferEmptyAndFileNotCompleted: Boolean = bufferQueueWithNewMeasurements.isEmpty && fileIterator.hasNext

    if(isBufferEmptyAndFileNotCompleted) {
      val measurementAnalysisO: Option[MeasurementAnalysis] = getNextMeasurementAnalysis(fileIterator)
      measurementAnalysisO match {
        case Some(measurementAnalysis) => bufferQueueWithNewMeasurements.enqueue(measurementAnalysis)
        case None => bufferQueueWithNewMeasurements
      }
    } else bufferQueueWithNewMeasurements

  }

  private def populateMeasurementAnalysisFromMeasurement(measurement: Measurement): MeasurementAnalysis = {
    MeasurementAnalysis(measurement.timeStamp, measurement.priceRatio, 1, measurement.priceRatio, measurement.priceRatio, measurement.priceRatio)
  }
  
}

object TimeSeriesService {
  def main(args: Array[String]) {
    val timeSeriesService = new TimeSeriesService()

    val file = new File(args(0))
    val fileIt = Source.fromFile(file).getLines()
    timeSeriesService.analyzeMeasurements(fileIt)
  }
}
