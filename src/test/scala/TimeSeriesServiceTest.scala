import java.io.File

import models.MeasurementAnalysis
import org.scalatest.{FunSpec, Matchers}
import output.Output
import service.TimeSeriesService

import scala.collection.immutable.Queue
import scala.io.Source

class TimeSeriesServiceTest extends FunSpec with Matchers {

  describe("Time series service test") {

    trait MockOutput extends Output {
      var messages: Seq[String] = Seq()
      override def print(s: String) = messages = messages :+ s
    }

    val timeSeriesService = new TimeSeriesService() with MockOutput



    describe("analyze measurements") {
      it("should only contain header if file is empty") {
        val timeSeriesService = new TimeSeriesService() with MockOutput

        val file = new File(getClass.getClassLoader.getResource("EmptyFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()
        timeSeriesService.analyzeMeasurements(fileIt)
        timeSeriesService.messages should be(Seq("""T          V       N RS      MinV    MaxV""",
                                                """--------------------------------------------"""))
      }

      it("should contain analysis of the data of given file") {
        val timeSeriesService = new TimeSeriesService() with MockOutput

        val file = new File(getClass.getClassLoader.getResource("MultipleEntriesFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()
        timeSeriesService.analyzeMeasurements(fileIt)
        timeSeriesService.messages should be(Seq("""T          V       N RS      MinV    MaxV""",
          """--------------------------------------------""",
          """1355270609 1.80215 1 1.80215 1.80215 1.80215""",
          """1355270621 1.80185 2 3.604 1.80185 1.80215""",
          """1355270646 1.80195 3 5.40595 1.80185 1.80215""",
          """1355270702 1.80225 2 3.6042 1.80195 1.80225""",
          """1355270702 1.80215 3 5.40635 1.80195 1.80225""",
          """1355270829 1.80235 1 1.80235 1.80235 1.80235""",
          """1355270854 1.80205 2 3.6044 1.80205 1.80235""",
          """1355270868 1.80225 3 5.40665 1.80205 1.80235""",
          """1355271000 1.80245 1 1.80245 1.80245 1.80245""",
          """1355271023 1.80285 2 3.6053 1.80245 1.80285""",
          """1355271024 1.80275 3 5.40805 1.80245 1.80285""",
          """1355271026 1.80285 4 7.2109 1.80245 1.80285""",
          """1355271027 1.80265 5 9.01355 1.80245 1.80285""",
          """1355271056 1.80275 6 10.8163 1.80245 1.80285""",
          """1355271428 1.80265 1 1.80265 1.80265 1.80265""",
          """1355271466 1.80275 2 3.6054 1.80265 1.80275""",
          """1355271471 1.80295 3 5.40835 1.80265 1.80295""",
          """1355271507 1.80265 3 5.40835 1.80265 1.80295""",
          """1355271562 1.80275 2 3.6054 1.80265 1.80275""",
          """1355271588 1.80295 2 3.6057 1.80275 1.80295"""))
      }

      it("should ignore problematic data from given file") {
        val timeSeriesService = new TimeSeriesService() with MockOutput

        val file = new File(getClass.getClassLoader.getResource("MultipleEntriesWithSyntaxError.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()
        timeSeriesService.analyzeMeasurements(fileIt)
        timeSeriesService.messages should be(Seq(
          """T          V       N RS      MinV    MaxV""",
          """--------------------------------------------""",
          """1355270609 1.80215 1 1.80215 1.80215 1.80215""",
          """1355270621 1.80185 2 3.604 1.80185 1.80215""",
          """1355270646 1.80195 3 5.40595 1.80185 1.80215""",
          """1355270702 1.80225 2 3.6042 1.80195 1.80225""",
          """1355270702 1.80215 3 5.40635 1.80195 1.80225""",
          """1355270829 1.80235 1 1.80235 1.80235 1.80235""",
          """1355270854 1.80205 2 3.6044 1.80205 1.80235""",
          """1355270868 1.80225 3 5.40665 1.80205 1.80235""",
          """1355271000 1.80245 1 1.80245 1.80245 1.80245""",
          """1355271023 1.80285 2 3.6053 1.80245 1.80285""",
          """1355271026 1.80285 3 5.40815 1.80245 1.80285""",
          """1355271027 1.80265 4 7.2108 1.80245 1.80285""",
          """1355271056 1.80275 5 9.01355 1.80245 1.80285""",
          """1355271428 1.80265 1 1.80265 1.80265 1.80265""",
          """1355271466 1.80275 2 3.6054 1.80265 1.80275""",
          """1355271471 1.80295 3 5.40835 1.80265 1.80295""",
          """1355271507 1.80265 3 5.40835 1.80265 1.80295""",
          """1355271562 1.80275 2 3.6054 1.80265 1.80275""",
          """1355271588 1.80295 2 3.6057 1.80275 1.80295"""))
      }
    }

    describe("update queue with current measurement") {

      it("should return empty if buffer queue is empty") {
        val bufferQueue = Queue.empty[MeasurementAnalysis]
        val currentEntry = MeasurementAnalysis(1231,2.1,1,2.1,2.1,2.1)
        timeSeriesService.updateQueueWithCurrentMeasurement(currentEntry, bufferQueue) should be(bufferQueue)
      }

      it("should return buffer as it is if buffer entries timeStamp is more than sliding window") {
        val bufferQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(1431,2.1,1,2.1,2.1,2.1))
        val currentEntry = MeasurementAnalysis(1231,2.1,1,2.1,2.1,2.1)
        timeSeriesService.updateQueueWithCurrentMeasurement(currentEntry, bufferQueue) should be(bufferQueue)
      }

      it("should update all buffer entries with current measurement which have timestamp less than sliding window") {
        val bufferQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(1240,1.91,1,2.1,1.91, 2.1), MeasurementAnalysis(1431,2.1,1,2.1,2.1,2.1))
        val currentEntry = MeasurementAnalysis(1231,2.3,1,2.1,2.1,2.3)
        val expectedBufferQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(1240,1.91,2,4.4,1.91,2.3), MeasurementAnalysis(1431,2.1,1,2.1,2.1,2.1))
        timeSeriesService.updateQueueWithCurrentMeasurement(currentEntry, bufferQueue) should be(expectedBufferQueue)

      }

    }

    describe("add new measurements within sliding range") {

      it("it should return same queue and file iterator if end of file") {
        val file = new File(getClass.getClassLoader.getResource("EmptyFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()
        val currentEntry = MeasurementAnalysis(1231,2.1,1,2.1,2.1,2.1)
        val bufferedQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(123,2.1,1,2.1,2.1,2.1))

        timeSeriesService.addNewMeasurementsWithinSlidingRange(bufferedQueue, fileIt, 1231+60, currentEntry) should be((bufferedQueue))
      }

      it("it should return same queue and file iterator with one entry file") {
        val file = new File(getClass.getClassLoader.getResource("OneEntryFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()
        val currentEntry = MeasurementAnalysis(1231,2.1,1,2.1,2.1,2.1)
        val bufferedQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(123,2.1,1,2.1,2.1,2.1))
        fileIt.next()

        timeSeriesService.addNewMeasurementsWithinSlidingRange(bufferedQueue, fileIt, 1231+60, currentEntry) should be(bufferedQueue)
      }

      it("it should return same queue and file iterator when timestamp + window value is more than next entry going to read") {
        val file = new File(getClass.getClassLoader.getResource("OneEntryFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()
        val currentEntry = MeasurementAnalysis(123,2.1,1,2.1,2.1,2.1)
        val bufferedQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(1234,2.1,1,2.1,2.1,2.1))

        val expectedBufferQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(1234,2.1,1,2.1,2.1,2.1), MeasurementAnalysis(12345,1.4543,1,1.4543,1.4543,1.4543))

        timeSeriesService.addNewMeasurementsWithinSlidingRange(bufferedQueue, fileIt, 123+60, currentEntry) should be(expectedBufferQueue)
      }

      it("it should add next line entry to buffer queue when its timestamp is less than current timestamp + window") {
        val file = new File(getClass.getClassLoader.getResource("OneEntryFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()
        val currentEntry = MeasurementAnalysis(12341,2.1,1,2.1,2.1,2.1)
        val bufferedQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(1234,2.1,1,2.1,2.1,2.1))
        val expectedBufferedQueue = bufferedQueue.enqueue(MeasurementAnalysis(12345, 1.4543, 2,  1.4543 + 2.1, 1.4543 , 2.1))


        timeSeriesService.addNewMeasurementsWithinSlidingRange(bufferedQueue, fileIt, 12341+60, currentEntry) should be(expectedBufferedQueue)
      }

      it("it should add next line entry to buffer queue when its timestamp is less than current timestamp + window and exclude rest") {
        val file = new File(getClass.getClassLoader.getResource("TwoEntryFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()
        val currentEntry = MeasurementAnalysis(12341,2.1,1,2.1,1.1,3.7)
        val bufferedQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(1234,2.1,1,2.1,2.1,2.1))
        val expectedBufferedQueue = bufferedQueue.enqueue(MeasurementAnalysis(12401, 3.4543, 2,  3.4543 + 2.1, 2.1, 3.4543)).enqueue(MeasurementAnalysis(12402,1.4543,1,1.4543,1.4543,1.4543))


        timeSeriesService.addNewMeasurementsWithinSlidingRange(bufferedQueue, fileIt, 12341+60, currentEntry) should be(expectedBufferedQueue)
      }

    }

    describe("add next entry to buffer if buffer is empty") {
      it("should return buffer queue as it is if it's not empty") {
        val bufferedQueue = Queue[MeasurementAnalysis](MeasurementAnalysis(1234,2.1,1,2.1,2.1,2.1))
        val file = new File(getClass.getClassLoader.getResource("EmptyFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()

        timeSeriesService.addNextEntryToBufferIfBufferIsEmpty(bufferedQueue, fileIt) should be(bufferedQueue)

      }

      it("should return buffer queue as it is if buffer is empty and file is empty") {
        val bufferedQueue = Queue.empty[MeasurementAnalysis]
        val file = new File(getClass.getClassLoader.getResource("EmptyFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()

        timeSeriesService.addNextEntryToBufferIfBufferIsEmpty(bufferedQueue, fileIt) should be(bufferedQueue)
      }

      it("should add new line if buffer is empty and file is not empty") {
        val bufferedQueue = Queue.empty[MeasurementAnalysis]
        val file = new File(getClass.getClassLoader.getResource("TwoEntryFile.txt").getPath)
        val fileIt = Source.fromFile(file).getLines()
        val expectedBuffer = Queue(MeasurementAnalysis(12401,3.4543,1,3.4543,3.4543,3.4543))

        timeSeriesService.addNextEntryToBufferIfBufferIsEmpty(bufferedQueue, fileIt) should be(expectedBuffer)
      }
    }
  }

}
