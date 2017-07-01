package output

import java.math.RoundingMode
import java.text.NumberFormat
import java.util.Locale

import models.MeasurementAnalysis

trait Output {
  def print(s: String) = println(s)
}


class OutputToConsole extends Output{

  def printHeader = {
    print( """T          V       N RS      MinV    MaxV""")
    print( """--------------------------------------------""")
  }

  def writeToStream(measurementAnalysis: MeasurementAnalysis) = {
    val formatter = NumberFormat.getInstance(Locale.US)
    formatter.setMaximumFractionDigits(5)
    formatter.setRoundingMode(RoundingMode.HALF_UP)

    print(s"${measurementAnalysis.timeStamp} ${formatter.format(measurementAnalysis.priceRatio)} ${measurementAnalysis.count} ${formatter.format(measurementAnalysis.rs)} ${formatter.format(measurementAnalysis.minV)} ${formatter.format(measurementAnalysis.maxV)}")

  }
}