package models

case class MeasurementAnalysis(timeStamp:Long, priceRatio: Double, count: Int, rs: Double, minV:Double, maxV:Double)

case class Measurement(timeStamp:Long, priceRatio: Double)
