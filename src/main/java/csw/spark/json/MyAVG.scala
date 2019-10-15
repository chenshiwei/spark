package csw.spark.json

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/7/15
  */
object MyAVG extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(
        StructField("age", DoubleType, false) :: Nil
    )

    override def bufferSchema: StructType = StructType(
        StructField("total", DoubleType, false) ::
            StructField("count", IntegerType, false) :: Nil
    )

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0, 0.0)
        buffer.update(1, 0)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer.update(0, buffer.getDouble(0) + input.getDouble(0))
        buffer.update(1, buffer.getInt(1) + 1)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
        buffer1.update(1, buffer1.getInt(1) + buffer2.getInt(1))
    }

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true


    override def evaluate(buffer: Row): Any = buffer.getDouble(0) / buffer.getInt(1)
}
