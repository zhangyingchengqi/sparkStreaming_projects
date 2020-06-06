package project2_wordcounts.streamingWC.utils


import java.io.{ ObjectInputStream, ObjectOutputStream }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import scala.reflect.ClassTag

/*
   在集群节点间进行数据传输会有大量序列化和反序列化操作， 通过引入broadcast对该变量进行广播。
   广播后的变量，会保证每个Executor的内存中，只驻留一份变量副本，而Executor中的task执行时共享该Executor中的那份变量副本。
   这样的话，可以大大减少变量副本的数量，从而减少网络传输的性能开销，并减少对Executor内存的占用开销，降低GC的频率。


   @transient ：  瞬态化，即这个变量不序列化
 */
case class BroadcastWrapper[T: ClassTag](
                                          @transient private val ssc: StreamingContext,
                                          @transient private val _v: T) {

  @transient private var v = ssc.sparkContext.broadcast(_v)

  /**
   * 广播变量是只读的，可以利用spark的unpersist()，它按照LRU( lease Recently used)最近最久没有使用原则删除老数据。
   * @param newValue
   * @param blocking
   */
  def update(newValue: T, blocking: Boolean = false): Unit = {
    v.unpersist(blocking)
    v = ssc.sparkContext.broadcast(newValue)
  }

  def value: T = v.value

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[T]]
  }
}
