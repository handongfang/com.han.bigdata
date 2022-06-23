import java.io.{BufferedReader, FileReader}
import java.util.function.IntFunction
import java.util.stream

class MutilIterableDemo() extends Iterable[String] {
  private val StrArray: Array[String] =
    ("It tests comprehension of common English expressions that include the word VOICE" +
      " It tests comprehension of common English expressions that include the word VOICE")
      .split(" ")

  /**
   * 正向迭代器
   * @return
   */
  override def iterator: Iterator[String] = {
    new Iterator[String]() {
      var index = 0

      override def hasNext: Boolean = {
        index < StrArray.length
      }

      override def next(): String = {
        val str = StrArray.apply(index)
        index += 1
        str
      }
    }
  }

  /**
   * 反向迭代器
   * @return
   */
  def reverseIterator: Iterable[String] = {
    new Iterable[String]() {
      override def iterator: Iterator[String] = {
        new Iterator[String]() {
          var index = StrArray.length - 1

          override def hasNext: Boolean = {
            index > -1
          }

          override def next(): String = {
            val str = StrArray.apply(index)
            index -= 1
            str
          }
        }
      }
    }
  }

}

object MutilIterableDemo {
  def main(args: Array[String]): Unit = {
    val mutilIterable = new MutilIterableDemo()
    val iterator = mutilIterable.iterator
    val reverseIterator = mutilIterable.reverseIterator

    iterator.foreach(println)
    println("=" * 50)
    reverseIterator.foreach(println)
    println("=" * 50)

    // iterator没有groupBy
    // iterator.flatMap(_.split(" ")).map((_,1)).groupBy().foreach(println)
    // iterable是iterator进一步封装，提供更多方法，例如groupBy
    mutilIterable.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(iter=>iter.size).foreach(println)

  }
}