import java.io.{BufferedReader, FileReader}

class fileIteratorDemo(filePath: String) extends Iterator[String] {

  private val bufferedReader = new BufferedReader(new FileReader(filePath))
  private var line: String = _

  override def hasNext: Boolean = {
    line = bufferedReader.readLine()
    line != null
  }

  override def next(): String = {
    line
  }
}


object fileIteratorDemo {
  def main(args: Array[String]): Unit = {
    val fileIterator = new fileIteratorDemo("data/test/wordcount/input/wordstr.txt")
//    while(fileIteratorDemo.hasNext){
//      println(fileIteratorDemo.next())
//    }
    fileIterator.flatMap(line=>line.split(" ")).map((_,1)).foreach(println)
  }
}