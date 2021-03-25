package Utils

import scala.io.Source

object ReadFile {

  def readFromFileByLine(filePath:String): Array[String] = {
    // Read the file
    val source = Source.fromFile(filePath, "UTF-8")
    // Put lines in a array
    val lines = source.getLines().toArray
    source.close()
    //println(lines.size)
    lines
  }

  def KeyFileUtil(array:Array[String]): Map[String, String] = {
    var keyMapList = Map[String, String]()
    for (i <- array.indices) {
      //println(array(i))
      val lineArray: Array[String] = array(i).trim.split(",")
      //println(lineArray.size)
      if(lineArray.length==2){
        keyMapList = keyMapList ++ Map(lineArray(0).trim -> lineArray(1).trim)
      }else if(lineArray.length==1){
        keyMapList = keyMapList ++ Map(lineArray(0).trim -> "")
      }else{
        keyMapList = keyMapList ++ Map("-" -> "")
      }
    }
    keyMapList
  }

  def isEmpty(s: String): Boolean = (s == null) || (s.length==0)

}