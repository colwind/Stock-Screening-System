package Utils

import java.io.File

object CheckDirectory {

  def apply(dir_name: String): Unit = {
    val dir_str = if (!dir_name.endsWith(File.separator)) dir_name + File.separator else dir_name
    val dir: File = new File(dir_str)
    if (!dir.exists()) dir.mkdirs()
  }

}
