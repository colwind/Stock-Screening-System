object test extends App{
  def insert[K,V](k: K, v: V, m: Map[K, List[V]]): Map[K, List[V]] = {
    m + (k -> (m.getOrElse(k, List()) :+ v))
  }

  insert('a', 25, Map('a' -> List(2)))
}

