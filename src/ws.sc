val arg:List[Any] = "123" :: 452 :: Nil
val s = arg.mkString(",")

val e  = Map(45 -> arg)
val r = List(e,e)
def listToString(l:List[Any])={
  "[\n    " + l.mkString(",\n    ") + "\n]"
}
def mapToString(m:Map[Int, List[Any]])={
  val key = m.keys.head
  "\n  \"" + key + "\" : " + listToString(m(key))
}
"{" + r.map(r => mapToString(r)).mkString(",") + "\n}"