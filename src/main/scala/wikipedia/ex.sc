
val t1 = List(1,2,4,5)
t1.map(x=>List(x+2))
t1.flatMap(x => List(x+2))


def func1( x: Int): List[(String, Int)] = {
  List(("hallo", 1))
}

val t2 = List(List("test", 4),List("test1", 2),List("test3", 5))
t2.map(x => x::"Bla"::Nil)
t2.flatMap(x => x::"Bla"::Nil)

val test = (1 to 5).map(i => for {
  x <- i
  y <- func1(x)
} yield (x, y))

