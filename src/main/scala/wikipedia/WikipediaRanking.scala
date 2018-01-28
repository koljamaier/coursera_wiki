package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Int = if(text.split(' ').contains(lang)) 1 else 0
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")


  val conf: SparkConf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("WikiEx")
  val sc: SparkContext = new SparkContext(conf)


  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(article => WikipediaData.parse(article)).persist()

  /** Returns the number of articles on which the language `lang` occurs.
    * Hint1: consider using method `aggregate` on RDD[T].
    * Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
    *
    * def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = rdd.aggregate(0)(
    (acc, article) => acc + article.mentionsLanguage(lang),
    (acc, article) => acc + article
  )


  // for reverse order
  implicit val sortIntegersByString = new Ordering[Int] {
    override def compare(a: Int, b: Int) = b.compare(a)
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   *
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val ranks = langs.map(lang => (lang, occurrencesOfLang(lang, rdd)))
    //for{ lang <- langs; occ = occurrencesOfLang(lang, rdd) if occ != 0} yield (lang, occ)
    ranks.sortBy(_._2)(sortIntegersByString)

  }


  def makeIndex1(langs1: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, List[WikipediaArticle])] = {
    val invertRdd = langs1.map(lang => rdd
    .filter(article => if(article.mentionsLanguage(lang)>0) true else false).map(farticle => (lang, List(farticle))))

    // Reduce List[RDD] to one RDD
    val single_rdd: RDD[(String, List[WikipediaArticle])] = invertRdd.reduce(_ union _)

    // Concatenate lists via ++ Operator
    single_rdd.reduceByKey(_ ++ _)

  }


  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   * Index: langs -> (lang, Collection(WikipepdiaArticle))
   *
   * flatMap and groupByKey on RDD for this part
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {

    val list = rdd.flatMap(article => for( lang <- langs if article.mentionsLanguage(lang) == 1) yield (lang, article))
    list.groupByKey()
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    val t1 = for ((lang, article) <- index) yield (lang, article.size)
    t1.reduceByKey(_+_).collect().toList.sortBy(_._2)(sortIntegersByString)
  }

  def rankLangsUsingIndex1(index: RDD[(String, List[WikipediaArticle])]): List[(String, Int)] = {
    val t1 = for ((lang, article) <- index) yield (lang, article.size)
    t1.reduceByKey(_+_).collect().toList.sortBy(_._2)(sortIntegersByString)
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   *
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rankLangsUsingIndex(makeIndex(langs, rdd))
  }


  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))
    print(langsRanked)

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    def index1: RDD[(String, List[WikipediaArticle])] = makeIndex1(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    val langsRanked22: List[(String, Int)] = timed("Part 4242: ranking using inverted index", rankLangsUsingIndex1(index1))
    print(langsRanked2)
    print("BLABLABLALBAL")
    print(langsRanked22)

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
