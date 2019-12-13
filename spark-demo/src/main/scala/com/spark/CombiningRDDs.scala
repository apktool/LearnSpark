package com.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object CombiningRDDs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Computations").setMaster("local[4]")

    val sc = new SparkContext(conf)

    // put some data in an RDD
    val letters = sc.parallelize('a' to 'z', 8)

    // another RDD of the same type
    val vowels = sc.parallelize(Seq('a', 'e', 'i', 'o', 'u'), 4)

    // subtract on from another, getting yet another RDD of same type
    val consonants = letters.subtract(vowels)
    println("There are " + consonants.count() + " consonants")

    val vowelsNotLetters = vowels.subtract(letters)
    println("There are " + vowelsNotLetters.count() + " vowels that aren't letters")

    // union
    val lettersAgain = consonants ++ vowels
    println("There really are " + lettersAgain.count() + " letters")

    // union with duplicates removed
    val tooManyVowels = vowels ++ vowels
    println("There aren't really " + tooManyVowels.count() + " vowels")
    val justVowels = tooManyVowels.distinct()
    println("There aren actually " + justVowels.count() + " vowels")

    // subtraction with duplicates
    val what = tooManyVowels.subtract(vowels)
    println("There are actually " + what.count() + " whats")


    // intersection
    val earlyLetters = sc.parallelize('a' to 'l', 2)
    val earlyVowels = earlyLetters.intersection(vowels)
    println("The early vowels:")
    earlyVowels.foreach(println)

    // another RDD, same size and partitioning as letters
    val twentySix = sc.parallelize(101 to 126, 8)

    // RDD of a different type
    val numbers = sc.parallelize(1 to 2, 2)

    // cartesian product
    val cp = vowels.cartesian(numbers)
    println("Product has " + cp.count() + " elements")

    // index the letters
    val indexed = letters.zipWithIndex()
    println("indexed letter")
    indexed.foreach {
      case (c, i) => println(i + ": " + c)
    }

    // zip the letters and numbers
    val differentlyIndexed = letters.zip(twentySix)
    differentlyIndexed.foreach {
      case (c, i) => println(i + ": " + c)
    }

    val twentySixBadPart = sc.parallelize(101 to 126, 3)
    val cantGet = letters.zip(twentySixBadPart)
    try {
      cantGet.foreach {
        case (c, i) => println(i + ": " + c)
      }
    } catch {
      case iae: IllegalArgumentException =>
        println("Exception caught: " + iae.getMessage)
    }

    def zipFunc(lIter: Iterator[Char], nIter: Iterator[Int]): Iterator[(Char, Int)] = {
      val res = new ListBuffer[(Char, Int)]

      while (lIter.hasNext || nIter.hasNext) {
        if (lIter.hasNext && nIter.hasNext) {
          res += ((lIter.next(), nIter.next()))
        } else if (lIter.hasNext) {
          res += ((lIter.next(), 0))
        } else if (nIter.hasNext) {
          res += ((' ', nIter.next()))
        }
      }

      res.iterator
    }

    val unequalOK = earlyLetters.zipPartitions(numbers)(zipFunc)
    println("this may not be what you expected with unequal length RDDs")
    unequalOK foreach {
      case (c, i) => println(i + ":  " + c)
    }
  }
}