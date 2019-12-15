# 本子项目功能

目前本子项目完成的功能表述了可以用基本的scala-repl启动交互式spark

```bash
scala -cp $(echo lib/*.jar | tr ' ' :)
```

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

val conf: SparkConf = new SparkConf()
conf.setIfMissing("spark.testing.memory", "2147480000")
conf.setIfMissing("spark.master","local[4]")
conf.setIfMissing("spark.app.name", "Spark Shell Demo")
val builder = SparkSession.builder.config(conf)
val sparkSession: SparkSession = builder.getOrCreate()
val sc: SparkContext = sparkSession.sparkContext

sc.parallelize(1 to 10).sum()
```

# Spark-shell介绍

因为Spark-shell希望在启动的时候，就把Spark-Context初始化，所以重（抄）写了ILoop中的`process`方法

```scala
override def process(settings: Settings): Boolean = {
  ...
  /*
  if (isReplPower) {
    replProps.power setValue true
    unleashAndSetPhase()
    asyncMessage(power.banner)
  }
  loadInitFiles()
   */

  if (isReplPower) {
    enablePowerMode(true)
  }
  initializeSpark()
  loadInitFiles()
  ...
}
```

## 使用说明

目前这段代码无法提交到Yarn上面运行
这里为了不再引入额外的代码，采用手动初始化Spark Context，如下所示

```bash
/home/li/Software/scala-2.12.8/bin/scala -Dspark.repl.local.jars=$(echo *.jar lib/*.jar | tr ' ' ',') -cp spark-repl-1.0-SNAPSHOT.jar com.spark.repl.Main

scala> :reset
```
