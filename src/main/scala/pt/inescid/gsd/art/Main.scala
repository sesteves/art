package pt.inescid.gsd.art

import java.util.ArrayList

import argonaut._, Argonaut._
import weka.classifiers.Classifier
import weka.classifiers.functions.LinearRegression
import weka.classifiers.trees.RandomForest
import weka.core._
import scala.collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by sesteves on 02-06-2015.
 */
object Main {


  def main(args: Array[String]): Unit = {

    val jsonStr = scala.io.Source.fromFile("sla").getLines.mkString
    val slas = jsonStr.decodeOption[List[SLA]].getOrElse(Nil)
    val sla = slas.find(_.application=="Ngrams").get
    println(slas)
    println(sla)



    val AttributeNames = Array("accuracy", "ingestionRate", "cost", "windowDuration", "execTime")

    val attributes = new ArrayList[Attribute]

    AttributeNames.foreach(attr => attributes.add(new Attribute(attr)))


    val trainingInstances = new Instances("art", attributes, 0)
    trainingInstances.setClassIndex(0)

    // val classifier: Classifier = new LinearRegression
    val classifier = new RandomForest

    val newInstance = new DenseInstance(AttributeNames.length)
    newInstance.setDataset(trainingInstances)
    newInstance.setValue(0, 90.0)
    newInstance.setValue(1, 14008.0)
    newInstance.setValue(2, 1.0)
    newInstance.setValue(3, 10.0)
    newInstance.setValue(4, 12260.0)

    trainingInstances.add(newInstance)
    classifier.buildClassifier(trainingInstances)



    val newInstance2 = new DenseInstance(AttributeNames.length)
    newInstance2.setDataset(trainingInstances)
    newInstance2.setValue(0, 20.0)
    newInstance2.setValue(1, 8000.0)
    newInstance2.setValue(2, 1.0)
    newInstance2.setValue(3, 10.0)
    newInstance2.setValue(4, 12260.0)
    trainingInstances.add(newInstance2)
    classifier.buildClassifier(trainingInstances)



    val instance = new DenseInstance(AttributeNames.length)
    instance.setDataset(trainingInstances)
    instance.setValue(0, 0.0)
    instance.setValue(1, 14008.0)
    instance.setValue(2, 1.0)
    instance.setValue(3, 10.0 )
    instance.setValue(4, 12260.0)

    println("Predicted Accuracy: " + classifier.classifyInstance(instance).toInt)



  }

}

