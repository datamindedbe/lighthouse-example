package com.companyname.jobs
import be.dataminded.lighthouse.config.{LighthouseConfiguration, LighthouseConfigurationParser}
import be.dataminded.lighthouse.spark.SparkApplication
import com.companyname.lake.CompanyDataLake
import com.companyname.lake.DataUIDs._

object ProjectJob extends SparkApplication {
  val parser = new LighthouseConfigurationParser()

  parser.parse(args, LighthouseConfiguration()) match {
    case Some(config) => process(new CompanyDataLake(config.localDate))
    case None => System.exit(1)
  }


  def process(lake: CompanyDataLake) = {
    //read in your clean data, produce some project data
    val first = lake(Clean.firstCleanSource).read()
    val second = lake(Clean.secondCleanSource).read()
    val third = lake(Clean.thirdCleanSource).read()

    //do the transformations

    lake(Project.Intermediary.firstIntermediary).write(first)
  }
}
