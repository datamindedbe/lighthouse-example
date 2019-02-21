package com.companyname.jobs
import be.dataminded.lighthouse.config.{LighthouseConfiguration, LighthouseConfigurationParser}
import be.dataminded.lighthouse.spark.SparkApplication
import com.companyname.lake.TypedCompanyDataLake

object ProjectJob extends SparkApplication {
  val parser = new LighthouseConfigurationParser()

  parser.parse(args, LighthouseConfiguration()) match {
    case Some(config) => process(TypedCompanyDataLake(config))
    case None => System.exit(1)
  }


  def process(lake: TypedCompanyDataLake) = {
    //read in your clean data, produce some project data
    val first = lake.clean.firstCleanSource.readTyped()

    //do the transformations

    lake.project.firstProjectData.writeTyped(first)
  }
}
