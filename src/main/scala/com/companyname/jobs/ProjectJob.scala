package com.companyname.jobs
import be.dataminded.lighthouse.config.{LighthouseConfiguration, LighthouseConfigurationParser}
import be.dataminded.lighthouse.spark.SparkApplication
import com.companyname.lake.TypedCompanyDataLake
import com.companyname.models.{CaseClassOne, CaseClassOneCleaned, ProjectCaseClassOne}
import org.apache.spark.sql.Dataset

object ProjectJob extends SparkApplication {
  val parser = new LighthouseConfigurationParser()

  parser.parse(args, LighthouseConfiguration()) match {
    case Some(config) => process(TypedCompanyDataLake(config))
    case None         => System.exit(1)
  }

  import spark.implicits._

  def process(lake: TypedCompanyDataLake): Unit = {
    //read in your clean data, produce some project data
    val first = lake.clean.firstCleanSource.readTyped()

    //do the transformations
    val firstTransformed = first.transform(transformFirstDataSource)

    lake.project.firstProjectData.writeTyped(firstTransformed)
  }

  def transformFirstDataSource(ds: Dataset[CaseClassOneCleaned]): Dataset[ProjectCaseClassOne] = {
    //do the actual transformation, this is just a small example
    ds.filter(d => d.age.isDefined)
      .map(
        d =>
          ProjectCaseClassOne(
            name = d.name,
            address = d.address,
            age = d.age.get
        ))
  }
}
