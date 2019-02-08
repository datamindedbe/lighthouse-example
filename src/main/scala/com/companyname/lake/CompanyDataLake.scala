package com.companyname.lake

import java.time.LocalDate

import be.dataminded.lighthouse.datalake._
import be.dataminded.lighthouse.spark._
import com.companyname.lake.DataUIDs._
import org.apache.spark.sql.SaveMode

class CompanyDataLake(val localDate: LocalDate) extends Datalake {

  //the name argument here must exactly match the environment you are passing from airflow via the -e argument
  environment("PROD") { refs => buildEnvironment(refs, new CompanyProdEnvironment) }
  environment("TEST") { refs => buildEnvironment(refs, new CompanyTestEnvironment) }

  private def buildEnvironment(refs: EnvironmentBuilder, environment: CompanyEnvironment) = {
    //define functions to calculate the path to your data.
    //separate your data into raw/src, project

    def getPath(folder: String) = s"s3://${environment.bucketName}/$folder"

    def getRawPath(folder: String) = {
      getPath("raw/" + folder)
    }

    def getCleanPath(folder: String) = {
      getPath("clean/" + folder)
    }

    def getProjectPath(folder: String) = {
      getPath("project/" + folder)
    }

    def snapshotDataLinkParquet(path: String) = new SnapshotDataLink(
      new FileSystemDataLink(getRawPath(path), Parquet),
      localDate,
      None
    )

    //define all your data sources like this:
    refs += Raw.InputSourceFirst.inputSourceFirstDataFirst -> snapshotDataLinkParquet("inputSourceFirst/dataFirst")

    refs += Raw.InputSourceFirst.inputSourceFirstDataFirst -> snapshotDataLinkParquet("inputSourceFirst/dataFirst")

    //one does not need to use helper functions
    refs += Raw.InputSourceFirst.inputSourceFirstDataSecond -> new SnapshotDataLink(
      new FileSystemDataLink(getRawPath("inputSourceFirst/dataSecond"),Orc),
      localDate,
      None
    )

    //output to clean
    refs += Clean.firstCleanSource -> new SnapshotDataLink(
      new FileSystemDataLink(getCleanPath("inputSourceFirst/dataSecond"),Orc,SaveMode.Overwrite),
      localDate,
      None
    )

  }
}
