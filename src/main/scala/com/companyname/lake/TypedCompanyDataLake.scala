package com.companyname.lake
import java.time.LocalDate

import be.dataminded.lighthouse.config.LighthouseConfiguration
import be.dataminded.lighthouse.datalake._
import be.dataminded.lighthouse.spark.{Parquet, SparkSessions}
import com.companyname.models._
import org.apache.spark.sql.Encoder

class TypedCompanyDataLake(
    environment: CompanyEnvironment,
    localDate: LocalDate
) extends Datalake
    with SparkSessions {
  import spark.implicits._

  private def getPath(folder: String) =
    s"s3://${environment.bucketName}/$folder"

  private def link[T: Encoder](path: String) =
    new TypedDataLink[T](
      new FileSystemDataLink(path, Parquet)
        .snapshotOf(localDate))

  object raw {

    val firstData: TypedDataLink[CaseClassOne] =
      link[CaseClassOne](getPath("raw/first/first"))

    val secondData: TypedDataLink[CaseClassTwo] =
      link[CaseClassTwo](getPath("raw/first/second"))
  }

  object clean {

    val firstCleanSource: TypedDataLink[CaseClassOneCleaned] =
      link[CaseClassOneCleaned](getPath("clean/cleanSource"))

  }

  object project {

    val firstProjectData: TypedDataLink[ProjectCaseClassOne] =
      link[ProjectCaseClassOne](getPath("project/projectFirst"))
  }

}

object TypedCompanyDataLake {

  def apply(
      config: LighthouseConfiguration
  ): TypedCompanyDataLake = {
    val env = config.environment match {
      case "PROD" => new CompanyProdEnvironment
      case _      => new CompanyTestEnvironment
    }

    new TypedCompanyDataLake(env, config.localDate)
  }
}
