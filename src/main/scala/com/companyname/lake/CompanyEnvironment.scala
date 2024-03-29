package com.companyname.lake

abstract class CompanyEnvironment {
  //for AWS, use different buckets for test/acc/prod
  val bucketName: String

  //for Azure, use different containers and/or storage accounts for test/acc/prod
  val containerName: String
  val storageAccount: String

  //add any other fields which needs to depend on environment, e.g. DB connections
}

class CompanyTestEnvironment extends CompanyEnvironment {
  override  val bucketName: String = "testBucket"

  override  val containerName: String = "testContainer"
  override  val storageAccount: String = "testStorageAccount"

}

class CompanyProdEnvironment extends CompanyEnvironment {
  override  val bucketName: String = "prodBucket"

  override  val containerName: String = "prodContainer"
  override  val storageAccount: String = "prodStorageAccount"

}

