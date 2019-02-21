package com.companyname.models

case class CaseClassOne(
                         name: String,
                         address: String,
                         age: String
                       )

case class CaseClassOneCleaned(
                         name: String,
                         address: String,
                         age: Option[Int]
                       )

case class ProjectCaseClassOne(
                         name: String,
                         address: String,
                         age: Int
                       )

case class CaseClassTwo(
                         field1: String,
                         field2: String
                       )
