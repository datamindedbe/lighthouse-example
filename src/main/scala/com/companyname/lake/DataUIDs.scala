package com.clientname.lake
import be.dataminded.lighthouse.datalake.DataUID

object ClientDataUIDs {
  object Raw {
    object InputSourceFirst {
      val inputSourceFirstDataFirst = DataUID("raw/inputFirst", "dataFirst")
      val inputSourceFirstDataSecond = DataUID("raw/inputFirst", "dataSecond")
    }
    object InputSourceSecond {
      val inputSourceSecondDataFirst = DataUID("raw/inputSecond", "dataFirst")
    }
  }

  object Clean {
    val firstCleanSource = DataUID("clean", "first")
    val secondCleanSource = DataUID("clean", "second")
    val thirdCleanSource = DataUID("clean", "third")
  }

  object Project {
    object Intermediary {
      val firstIntermediary = DataUID("project/intermediary", "first")
      val secondIntermediary = DataUID("project/intermediary", "second")
    }

    val firstOutput = DataUID("project", "firstOutput")
    val thirdOutput = DataUID("project", "thirdOutput")
  }

}
