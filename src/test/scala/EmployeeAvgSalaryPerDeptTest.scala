import junit.framework.Assert
import org.specs2.mutable.Specification

/**
  * Created by govind.bhone on 2/11/2017.
  */
class EmployeeAvgSalaryPerDeptTest extends Specification {

  import EmployeeAvgSalaryPerDept._
  import com.twitter.scalding._

  val records = List(("1", "Randy", "Griffin", "rgriffin0@discuz.net", "Services", "€3244,84"),
    ("2", "Teresa", "Perry", "tperry1@microsoft.com", "Services", "€2138,48"),
    ("3", "Matthew", "Ellis", "mellis2@livejournal.com", "Human Resources", "€2431,08"),
    ("4", "Joyce", "Rogers", "jrogers3@miibeian.gov.cn", "Marketing", "€3100,05"))

  "A Avg Salary job" should {
    JobTest("EmployeeAvgSalaryPerDept")
      .arg(inputArg, inputArg)
      .arg(outputArg, outputArg)
      .source(Csv(inputArg, separator = "\t"), records)
      .sink[(String, String)](Csv(outputArg, "\t")) { outputBuffer =>
      val records = outputBuffer.toList
      Assert.assertEquals(records, List(("Human Resources", "243108.0"), ("Marketing", "310005.0"), ("Services", "269166.0")))
    }
      .run
      .finish
  }
}
