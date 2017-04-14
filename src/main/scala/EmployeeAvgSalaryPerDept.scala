import com.twitter.scalding._

/**
  * Created by govind.bhone on 2/11/2017.
  */

class EmployeeAvgSalaryPerDept(args: Args) extends Job(args) {

  import EmployeeAvgSalaryPerDept._

  val input = args(inputArg)
  val output = args(outputArg)


  Csv(input, "\t", employeeSchema)
    .map(ipSalary -> ipSalary) {
      in: String =>
        val salary = in
        salary.replace("€", "").replace(",", "")
    }
    .groupBy(department) {
      _.average(ipSalary)
    }
    .write(Csv(args("output"),"\t"))

}


object EmployeeAvgSalaryPerDept {

  val inputArg = "input"
  val outputArg = "output"

  val id = 'id
  val firstName = 'firstName
  val lastName = 'lastName
  val email = 'email
  val department = 'department
  val ipSalary = 'ipSalary

  val employeeSchema = List(id, firstName, lastName, email, department, ipSalary)
}