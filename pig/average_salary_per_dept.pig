load_csv_data = LOAD './data/salaries.tsv' USING PigStorage('\t') AS (id:chararray,first_name:chararray,last_name:chararray,
    email:chararray,department:chararray,ip_salary:chararray);

format_salary_per_dept = FOREACH load_csv_data GENERATE  department as department:chararray,(double)REPLACE(REPLACE(ip_salary,'€',''),',','') as ip_salary:double;

group_data_by_dept = GROUP format_salary_per_dept BY department ;

avg_salary_per_dept = FOREACH group_data_by_dept  GENERATE FLATTEN(group) as department:chararray ,AVG(format_salary_per_dept.ip_salary) as avg_salary:chararray;

STORE avg_salary_per_dept INTO './data/output' USING PigStorage('\t');
