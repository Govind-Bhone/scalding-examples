#!/bin/sh

pig -useHCatalog -stop_on_failure ./pig/average_salary_per_dept.pig;