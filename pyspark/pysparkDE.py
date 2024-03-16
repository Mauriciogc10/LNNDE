from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,concat

# Create SparkSession
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Create sample data
employee_data = [
    (10,"Alice", "Marketing","100", 3000),
    (20,"Bob", "Engineering","200", 4000),
    (30,"Charlie", "Sales","100", 2200),
    (40,"David", "Marketing","400", 2500),
    (50,"Eve", "Engineering","500" ,5000),
]

# Create DataFrame
df_employee = spark.createDataFrame(employee_data, ["id","name", "department","employee_dept_id" ,"salary"])

# Filter for salaries greater than 2444
filtered_df = df_employee.filter(df_employee.salary > 2444)

df_employee.withColumn("new_column_name",value)

df_employee.withColumnRenamed("old_name","new_name")
df_employee.drop("column_name")

empDF_addcolumn = df_employee.withColumn("Location",lit("Mumbai")).show()

empDF_addcolumnbonus = df_employee.withColumn("Bonus",df_employee.salary*0.1).withColumn("Name",concat("first_name",lit(" "),"last_name"))

empDF_dropcolumn= empDF_addcolumnbonus.drop("name").show()

###Join Methods
department_data  = [
    ("HR","100"),
    ("Supply","200"),
    ("Sales","300"),
    ("Stock","400"),
]
department_schema =["dept_name","dept_id"]
df_deparment = spark.createDataFrame(data=department_data,schema= department_schema)

##inner join
df_join= df_employee.join(df_deparment.df_employee.employee_dept_id == df_deparment.dept_id,"inner")

