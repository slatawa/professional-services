# Usage guidelines


## OVERVIEW:

There have been several upgrades in the DAG's from composer v1(airflow-1.x) to
composer v2(airflow 2.x) especially on the import statements and operators used. These changes when done manually 
can be time-consuming and error-prone. The objective of the tool is to automate the code changes from composer-v1(airflow-1.x) 
to composer-v2(airflow 2.x).


## DEPLOYMENT:

This tool takes the input DAG based on the location provided in the _--input_DAG parameter_. It reads 
through the input DAG and look for the operators to be changed based on the rules.csv file. The rules.csv can be 
custom created and provided to the tool using _--rules_file parameter_. The changes are written under output DAG file and 
saved under the location provided in _--output_DAG_ _parameter_. A default or custom comment is added above all the changes
made in the DAG file. The custom comment can be provided by setting _--add_comments_ to _TRUE_ and adding a custom comment under _--comments_ flag. 
The comments also indicate changes that need to be done manually for operator arguments. 
The tool takes 6 parameters - 2 required and 4 optional as listed below,

| Parameter Name    | Description                                                                | Required/Optional                                                                        |
|-------------------|----------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| input_dag_folder  | Input folder where Airflow 1.x DAG code is present                         | Required - supports local and gcs location                                               |
| output_dag_folder | Output folder location where the migrated code will be saved               | Required - supports local and gcs location                                               |
| rules_file        | Location to custom rules.csv file                                          | Optional - this will default to rules.csv packaged with the utility in case not provided |
| add_comments      | If Flag is True, custom comments can be added via the --comments parameter | Optional - True by default                                                               |
| comments          | Client can customize to custom comment                                     | Optional- by default will be a generic comment                                           |
| report_req        | True/False - create migration report in output DAG folder                  | Optional - by default will be True                                                       |


## Commands:

#### With mandatory parameters: 

``` 
python3 run_mig.py --input_dag_folder="Input-DAG-location" --output_dag_folder="Output-DAG-location" 
```

#### With all parameters: 

```` 
python3 run_mig.py --input_dag_folder="Input-DAG-location" --output_dag_folder="Output-DAG-location" --rules_file="Rules_folder location" --add_comments=FALSE --comments="Custom comments to be added for the changes" --report_req=TRUE
````

#### Sample Command

```
python3 run_mig.py --input_dag_folder=./input.dag --output_dag_folder=./output.dag  --rules_file=./migration_rules/rules.csv --add_comments=TRUE  --comments=”Operator Name changed” --report_req=TRUE
```


## PRE-REQUISITES: 

1. DAG folders,which are to be migrated must be available on the local machine.
2. Airflow v1 to v2 migration tool repo to be forked to the local machine.


## TEST-ENV SETUP

| Airflow 1.x Environment | Airflow 2.x Environment                        |
|-------------------------|------------------------------------------------|
| Python 3.7.1            | Python 3.7.1                                   |
| Airflow - 1.10.15       | Airflow - 2.5.3                                |
|                         | apache-airflow-providers-google version 8.12.0 |


## REPORT GENERATION

**** Include info on the report generation once feature implemented ***

## LIMITATIONS: 

1. This tool makes changes only to the import statements and operators based on the default rules.csv or the custom
rules.csv.
2. For possible changes to the arguments of the operators itself, a comment indicating the change to be made would be added above
the operator. These changes however must be done manually. ** Modify this comment once the operator change feature is implemented ** 

