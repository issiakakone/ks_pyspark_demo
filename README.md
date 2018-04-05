
# KoneSoft PySpark Example

Welcome to an end to end demo example of PySpark DataFrame

Please see also for the project specification document: [requirement](./doc/specification.md)

## Build and package the project

To build the deliverable package, please run the following command in the shell script:

```sh
mvn clean package
```

The deliverable package can be found in the target folder in zip and tar.gz format under the name like:
* ks_pyspark_demo-1.0.0-akuna-matata.zip
* ks_pyspark_demo-1.0.0-akuna-matata.tar.gz

## Run the project

To run this project, extract the package in a desired folder. Set the environment  variable `KS_SPARK_HOME` to that folder.
Then execute the following command:

```sh
export KS_SPARK_HOME='/path/to/extracted/folder'
chmod u+x $KS_SPARK_HOME/runner.bash
$KS_SPARK_HOME/runner.bash
```

This will submit the job to a local Spark standalone server. You can specify an existing Spark cluster by modifying the script.

## TODO

The function `toolkit.ConfigCheck(config)` must be implemented to check the validity of the configuration data semantically
The `runner.bash` can be modified to take Spark option and configuration file as an argument
The logging system needs to be improved

## Input Data Format

The input file sample is below:

```
"identifiant";"Date";"Montant_1";"Montant_2";"Montant_3";"Télephone"
"1";"12/11/2015 23:22:26";"15.12";"17.12";"23.74";"06.23.15.48.29"
"2";"01/02/2013 08:29:45";"18.03";"15.12";"41.12";"07.26.95.15.48"
"3";"26/09/2014 15:36:35";"2215.06";"";"45.92";"06.98.65.12.32"
"4";"19/12/2014 13:28:26";"6.29";"81.01";"";"01.26 58 69.84"
"5";"12/11/2015 23:22:41";"48.256";"";"";"04.68.87.34.10"
"6";"01/02/2013 08:29:59";"84.2";"15.12";"15.12";"07 26 89 15 26"
"7";"26/09/2014 15:36:35";"165";"15.12";"15.12";"0625689515"
"8";"19/12/2014 13:28:28";"14.02";"15.12";"15.12";"07.21.36.35.48"
"9";"12/11/2015 23:22:39";"95.35";"15.12";"15.12";"0156248628"
"10";"01/02/2013 08:29:01";"";"";"";"04.26.58.95.12"
"11";"44/99/0015 23:22:39";"95.35";"15.12";"15.12";"0156248628"
"12";"0 08:29:01";"";"";"";"04.26.58.95.12"
```