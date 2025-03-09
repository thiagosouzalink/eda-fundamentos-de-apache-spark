# Fundamentos de Apache Spark

![PYTHON](https://img.shields.io/static/v1?label=Python&message=v3.12.0&color=blue&logo=PYTHON)
![PYSPARK](https://img.shields.io/static/v1?label=PySpark&message=v3.5.5&color=blue&logo=APACHESPARK)
![HADOOP](https://img.shields.io/static/v1?label=Apache%20Hadoop&message=v3.3.0&color=yellow&logo=APACHEHADOOP&logoColor=yellow)

Curso Fundamentos de Apache Spark - Engenharia de Dados Academy

## Download and Install Java

<a href="https://www.oracle.com/java/technologies/javase-jdk11-downloads.html" target="_blank">Download Java 17</a>


## Download and extract Apache Spark

<a href="https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz" target="_blank">Download Apache Spark 3.5.5</a>


## Download winutils from github repository

Required files:
- `winutils.exe`
- `hadoop.dll`

<a href="https://www.oracle.com/java/technologies/javase-jdk11-downloads.html" target="_blank">Repository Winutils Hadoop 3.3.0</a>

## Configure Spark and Hadoop

### Configure the Spark Environment

1. Set the `SPARK_HOME` environment variable:
   - Right-click on "This PC" > "Properties" > "Advanced system settings".
   - Click on "Environment Variables".
   - Under "System variables", click "New" and add:
     - Name: `SPARK_HOME`
     - Value: The path where Spark was extracted (e.g., `C:\spark\spark-3.5.5-bin-hadoop3.3`).
2. Add the `bin` directory of Spark to the `Path` variable:
   - In "Path", add: `C:\spark\spark-3.5.5-bin-hadoop3.3\bin`.

### Install Hadoop

1. Extract Hadoop to a directory of your choice, for example, `C:\hadoop`.
2. Set the `HADOOP_HOME` environment variable:
   - Under "System variables", click "New" and add:
     - Name: `HADOOP_HOME`
     - Value: The path where Hadoop was extracted (e.g., `C:\hadoop`).
3. Add the `bin` directory of Hadoop to the `Path` variable:
   - In "Path", add: `C:\hadoop\bin`.
4. Add the `winutils.exe` and `hadoop.dll` files to `C:\hadoop\bin`.
