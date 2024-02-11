# Quantexa Scala Malaga
In this repo you will find the Dev Case code solution in Scala and the utilities to generate the solution for each question.

# Requirements
In order to compile, test, package, and run in a local spark cluster the following are required:
- SBT>=1.9<2.0
- Scala=2.12<=2.13
- JRE=8
- Docker
- **The data files**. This Git repo only has lightweight sample files. Replace them with the real files (included in the ZIP).

# How to
### Compile, test, and package
Use sbt!
```
$ sbt
>>> compile
>>> test
>>> package
```

### Run
Submit the job to a Spark cluster. To simulate this locally we use Docker.

You can either:
- Pull Apache's Sponsored `spark` image [here](https://hub.docker.com/r/apache/spark)
- Pull Docker's official `spark` image [here](https://hub.docker.com/_/spark).
- Or you can build your own using the `Dockerfile` provided in this repo as a template. Parametrize it for your desired Spark version [see the archive](https://archive.apache.org/dist/spark/). **Use only Spark WITH hadoop!**.
    DISCLAIMER: Potential errors issues with lower versions of Spark and Scala (i.e. Spark 2.4.8 and Scala 2.12.10).

**Using the script**
Use `run.sh <JOB_NAME>`.

Output is hardcoded to the `output` folder.

The available job names are:
    - `TotalFlightsPerMonth`
    - `MostFrequentFliers`
    - `LongestRunOutsideUK`
    - `TotalSharedFlights`
    - `TotalSharedFlightsInRange`
