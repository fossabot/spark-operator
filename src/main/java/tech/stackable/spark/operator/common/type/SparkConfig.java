package tech.stackable.spark.operator.common.type;

public enum SparkConfig {
	/*
	sparn-env.sh
	# Options read when launching programs locally with
	# ./bin/run-example or ./bin/spark-submit
	# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
	# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
	# - SPARK_PUBLIC_DNS, to set the public dns name of the driver program

	# Options read by executors and drivers running inside the cluster
	# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
	# - SPARK_PUBLIC_DNS, to set the public DNS name of the driver program
	# - SPARK_LOCAL_DIRS, storage directories to use on this node for shuffle and RDD data
	# - MESOS_NATIVE_JAVA_LIBRARY, to point to your libmesos.so if you use Mesos

	# Options read in YARN client/cluster mode
	# - SPARK_CONF_DIR, Alternate conf dir. (Default: ${SPARK_HOME}/conf)
	# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
	# - YARN_CONF_DIR, to point Spark towards YARN configuration files when you use YARN
	# - SPARK_EXECUTOR_CORES, Number of cores for the executors (Default: 1).
	# - SPARK_EXECUTOR_MEMORY, Memory per Executor (e.g. 1000M, 2G) (Default: 1G)
	# - SPARK_DRIVER_MEMORY, Memory for Driver (e.g. 1000M, 2G) (Default: 1G)

	# Options for the daemons used in the standalone deploy mode
	# - SPARK_MASTER_HOST, to bind the master to a different IP address or hostname
	# - SPARK_MASTER_PORT / SPARK_MASTER_WEBUI_PORT, to use non-default ports for the master
	# - SPARK_MASTER_OPTS, to set config properties only for the master (e.g. "-Dx=y")
	# - SPARK_WORKER_CORES, to set the number of cores to use on this machine
	# - SPARK_WORKER_MEMORY, to set how much total memory workers have to give executors (e.g. 1000m, 2g)
	# - SPARK_WORKER_PORT / SPARK_WORKER_WEBUI_PORT, to use non-default ports for the worker
	# - SPARK_WORKER_DIR, to set the working directory of worker processes
	# - SPARK_WORKER_OPTS, to set config properties only for the worker (e.g. "-Dx=y")
	# - SPARK_DAEMON_MEMORY, to allocate to the master, worker and history server themselves (default: 1g).
	# - SPARK_HISTORY_OPTS, to set config properties only for the history server (e.g. "-Dx=y")
	# - SPARK_SHUFFLE_OPTS, to set config properties only for the external shuffle service (e.g. "-Dx=y")
	# - SPARK_DAEMON_JAVA_OPTS, to set config properties for all daemons (e.g. "-Dx=y")
	# - SPARK_DAEMON_CLASSPATH, to set the classpath for all daemons
	# - SPARK_PUBLIC_DNS, to set the public dns name of the master or workers

	# Options for launcher
	# - SPARK_LAUNCHER_OPTS, to set config properties and Java options for the launcher (e.g. "-Dx=y")

	# Generic options for the daemons used in the standalone deploy mode
	# - SPARK_CONF_DIR      Alternate conf dir. (Default: ${SPARK_HOME}/conf)
	# - SPARK_LOG_DIR       Where log files are stored.  (Default: ${SPARK_HOME}/logs)
	# - SPARK_PID_DIR       Where the pid file is stored. (Default: /tmp)
	# - SPARK_IDENT_STRING  A string representing this instance of spark. (Default: $USER)
	# - SPARK_NICENESS      The scheduling priority for daemons. (Default: 0)
	# - SPARK_NO_DAEMONIZE  Run the proposed command in the foreground. It will not output a PID file.
	# Options for native BLAS, like Intel MKL, OpenBLAS, and so on.
	# You might get better performance to enable these options if using native BLAS (see SPARK-21305).
	# - MKL_NUM_THREADS=1        Disable multi-threading of Intel MKL
	# - OPENBLAS_NUM_THREADS=1   Disable multi-threading of OpenBLAS
	*/

  // Options read in YARN client/cluster mode
  /**
   * Alternate conf dir. (Default: ${SPARK_HOME}/conf)
   */
  SPARK_CONF_DIR("SPARK_CONF_DIR", "spark.conf.dir"),
  /**
   * to point Spark towards Hadoop configuration files
   */
  HADOOP_CONF_DIR("HADOOP_CONF_DIR", "hadoop.conf.dir"),
  /**
   * Number of cores for the executors (Default: 1).
   */
  SPARK_EXECUTOR_CORES("SPARK_EXECUTOR_CORES", "spark.executor.cores"),
  /**
   * Memory per Executor (e.g. 1000M, 2G) (Default: 1G)
   */
  SPARK_EXECUTOR_MEMORY("SPARK_EXECUTOR_MEMORY", "spark.executor.memory"),
  /**
   * Number of cores for the driver
   */
  SPARK_DRIVER_CORES("SPARK_DRIVER_MEMORY", "spark.driver.cores"),
  /**
   * Memory for Driver (e.g. 1000M, 2G) (Default: 1G)
   */
  SPARK_DRIVER_MEMORY("SPARK_DRIVER_MEMORY", "spark.driver.memory"),

  // Options for the daemons used in the standalone deploy mode
  /**
   * to bind the master to a different IP address or hostname
   */
  SPARK_MASTER_HOST("SPARK_MASTER_HOST", "spark.master.host"),
  /**
   * to use non-default master port
   */
  SPARK_MASTER_PORT("SPARK_MASTER_PORT", "spark.master.port"),
  /**
   * to use non-default web ui port for the master
   */
  SPARK_MASTER_WEBUI_PORT("SPARK_MASTER_WEBUI_PORT", "spark.master.webui.port"),
  /**
   * to set the number of cores to use on this machine
   */
  SPARK_WORKER_CORES("SPARK_WORKER_CORES", "spark.worker.cores"),
  /**
   * to set how much total memory workers have to give executors (e.g. 1000m, 2g)
   */
  SPARK_WORKER_MEMORY("SPARK_WORKER_MEMORY", "spark.worker.memory"),
  /**
   * set (true) to activate authentication
   */
  SPARK_AUTHENTICATE("SPARK_AUTHENTICATE", "spark.authenticate"),
  /**
   * set secret for authentication
   */
  SPARK_AUTHENTICATE_SECRET("SPARK_AUTHENTICATE_SECRET", "spark.authenticate.secret");

  private String env;
  private String config;

  SparkConfig(String env, String config) {
    this.env = env;
    this.config = config;
  }

  public String getEnv() {
    return env;
  }

  public String getConfig() {
    return config;
  }
}
