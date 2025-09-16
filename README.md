For optimizing the spark session in GCP Dataproc cluster use the following

from pyspark.sql import SparkSession

# Spark Session Configuration Example
# -----------------------------------
# These configs are tuned for performance and readability.
# NOTE: Executor/driver memory and cores must be set BEFORE
# SparkSession is created (or via spark-submit). If you call
# getOrCreate() in a notebook with an existing session, only
# runtime SQL configs (like shuffle partitions) will apply.

spark = (
    SparkSession.builder
    .appName("Olist Ecommerce Performance Optimization")

    # Executor settings (per JVM process)
    .config("spark.executor.memory", "6g")         # Memory per executor
    .config("spark.executor.cores", "4")           # Cores per executor
    .config("spark.executor.instances", "2")       # Number of executors

    # Driver settings
    .config("spark.driver.memory", "4g")           # Driver JVM heap memory
    .config("spark.driver.maxResultSize", "2g")    # Max result size to driver

    # Shuffle & parallelism
    .config("spark.sql.shuffle.partitions", "64")  # Default shuffle partitions (default: 200)
    .config("spark.default.parallelism", "64")     # Default parallelism for RDDs

    # Adaptive Query Execution (AQE) for dynamic optimization
    .config("spark.sql.adaptive.enabled", "true")                   # Enable AQE
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")# Coalesce small shuffle partitions

    # Join optimizations
    # Default broadcast join threshold = 10MB
    # WARNING: avoid setting this to GBs unless sure executors can handle it
    .config("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50 MB

    # File reading optimizations
    .config("spark.sql.files.maxPartitionBytes", "128MB")   # Target partition size when reading files
    .config("spark.sql.files.openCostInBytes", "4MB")       # Cost of opening small files (affects bin packing)

    # Memory management
    .config("spark.memory.fraction", 0.6)        # Fraction of heap for execution + storage (default: 0.6)
    .config("spark.memory.storageFraction", 0.5) # Fraction reserved for storage (default: 0.5)

    .getOrCreate()
)
