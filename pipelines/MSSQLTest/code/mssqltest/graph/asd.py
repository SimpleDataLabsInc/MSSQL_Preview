from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mssqltest.config.ConfigStore import *
from mssqltest.udfs.UDFs import *

def asd(spark: SparkSession) -> DataFrame:
    reader = spark.read\
                 .format("com.microsoft.sqlserver.jdbc.spark")\
                 .option("url", None)\
                 .option(
                   "user",
                   "{}".format(
                     MSSQLProperties(  #skiptraversal
                       schema = None, 
                       description = "", 
                       credType = "secrets", 
                       secretScopeName = None, 
                       serverUrl = None, 
                       authType = "sql", 
                       tenantId = None, 
                       authority = "https://login.windows.net/", 
                       resourceAppIdUrl = None, 
                       spId = None, 
                       spSecret = None, 
                       encrypt = True, 
                       hostnameInCertificate = "*.database.windows.net", 
                       dbName = None, 
                       user = None, 
                       password = None, 
                       isSynapse = False, 
                       reliabilityLevel = "BEST_EFFORT", 
                       dataPoolDataSource = "none", 
                       isolationLevel = "READ_COMMITTED", 
                       tableLock = False, 
                       schemaCheckEnabled = True, 
                       readFromType = "dbtable", 
                       dbtable = None, 
                       query = None, 
                       partitionColumn = None, 
                       lowerBound = None, 
                       upperBound = None, 
                       pushDownPredicate = True, 
                       pushDownAggregate = None, 
                       numPartitions = None, 
                       queryTimeout = None, 
                       fetchsize = None, 
                       batchsize = None, 
                       sessionInitStatement = None, 
                       customSchema = None, 
                       truncate = None, 
                       cascadeTruncate = None, 
                       createTableOptions = None, 
                       createTableColumnTypes = None, 
                       writeMode = None
                     )\
                       .textUsername
                   )
                 )\
                 .option(
        "password",
        "{}".format(
          MSSQLProperties(  #skiptraversal
            schema = None, 
            description = "", 
            credType = "secrets", 
            secretScopeName = None, 
            serverUrl = None, 
            authType = "sql", 
            tenantId = None, 
            authority = "https://login.windows.net/", 
            resourceAppIdUrl = None, 
            spId = None, 
            spSecret = None, 
            encrypt = True, 
            hostnameInCertificate = "*.database.windows.net", 
            dbName = None, 
            user = None, 
            password = None, 
            isSynapse = False, 
            reliabilityLevel = "BEST_EFFORT", 
            dataPoolDataSource = "none", 
            isolationLevel = "READ_COMMITTED", 
            tableLock = False, 
            schemaCheckEnabled = True, 
            readFromType = "dbtable", 
            dbtable = None, 
            query = None, 
            partitionColumn = None, 
            lowerBound = None, 
            upperBound = None, 
            pushDownPredicate = True, 
            pushDownAggregate = None, 
            numPartitions = None, 
            queryTimeout = None, 
            fetchsize = None, 
            batchsize = None, 
            sessionInitStatement = None, 
            customSchema = None, 
            truncate = None, 
            cascadeTruncate = None, 
            createTableOptions = None, 
            createTableColumnTypes = None, 
            writeMode = None
          )\
            .textPassword
        )
                 )

    if (
        MSSQLProperties(#skiptraversal
schema = None, description = "", credType = "secrets", secretScopeName = None, serverUrl = None, authType = "sql", tenantId = None, authority = "https://login.windows.net/", resourceAppIdUrl = None, spId = None, spSecret = None, encrypt = True, hostnameInCertificate = "*.database.windows.net", dbName = None, user = None, password = None, isSynapse = False, reliabilityLevel = "BEST_EFFORT", dataPoolDataSource = "none", isolationLevel = "READ_COMMITTED", tableLock = False, schemaCheckEnabled = True, readFromType = "dbtable", dbtable = None, query = None, partitionColumn = None, lowerBound = None, upperBound = None, pushDownPredicate = True, pushDownAggregate = None, numPartitions = None, queryTimeout = None, fetchsize = None, batchsize = None, sessionInitStatement = None, customSchema = None, truncate = None, cascadeTruncate = None, createTableOptions = None, createTableColumnTypes = None, writeMode = None).readFromSource
        == "dbtable"
    ):
        reader = spark.read\
                     .format("com.microsoft.sqlserver.jdbc.spark")\
                     .option("url", None)\
                     .option(
                       "user",
                       "{}".format(
                         MSSQLProperties(  #skiptraversal
                           schema = None, 
                           description = "", 
                           credType = "secrets", 
                           secretScopeName = None, 
                           serverUrl = None, 
                           authType = "sql", 
                           tenantId = None, 
                           authority = "https://login.windows.net/", 
                           resourceAppIdUrl = None, 
                           spId = None, 
                           spSecret = None, 
                           encrypt = True, 
                           hostnameInCertificate = "*.database.windows.net", 
                           dbName = None, 
                           user = None, 
                           password = None, 
                           isSynapse = False, 
                           reliabilityLevel = "BEST_EFFORT", 
                           dataPoolDataSource = "none", 
                           isolationLevel = "READ_COMMITTED", 
                           tableLock = False, 
                           schemaCheckEnabled = True, 
                           readFromType = "dbtable", 
                           dbtable = None, 
                           query = None, 
                           partitionColumn = None, 
                           lowerBound = None, 
                           upperBound = None, 
                           pushDownPredicate = True, 
                           pushDownAggregate = None, 
                           numPartitions = None, 
                           queryTimeout = None, 
                           fetchsize = None, 
                           batchsize = None, 
                           sessionInitStatement = None, 
                           customSchema = None, 
                           truncate = None, 
                           cascadeTruncate = None, 
                           createTableOptions = None, 
                           createTableColumnTypes = None, 
                           writeMode = None
                         )\
                           .textUsername
                       )
                     )\
                     .option(
                       "password",
                       "{}".format(
                         MSSQLProperties(  #skiptraversal
                           schema = None, 
                           description = "", 
                           credType = "secrets", 
                           secretScopeName = None, 
                           serverUrl = None, 
                           authType = "sql", 
                           tenantId = None, 
                           authority = "https://login.windows.net/", 
                           resourceAppIdUrl = None, 
                           spId = None, 
                           spSecret = None, 
                           encrypt = True, 
                           hostnameInCertificate = "*.database.windows.net", 
                           dbName = None, 
                           user = None, 
                           password = None, 
                           isSynapse = False, 
                           reliabilityLevel = "BEST_EFFORT", 
                           dataPoolDataSource = "none", 
                           isolationLevel = "READ_COMMITTED", 
                           tableLock = False, 
                           schemaCheckEnabled = True, 
                           readFromType = "dbtable", 
                           dbtable = None, 
                           query = None, 
                           partitionColumn = None, 
                           lowerBound = None, 
                           upperBound = None, 
                           pushDownPredicate = True, 
                           pushDownAggregate = None, 
                           numPartitions = None, 
                           queryTimeout = None, 
                           fetchsize = None, 
                           batchsize = None, 
                           sessionInitStatement = None, 
                           customSchema = None, 
                           truncate = None, 
                           cascadeTruncate = None, 
                           createTableOptions = None, 
                           createTableColumnTypes = None, 
                           writeMode = None
                         )\
                           .textPassword
                       )
                     )\
                     .option("dbtable", None)
    elif (
        MSSQLProperties(#skiptraversal
schema = None, description = "", credType = "secrets", secretScopeName = None, serverUrl = None, authType = "sql", tenantId = None, authority = "https://login.windows.net/", resourceAppIdUrl = None, spId = None, spSecret = None, encrypt = True, hostnameInCertificate = "*.database.windows.net", dbName = None, user = None, password = None, isSynapse = False, reliabilityLevel = "BEST_EFFORT", dataPoolDataSource = "none", isolationLevel = "READ_COMMITTED", tableLock = False, schemaCheckEnabled = True, readFromType = "dbtable", dbtable = None, query = None, partitionColumn = None, lowerBound = None, upperBound = None, pushDownPredicate = True, pushDownAggregate = None, numPartitions = None, queryTimeout = None, fetchsize = None, batchsize = None, sessionInitStatement = None, customSchema = None, truncate = None, cascadeTruncate = None, createTableOptions = None, createTableColumnTypes = None, writeMode = None).readFromSource
        == "query"
    ):
        reader = spark.read\
                     .format("com.microsoft.sqlserver.jdbc.spark")\
                     .option("url", None)\
                     .option(
                       "user",
                       "{}".format(
                         MSSQLProperties(  #skiptraversal
                           schema = None, 
                           description = "", 
                           credType = "secrets", 
                           secretScopeName = None, 
                           serverUrl = None, 
                           authType = "sql", 
                           tenantId = None, 
                           authority = "https://login.windows.net/", 
                           resourceAppIdUrl = None, 
                           spId = None, 
                           spSecret = None, 
                           encrypt = True, 
                           hostnameInCertificate = "*.database.windows.net", 
                           dbName = None, 
                           user = None, 
                           password = None, 
                           isSynapse = False, 
                           reliabilityLevel = "BEST_EFFORT", 
                           dataPoolDataSource = "none", 
                           isolationLevel = "READ_COMMITTED", 
                           tableLock = False, 
                           schemaCheckEnabled = True, 
                           readFromType = "dbtable", 
                           dbtable = None, 
                           query = None, 
                           partitionColumn = None, 
                           lowerBound = None, 
                           upperBound = None, 
                           pushDownPredicate = True, 
                           pushDownAggregate = None, 
                           numPartitions = None, 
                           queryTimeout = None, 
                           fetchsize = None, 
                           batchsize = None, 
                           sessionInitStatement = None, 
                           customSchema = None, 
                           truncate = None, 
                           cascadeTruncate = None, 
                           createTableOptions = None, 
                           createTableColumnTypes = None, 
                           writeMode = None
                         )\
                           .textUsername
                       )
                     )\
                     .option(
                       "password",
                       "{}".format(
                         MSSQLProperties(  #skiptraversal
                           schema = None, 
                           description = "", 
                           credType = "secrets", 
                           secretScopeName = None, 
                           serverUrl = None, 
                           authType = "sql", 
                           tenantId = None, 
                           authority = "https://login.windows.net/", 
                           resourceAppIdUrl = None, 
                           spId = None, 
                           spSecret = None, 
                           encrypt = True, 
                           hostnameInCertificate = "*.database.windows.net", 
                           dbName = None, 
                           user = None, 
                           password = None, 
                           isSynapse = False, 
                           reliabilityLevel = "BEST_EFFORT", 
                           dataPoolDataSource = "none", 
                           isolationLevel = "READ_COMMITTED", 
                           tableLock = False, 
                           schemaCheckEnabled = True, 
                           readFromType = "dbtable", 
                           dbtable = None, 
                           query = None, 
                           partitionColumn = None, 
                           lowerBound = None, 
                           upperBound = None, 
                           pushDownPredicate = True, 
                           pushDownAggregate = None, 
                           numPartitions = None, 
                           queryTimeout = None, 
                           fetchsize = None, 
                           batchsize = None, 
                           sessionInitStatement = None, 
                           customSchema = None, 
                           truncate = None, 
                           cascadeTruncate = None, 
                           createTableOptions = None, 
                           createTableColumnTypes = None, 
                           writeMode = None
                         )\
                           .textPassword
                       )
                     )\
                     .option("query", None)

    reader = reader.option("pushDownPredicate", True)

    return reader.load()
