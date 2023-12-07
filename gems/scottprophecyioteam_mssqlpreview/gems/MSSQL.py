
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.NumberUtils import parseInt
from prophecy.cb.util.StringUtils import isBlank


class MSSQLFormat(DatasetSpec):
    name: str = "MSSQL"
    datasetType: str = "Warehouse"

    def optimizeCode(self) -> bool:
        return True

    # todo
    #  These 4 props are defined but not used in any binding in the dialogs: customSchema, keytab, principal, refreshKrb5Config
    @dataclass(frozen=True)
    class MSSQLProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""

        credType: str = "secrets" # vs "config"
        secretScopeName: Optional[str] = None
        serverUrl: Optional[str] = None

        authType: str = "sql" # vs "sp" vs "ad"
        tenantId: Optional[str] = None
        authority: str = "https://login.windows.net/"
        resourceAppIdUrl: Optional[str] = None
        spId: Optional[str] = None
        spSecret: Optional[str] = None
        encrypt: bool = True
        hostnameInCertificate:str = "*.database.windows.net"
        dbName:Optional[str] = None
        user:Optional[str] = None
        password:Optional[str] = None
        isSynapse:bool = False
        reliabilityLevel:str = "BEST_EFFORT" # NO_DUPLICATES
        dataPoolDataSource:str = "none"
        isolationLevel:str = "READ_COMMITTED" #W NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, or SERIALIZABLE
        tableLock:bool = False
        schemaCheckEnabled: bool = True
        readFromType: Optional[str] = "dbtable"
        dbtable: Optional[str] = None
        query: Optional[str] = None
        partitionColumn: Optional[str] = None #R
        lowerBound: Optional[str] = None #R
        upperBound: Optional[str] = None #R
        pushDownPredicate: Optional[bool] = True  #R
        pushDownAggregate: Optional[bool] = None #W
        numPartitions: Optional[str] = None #RW
        queryTimeout: Optional[str] = None #RW
        fetchsize: Optional[str] = None #R
        batchsize: Optional[str] = None #W
        sessionInitStatement: Optional[str] = None #R
        customSchema: Optional[str] = None  #R
        truncate: Optional[bool] = None #W
        cascadeTruncate: Optional[bool] = None #W
        createTableOptions: Optional[str] = None #W
        createTableColumnTypes: Optional[str] = None #W
        writeMode: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        fieldPickerDesc = FieldPicker(height=("100%")) \
            .addField(
            TextArea("Description", 2, placeholder="Dataset description..."),
            "description",
            True
        )

        spAuthView = StackLayout(direction=("vertical"), gap=("1rem")) \
            .addElement(TextBox("Tenant ID").bindProperty("tenantId")) \
            .addElement(TextBox("Authority").bindPlaceholder("https://login.windows.net/").bindProperty("authority")) \
            .addElement(TextBox("Resource App ID URL").bindProperty("resourceAppIdUrl")) \
            .addElement(TextBox("Service Principal ID").bindProperty("spId")) \
            .addElement(TextBox("Service Principal Secret").bindProperty("spSecret"))

        userPwdAuthView = StackLayout(direction=("vertical"), gap=("1rem")) \
            .addElement(TextBox("Username").bindProperty("user")) \
            .addElement(TextBox("Password").bindProperty("password"))

        locView = ColumnsLayout() \
            .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    .addElement(
                        RadioGroup("Secrets Type")
                            .addOption("Databricks Secrets", "secrets")
                            .addOption("Config Variables", "config")
                            .bindProperty("credType")
                    )
                    .addElement(
                        Condition()
                            .ifEqual(PropExpr("component.properties.credType"), StringExpr("secrets"))
                            .then(TextBox("Secret Scope name").bindProperty("secretScopeName"))
                    )
                    .addElement(
                        TextBox("Server URL").bindProperty("serverUrl")
                    )
                    .addElement(
                        TextBox("Database").bindProperty("dbName")
                    )
                    #.addElement(
                    #    Checkbox("Is this a Synapse instance?").bindProperty("isSynapse")
                    #)
                    .addElement(
                        RadioGroup("Authentication Type")
                            .addOption("ActiveDirectory (Entra)", "ad")
                            .addOption("SQL Server", "sql")
                            .addOption("Service Principal", "sp")
                            .bindProperty("authType")
                    )
                    .addElement(
                        Condition()
                            .ifEqual(PropExpr("component.properties.authType"), StringExpr("ad"))
                            .then(userPwdAuthView)
                            .ifEqual(PropExpr("component.properties.authType"), StringExpr("sql"))
                            .then(userPwdAuthView)
                            .otherwise(spAuthView)
                    )
            )
        
        read_fields = {
            "Partition Column": "partitionColumn",
            "Lower Bound": "lowerBound",
            "Upper Bound": "upperBound",
            "Partition Count": "numPartitions",
            "Query Timeout": "queryTimeout",
            "Fetch Size": "fetchSize",
            "Session Init Statement": "sessionInitStatement",
            "Custom Schema": "customSchema"
        }

        readFP = FieldPicker(height=("100%"))
        for (k, v) in read_fields.items():
            readFP = readFP.addField(TextBox(k), v)

        readFP.addField(Checkbox("Push-down Predicate"), "pushDownPredicate")


        tableView = ColumnsLayout().addColumn(readFP, "auto") \
            .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem")) \
                    .addElement(
                        RadioGroup("Source type")
                            .addOption("Table", "dbtable")
                            .addOption("Query", "query")
                            .bindProperty("readFromType")
                    ) \
                    .addElement(
                        Condition()
                            .ifEqual(PropExpr("component.properties.readFromType"), StringExpr("dbtable"))
                            .then(TextBox("Table name").bindProperty("dbtable"))
                            .otherwise(TextArea("SQL Query", 4).bindProperty("query"))
                    ) 
            )

        return DatasetDialog("mssql") \
            .addSection(
                "LOCATION",
                locView
            ) \
            .addSection(
                "TABLE",
                tableView
            ) \
            .addSection(
                "SCHEMA",
                ColumnsLayout()
                    .addColumn(fieldPickerDesc, "auto")
                    .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr")
            ) \
            .addSection(
                "PREVIEW",
                PreviewTable("").bindProperty("schema")
            )

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("mssql")

    def validate(self, component: Component, foo) -> list:
        return []
        #diagnostics = super(MSSQLFormat, self).validate(component)

        #return diagnostics

    def onChange(self, oldState: Component, newState: Component) -> Component:
        return newState

    class MSSQLFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: MSSQLFormat.MSSQLProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            if self.props.authType == "sp":
                import adal
                # Do Service Principal auth here

            reader = spark.read.format("com.microsoft.sqlserver.jdbc.spark")
            reader = reader.option("url", self.props.serverUrl)
            
            reader = reader.option("user", f"{self.props.textUsername}")
            reader = reader.option("password", f"{self.props.textPassword}")

            if self.props.authType == "ad":
                reader = reader.option("authentication", "ActiveDirectoryPassword")

            if self.props.dbName is not None:
                reader = reader.option("databaseName", self.props.dbName)

            if self.props.readFromSource == "dbtable":
                reader = reader.option("dbtable", self.props.dbtable)
            elif self.props.readFromSource == "query":
                reader = reader.option("query", self.props.query)

            if self.props.partitionColumn is not None:
                reader = reader.option("partitionColumn", self.props.partitionColumn)
            if self.props.lowerBound is not None:
                reader = reader.option("lowerBound", self.props.lowerBound)
            if self.props.upperBound is not None:
                reader = reader.option("upperBound", self.props.upperBound)
            if self.props.numPartitions is not None:
                reader = reader.option("numPartitions", self.props.numPartitions)
            if self.props.queryTimeout is not None:
                reader = reader.option("queryTimeout", self.props.queryTimeout)
            if self.props.fetchsize is not None:
                reader = reader.option("fetchsize", self.props.fetchsize)
            if self.props.sessionInitStatement is not None:
                reader = reader.option("sessionInitStatement", self.props.sessionInitStatement)
            if self.props.customSchema is not None:
                reader = reader.option("customSchema", self.props.customSchema)
            if self.props.pushDownPredicate is not None:
                reader = reader.option("pushDownPredicate", self.props.pushDownPredicate)
            if self.props.pushDownAggregate is not None:
                reader = reader.option("pushDownAggregate", self.props.pushDownAggregate)

            return reader.load()

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            pass
