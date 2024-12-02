package org.apache.doris.spark.client.read;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.sdk.thrift.TPrimitiveType;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.client.entity.DorisReaderPartition;
import org.apache.doris.spark.client.entity.Frontend;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.exception.ShouldNeverHappenException;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DorisFlightSqlReader extends DorisReader {

    private static final Logger log = LoggerFactory.getLogger(DorisFlightSqlReader.class);
    private final AtomicBoolean endOfStream = new AtomicBoolean(false);
    private final DorisFrontendClient frontendClient;
    private final Schema schema;
    private AdbcConnection connection;
    private final ArrowReader arrowReader;

    public DorisFlightSqlReader(DorisReaderPartition partition) throws Exception {
        super(partition);
        this.frontendClient = new DorisFrontendClient(partition.getConfig());
        List<Frontend> frontends = frontendClient.getFrontends();
        for (Frontend frontend : frontends) {
            try {
                this.connection = initializeConnection(frontend, partition.getConfig());
                break;
            } catch (OptionRequiredException | AdbcException e) {
                throw new DorisException("init adbc connection failed", e);
            }
        }
        String tableIdentifier = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER);
        String[] arr = tableIdentifier.split("\\.");

        Schema tableSchema = frontendClient.getTableSchema(arr[0], arr[1]);
        this.schema = processDorisSchema(partition, tableSchema);
        log.debug("origin flight sql read Schema: " + tableSchema + ", processed schema: " + schema);
        this.arrowReader = executeQuery();
    }

    @Override
    public boolean hasNext() throws DorisException {
        if (!endOfStream.get() && (rowBatch == null || !rowBatch.hasNext())) {
            try {
                endOfStream.set(!arrowReader.loadNextBatch());
            } catch (IOException e) {
                throw new DorisException(e);
            }
            if (!endOfStream.get()) {
                rowBatch = new RowBatch(arrowReader, schema);
            }
        }
        return !endOfStream.get();
    }

    @Override
    public Object next() throws DorisException {
        if (!hasNext()) {
            throw new ShouldNeverHappenException();
        }
        return rowBatch.next().toArray();
    }

    @Override
    public void close() {
        if (rowBatch != null) {
            rowBatch.close();
        }
        if (arrowReader != null) {
            try {
                arrowReader.close();
            } catch (IOException e) {
                log.warn("close arrow reader error", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.warn("close adbc connection error", e);
            }
        }
    }

    private AdbcConnection initializeConnection(Frontend frontend, DorisConfig config) throws OptionRequiredException, AdbcException {
        BufferAllocator allocator = new RootAllocator();
        FlightSqlDriver driver = new FlightSqlDriver(allocator);
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, Location.forGrpcInsecure(frontend.getHost(), frontend.getFlightSqlPort()).getUri().toString());
        AdbcDriver.PARAM_USERNAME.set(params, config.getValue(DorisOptions.DORIS_USER));
        AdbcDriver.PARAM_PASSWORD.set(params, config.getValue(DorisOptions.DORIS_PASSWORD));
        AdbcDatabase database = driver.open(params);
        return database.connect();
    }

    private ArrowReader executeQuery() throws AdbcException, OptionRequiredException {
        AdbcStatement statement = connection.createStatement();
        String flightSql = generateQuerySql(partition);
        statement.setSqlQuery(flightSql);
        AdbcStatement.QueryResult queryResult = statement.executeQuery();
        return queryResult.getReader();
    }

    protected String generateQuerySql(DorisReaderPartition partition) throws OptionRequiredException {
        String columns = String.join(",", partition.getReadColumns());
        String fullTableName = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER);
        String tablets = String.format("TABLET(%s)", StringUtils.join(partition.getTablets(), ","));
        String predicates = partition.getFilters().length == 0 ? "" : " WHERE " + String.join(" AND ", partition.getFilters());
        return String.format("SELECT %s FROM %s %s%s", columns, fullTableName, tablets, predicates);
    }

    protected Schema processDorisSchema(DorisReaderPartition partition, final Schema originSchema) throws Exception {
        Schema processedSchema = new Schema();
        Schema tableSchema = frontendClient.getTableSchema(partition.getDatabase(), partition.getTable());
        Map<String, Field> fieldTypeMap = tableSchema.getProperties().stream()
                .collect(Collectors.toMap(Field::getName, Function.identity()));
        String[] readColumns = partition.getReadColumns();
        List<Field> newFieldList = new ArrayList<>();
        int offset = 0;
        for (int i = 0; i < readColumns.length; i++) {
            String readColumn = readColumns[i];
            if (!fieldTypeMap.containsKey(readColumn) && readColumn.contains(" AS ")) {
                int asIdx = readColumn.indexOf(" AS ");
                String realColumn = readColumn.substring(asIdx + 4).trim().replaceAll("`", "");
                if (fieldTypeMap.containsKey(realColumn)
                        && ("BITMAP".equalsIgnoreCase(fieldTypeMap.get(realColumn).getType())
                        || "HLL".equalsIgnoreCase(fieldTypeMap.get(realColumn).getType()))) {
                    newFieldList.add(new Field(realColumn, TPrimitiveType.VARCHAR.name(), null, 0, 0, null));
                    offset++;
                }
            } else {
                newFieldList.add(originSchema.getProperties().get(i + offset));
            }
        }
        processedSchema.setProperties(newFieldList);
        return processedSchema;
    }

}