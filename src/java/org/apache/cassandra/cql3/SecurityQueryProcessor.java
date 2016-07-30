package org.apache.cassandra.cql3;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.PermissionDetails;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.Restriction;
import org.apache.cassandra.cql3.statements.SchemaAlteringStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.Selection;
import org.apache.cassandra.cql3.statements.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Prepared;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

// filter by tenant id..
// authorization?
// how to dynamically change authorization
// all modification to the tenant id table will be intercepted here..
// need cache? how to update the data realtime
public class SecurityQueryProcessor implements QueryHandler
{
    private static final Logger logger = LoggerFactory.getLogger(SecurityQueryProcessor.class);

    private static final QueryProcessor internal = QueryProcessor.instance;

    // setup the table for authorization
    public static final String TENANT_ID_COLUMN = "tenant_id";

    private static SelectStatement allResourceStatement;
    private static final String PERMISSIONS_CF = "permissions"; // copied from CassandraAuthorizer
    private static final String USERNAME = "username"; // pk name
    private static final String RESOURCE = "resource"; // ck name
    private static final String PERMISSIONS = "permissions"; // column name

    private static Field keyRestrictionsField;// in SelectStatement.class

    public SecurityQueryProcessor()
    {
        verifySecurityConfig();
        try
        {
            keyRestrictionsField = SelectStatement.class.getDeclaredField("keyRestrictions");
        }
        catch (NoSuchFieldException | SecurityException e)
        {
            logger.error("Unable to initialize due to " + e.getMessage(), e);
            throw new IllegalStateException(e);
        }
        keyRestrictionsField.setAccessible(true);
    }

    private static SelectStatement getAllResourcesStatement()
    {
        if (allResourceStatement != null)
            return allResourceStatement;
        try
        {
            String query = String.format("SELECT * FROM %s.%s WHERE username = ?", Auth.AUTH_KS, PERMISSIONS_CF);
            allResourceStatement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;
            return allResourceStatement;
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    private static void verifySecurityConfig()
    {
        if (DatabaseDescriptor.getAuthenticator() instanceof AllowAllAuthenticator)
            throw new IllegalArgumentException("Security Query Processor only work with PasswordAuthenticator");
        if (DatabaseDescriptor.getAuthorizer() instanceof AllowAllAuthorizer)
            throw new IllegalArgumentException("Security Query Processor only work with CassandraAuthorizer");
    }
    
    public static class TenantResource implements IResource
    {
        private final String tenantIdAsString;

        private static final String ALL_TENANT_ID = "";

        enum Level
        {
            ALL, SINGLE
        }

        public static TenantResource ALL_TENANT = new TenantResource();

        private final Level level;

        public TenantResource()
        {
            this(ALL_TENANT_ID);
        }

        public TenantResource(String tenantIdAsString)
        {
            if (ALL_TENANT_ID.equals(tenantIdAsString))
                this.level = Level.ALL;
            else
                this.level = Level.SINGLE;
            this.tenantIdAsString = tenantIdAsString;
        }

        @Override
        public String getName()
        {
            return tenantIdAsString;
        }

        @Override
        public IResource getParent()
        {
            return level == Level.ALL ? null : ALL_TENANT;
        }

        @Override
        public boolean hasParent()
        {
            return level != Level.ALL;
        }

        @Override
        public boolean exists()
        {
            return true;
        }

        @Override
        public String toString()
        {
            switch (level)
            {
            case ALL:
                return "<all tenant ids>";
            case SINGLE:
                return String.format("<tenant id %s>", tenantIdAsString);
            }
            throw new AssertionError();
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((level == null) ? 0 : level.hashCode());
            result = prime * result + ((tenantIdAsString == null) ? 0 : tenantIdAsString.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TenantResource other = (TenantResource) obj;
            if (level != other.level)
                return false;
            if (tenantIdAsString == null)
            {
                if (other.tenantIdAsString != null)
                    return false;
            }
            else if (!tenantIdAsString.equals(other.tenantIdAsString))
                return false;
            return true;
        }
    }

    @Override
    public ResultMessage process(String query, QueryState state, QueryOptions options)
            throws RequestExecutionException, RequestValidationException
    {
        ParsedStatement.Prepared p = QueryProcessor.getStatement(query, state.getClientState());
        options.prepare(p.boundNames);
        CQLStatement prepared = p.statement;
        if (prepared.getBoundTerms() != options.getValues().size())
            throw new InvalidRequestException("Invalid amount of bind variables");

        if (!state.getClientState().isInternal)
            QueryProcessor.metrics.regularStatementsExecuted.inc();

        return processStatement(prepared, state, options);
    }

    private ResultMessage processStatement(CQLStatement statement, QueryState queryState, QueryOptions options)
            throws RequestExecutionException, RequestValidationException
    {
        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = queryState.getClientState();
        statement.checkAccess(clientState);
        statement.validate(clientState);

        // check tenant id
        boolean filtering = checkAccess(statement, queryState, options);

        ResultMessage result = statement.execute(queryState, options);
        result = filtering
                ? filterResultByAuthorizedTenantId(queryState.getClientState().getUser(), statement, result) : result;
        return result == null ? new ResultMessage.Void() : result;
    }

    private static final List<String> SYS_KS = Arrays.asList("system_auth", "system", "system_traces");

    // return if filtering is needed
    private static boolean checkAccess(CQLStatement prepared, QueryState state, QueryOptions options)
            throws UnauthorizedException, InvalidRequestException
    {
        if (state.getClientState().getUser() == null)
            return false;// for test cases.. we need to ensure login

        AuthenticatedUser user = state.getClientState().getUser();
        if (user.isSuper())
            return false; // don't check super user

        if (prepared instanceof ModificationStatement)
        {
            ModificationStatement modify = (ModificationStatement) prepared;
            failOnSystemKeyspace(user.getName(), "Modify", modify.keyspace());
            if (!hasTenantIdCF(modify))
                return false;// no tenant_id as first column
            String tenantId = extractTenantId(modify, options);
            if (tenantId == null) // wrong modify statement, cassandra will handle
                return false;
            checkTenantIdAccess(state, Permission.MODIFY, new TenantResource(tenantId));
        }
        else if (prepared instanceof TruncateStatement)
        {
            TruncateStatement truncate = (TruncateStatement) prepared;
            fail(user.getName(), "Truncate", truncate.keyspace());
        }
        else if (prepared instanceof BatchStatement)
        {
            BatchStatement batch = (BatchStatement) prepared;
            String query = batch.type.toString();
            for (ModificationStatement modify : batch.getStatements())
                checkAccess(modify, state, options);
        }
        else if (prepared instanceof SchemaAlteringStatement)
        {
            SchemaAlteringStatement alter = (SchemaAlteringStatement) prepared;
            // Drop/Alter on normal schema should be block by C* native feature
            failOnSystemKeyspace(user.getName(), "Alter", alter.keyspace());
        }
        else if (prepared instanceof SelectStatement)
        {
            SelectStatement statement = (SelectStatement) prepared;
            failOnSystemAuthKeyspace(user.getName(), "SELECT", statement.keyspace());

            if (!hasTenantIdCF(statement))
                return false;// no tenant_id as first column
            String tenantId = extractTenantId(statement, options);
            if (tenantId == null)
            {
                if (statement.parameters.isCount)
                    return false;// count all
                return true;
            }
            checkTenantIdAccess(state, Permission.SELECT, new TenantResource(tenantId));
        }
        // rest, don't care
        return false;
    }

    // protect password
    private static void failOnSystemAuthKeyspace(String userId, String ops, String keyspace)
            throws UnauthorizedException
    {
        if (Auth.AUTH_KS.equals(keyspace))
            throw new UnauthorizedException(String.format(
                    "User %s has no %s permission on %s or any of its parents",
                    userId, ops, keyspace));
    }

    private static void failOnSystemKeyspace(String userId, String ops, String keyspace) throws UnauthorizedException
    {
        if (SYS_KS.contains(keyspace))
            fail(userId, ops, keyspace);
    }

    private static void fail(String userId, String ops, String keyspace) throws UnauthorizedException
    {
        throw new UnauthorizedException(String.format(
                "User %s has no %s permission on %s or any of its parents",
                userId, ops, keyspace));
    }

    private static void checkTenantIdAccess(QueryState state, Permission permission, IResource resource)
            throws UnauthorizedException
    {
        state.getClientState().ensureHasPermission(permission, resource);
    }

    private static String extractTenantId(ModificationStatement modify, QueryOptions options)
            throws InvalidRequestException
    {
        try
        {
            List<ByteBuffer> pks = modify.buildPartitionKeyNames(options);
            if (pks == null || pks.isEmpty())
                return null;
            return extractFirstPK(modify.cfm.partitionKeyColumns().get(0).type, pks.get(0));
        }
        catch (RuntimeException e)
        {
            // check error
            String format = "Unable to extra TId from modify statement %s.%s %s - %s";
            logger.error(String.format(format, modify.keyspace(), modify.columnFamily(), modify.type.toString(),
                    modify.attrs.toString()), e);
            return null;
        }
    }

    private static String extractFirstPK(AbstractType<?> type, ByteBuffer pk)
    {
        ByteBuffer bytes = ByteBufferUtil.readBytesWithShortLength(pk.duplicate());
        return type.getString(bytes);
    }

    private static boolean hasTenantIdCF(ModificationStatement modify)
    {
        if (modify.cfm.partitionKeyColumns().get(0).name.toString().equals(TENANT_ID_COLUMN))
            return true;
        return false;
    }

    private static boolean hasTenantIdCF(SelectStatement select)
    {
        if (select.cfm.partitionKeyColumns().get(0).name.toString().equals(TENANT_ID_COLUMN))
            return true;
        return false;
    }

    private static ResultMessage filterResultByAuthorizedTenantId(AuthenticatedUser user, CQLStatement statement,
            ResultMessage resultMessage) throws InvalidRequestException
    {
        if(resultMessage instanceof Rows){
            SelectStatement select = (SelectStatement) statement;

            Rows resultRows = (Rows) resultMessage;
            ResultSet resultSet = resultRows.result;
            Set<String> pkResources = getAllReadAuthorizedPkResources(user);
            if (isAllResource(pkResources))
                return resultMessage;
            List<List<ByteBuffer>> filteredRows = getFilteredRows(resultSet.rows, select.getSelection(), pkResources);
            ResultSet filtered = new ResultSet(resultSet.metadata, filteredRows);
            return new Rows(filtered);
        }
        return resultMessage;
    }

    // check is it's in resources
    private static List<List<ByteBuffer>> getFilteredRows(List<List<ByteBuffer>> rows, Selection selection,
            Set<String> pkResources) throws InvalidRequestException
    { 
        int tenantIdIndex = getTenantIdSelectionIndex(selection); 
        AbstractType<?> type = selection.getResultMetadata().names.get(tenantIdIndex).type;
        List<List<ByteBuffer>> filtered = new ArrayList<>();
        for(List<ByteBuffer> row:rows){
            ByteBuffer tenantIdKey = row.get(tenantIdIndex);
            if (validateRow(tenantIdKey, type, pkResources))
                filtered.add(row);
        }
        return filtered;
    }

    private static boolean validateRow(ByteBuffer tenantIdKey, AbstractType<?> type, Set<String> resources)
    {
        String pk = type.getString(tenantIdKey);
        return resources.contains(pk);
    }

    private static int getTenantIdSelectionIndex(Selection selection) throws InvalidRequestException
    {
        int pkSelectionIndex = -1;
        List<ColumnSpecification> names = selection.getResultMetadata().names;
        for (int i = 0; i < names.size(); i++)
            if (names.get(i).name.toString().equals(TENANT_ID_COLUMN))
                pkSelectionIndex = i;
        if (pkSelectionIndex < 0)// not selected
            throw new InvalidRequestException(TENANT_ID_COLUMN + " must be selected");
        return pkSelectionIndex;
    }

    private static boolean isAllResource(Set<String> resources)
    {
        return resources.contains(TenantResource.ALL_TENANT.tenantIdAsString);
    }

    private static Set<String> getAllReadAuthorizedPkResources(AuthenticatedUser user)
    {
        Set<String> pkResources = new HashSet<>();

        if (user.isSuper())
        {
            pkResources.add(TenantResource.ALL_TENANT.tenantIdAsString);
            return pkResources;
        }

        UntypedResultSet result;
        try
        {
            ResultMessage.Rows rows = getAllResourcesStatement().execute(QueryState.forInternalCalls(),
                    QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                            Lists.newArrayList(ByteBufferUtil.bytes(user.getName()))));
            result = UntypedResultSet.create(rows.result);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            logger.warn("SecurityQueryProcessor failed to load all authorized PKs for {}", user);
            throw new RuntimeException(e);
        }
        if (result == null || result.isEmpty())
            return pkResources;
        Iterator<Row> rowItr = result.iterator();
        while (rowItr.hasNext())
        {
            Row row = rowItr.next();
            String tenantId = getPkResource(row);
            if (tenantId != null && containsSelect(row))
                pkResources.add(tenantId);
        }
        return pkResources;
    }

    private static String getPkResource(Row row)
    {
        String resource = row.getString(RESOURCE);
        if (resource.startsWith(DataResource.root().getName()))
            return null;// data resource
        return resource;
    }

    private static boolean containsSelect(Row row)
    {
        Set<String> permissions = row.getSet(PERMISSIONS, UTF8Type.instance);
        if (permissions.contains(Permission.SELECT.toString()))
            return true;
        return false;
    }


    private static String extractTenantId(SelectStatement select, QueryOptions options) throws InvalidRequestException
    {
        if (!select.hasPartitionKeyRestriction())
            return null;
        Restriction[] keyRestrictions;
        try
        {
            keyRestrictions = (Restriction[]) keyRestrictionsField.get(select);
        }
        catch (IllegalArgumentException | IllegalAccessException e)
        {
            logger.error("Unable to extract pk due to " + e.getMessage(), e);
            return null;
        }
        if (!(keyRestrictions[0] instanceof SingleColumnRestriction.EQ))
            return null; // cannot get PK..
        try
        {
            List<ByteBuffer> pks = keyRestrictions[0].values(options);
            if (pks == null || pks.isEmpty())
                return null;
            return select.cfm.partitionKeyColumns().get(0).type.getString(pks.get(0));
        }
        catch (RuntimeException e)
        {
            // check error
            String format = "Unable to extra TId from Select statement %s.%s %s";
            logger.error(String.format(format, select.keyspace(), select.columnFamily(), select.parameters.toString()),
                    e);
            return null;
        }
    }

    // for test cases
    public static UntypedResultSet executeOnceInternal(String query, Object... values)
    {
        try
        {
            ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(query, internalQueryState());
            boolean filter = checkAccess(prepared.statement, internalQueryState(),
                    makeInternalOptions(prepared, values));
            prepared.statement.validate(internalQueryState().getClientState());
            ResultMessage result = prepared.statement.executeInternal(internalQueryState(),
                    makeInternalOptions(prepared, values));
            // TODO filter for testing
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows) result).result);
            else
                return null;
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException("Error validating query " + query, e);
        }
    }

    @Override
    public Prepared prepare(String query, QueryState state) throws RequestValidationException
    {
        // TODO maybe it's okay to ignore prepared query containing key. it won't be executed
        return internal.prepare(query, state);
    }

    @Override
    public ParsedStatement.Prepared getPrepared(MD5Digest id)
    {
        // No need to intercept
        return internal.getPrepared(id);
    }

    @Override
    public ParsedStatement.Prepared getPreparedForThrift(Integer id)
    {
        // No need to intercept
        return internal.getPreparedForThrift(id);
    }

    @Override
    public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options)
            throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.getBoundTerms() == 0)))
        {
            if (variables.size() != statement.getBoundTerms())
                throw new InvalidRequestException(
                        String.format("there were %d markers(?) in CQL but %d bound variables",
                                statement.getBoundTerms(),
                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i + 1, variables.get(i));
        }

        QueryProcessor.metrics.preparedStatementsExecuted.inc();
        return processStatement(statement, state, options);
    }

    @Override
    public ResultMessage processBatch(BatchStatement batch, QueryState state, BatchQueryOptions options)
            throws RequestExecutionException, RequestValidationException
    {
        ClientState clientState = state.getClientState();
        batch.checkAccess(clientState);
        for (int i = 0; i < batch.getStatements().size(); i++)
        {
            checkAccess(batch.getStatements().get(i), state, options.forStatement(i));
        }
        batch.validate();
        batch.validate(clientState);
        return batch.execute(state, options);
    }

    private static QueryState internalQueryState()
    {
        return InternalStateInstance.INSTANCE.queryState;
    }

    private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values)
    {
        if (prepared.boundNames.size() != values.length)
            throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d",
                    prepared.boundNames.size(), values.length));

        List<ByteBuffer> boundValues = new ArrayList<ByteBuffer>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            AbstractType type = prepared.boundNames.get(i).type;
            boundValues.add(value instanceof ByteBuffer || value == null ? (ByteBuffer) value : type.decompose(value));
        }
        return QueryOptions.forInternalCalls(boundValues);
    }

    // Work around initialization dependency
    private static enum InternalStateInstance
    {
        INSTANCE;

        private final QueryState queryState;

        InternalStateInstance()
        {
            ClientState state = ClientState.forInternalCalls();
            try
            {
                state.setKeyspace(Keyspace.SYSTEM_KS);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException();
            }
            this.queryState = new QueryState(state);
        }
    }

    public static class SecurityCassandraAuthorizer extends CassandraAuthorizer
    {

        private static final String USERNAME = "username";
        private static final String RESOURCE = "resource";
        private static final String PERMISSIONS = "permissions";
        private static final String PERMISSIONS_CF = "permissions";

        private static final String PREFIX_DATA = DataResource.root().getName();

        public Set<PermissionDetails> list(AuthenticatedUser performer, Set<Permission> permissions,
                IResource resource, String of)
                throws RequestValidationException, RequestExecutionException
        {
            if (!performer.isSuper() && !performer.getName().equals(of))
                throw new UnauthorizedException(String.format("You are not authorized to view %s's permissions",
                        of == null ? "everyone" : of));

            Set<PermissionDetails> details = new HashSet<PermissionDetails>();

            for (UntypedResultSet.Row row : process(buildListQuery(resource, of)))
            {
                if (row.has(PERMISSIONS))
                {
                    for (String p : row.getSet(PERMISSIONS, UTF8Type.instance))
                    {
                        Permission permission = Permission.valueOf(p);
                        if (permissions.contains(permission))
                        {
                            String resourceStr = row.getString(RESOURCE);
                            IResource iResource = resourceStr.startsWith(PREFIX_DATA) ? DataResource
                                    .fromName(resourceStr)
                                    : new TenantResource(resourceStr);
                            details.add(new PermissionDetails(row.getString(USERNAME),
                                    iResource,
                                    permission));
                        }
                    }
                }
            }

            return details;
        }

        private static UntypedResultSet process(String query) throws RequestExecutionException
        {
            return QueryProcessor.process(query, ConsistencyLevel.ONE);
        }

        private static String buildListQuery(IResource resource, String of)
        {
            List<String> vars = Lists.newArrayList(Auth.AUTH_KS, PERMISSIONS_CF);
            List<String> conditions = new ArrayList<String>();

            if (resource != null)
            {
                conditions.add("resource = '%s'");
                vars.add(escape(resource.getName()));
            }

            if (of != null)
            {
                conditions.add("username = '%s'");
                vars.add(escape(of));
            }

            String query = "SELECT username, resource, permissions FROM %s.%s";

            if (!conditions.isEmpty())
                query += " WHERE " + StringUtils.join(conditions, " AND ");

            if (resource != null && of == null)
                query += " ALLOW FILTERING";

            return String.format(query, vars.toArray());
        }

        private static String escape(String name)
        {
            return StringUtils.replace(name, "'", "''");
        }
    }
}
