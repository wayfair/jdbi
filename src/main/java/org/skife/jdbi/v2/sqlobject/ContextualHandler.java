/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.skife.jdbi.v2.sqlobject;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.cglib.proxy.MethodProxy;

import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.Call;
import org.skife.jdbi.v2.ConcreteStatementContext;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultColumnMapperFactory;
import org.skife.jdbi.v2.ResultSetMapperFactory;
import org.skife.jdbi.v2.Script;
import org.skife.jdbi.v2.TimingCollector;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionConsumer;
import org.skife.jdbi.v2.TransactionIsolationLevel;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.exceptions.TransactionFailedException;
import org.skife.jdbi.v2.tweak.ArgumentFactory;
import org.skife.jdbi.v2.tweak.ContainerFactory;
import org.skife.jdbi.v2.tweak.ResultColumnMapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.tweak.SQLLog;
import org.skife.jdbi.v2.tweak.StatementBuilder;
import org.skife.jdbi.v2.tweak.StatementLocator;
import org.skife.jdbi.v2.tweak.StatementRewriter;

class ContextualHandler implements Handler {
    private final Class<?> sqlObjectType;
    private final Method sqlObjectMethod;
    private final Handler delegate;

    ContextualHandler(Class<?> sqlObjectType, Method sqlObjectMethod, Handler delegate) {
        this.sqlObjectType = sqlObjectType;
        this.sqlObjectMethod = sqlObjectMethod;
        this.delegate = delegate;
    }

    @Override
    public Object invoke(HandleDing h, Object target, Object[] args, MethodProxy mp) {
        return delegate.invoke(new ContextualDing(h), target, args, mp);
    }

    private class ContextualDing implements HandleDing {
        private Map<Handle, Handle> handleWrappers = new WeakHashMap<Handle, Handle>();
        private final HandleDing delegate;

        ContextualDing(HandleDing delegate) {
            this.delegate = delegate;
        }

        @Override
        public Handle getHandle() {
            Handle handle = delegate.getHandle();
            Handle wrapper = handleWrappers.get(handle);
            if (wrapper == null) {
                handleWrappers.put(handle, wrapper = new ContextualHandle(handle));
            }
            return wrapper;
        }

        @Override
        public void release(String name) {
            delegate.release(name);
        }

        @Override
        public void retain(String name) {
            delegate.retain(name);
        }
    }

    private class ContextualHandle implements Handle {
        private final Handle delegate;

        ContextualHandle(Handle delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection getConnection() {
            return delegate.getConnection();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public void define(String key, Object value) {
            delegate.define(key, value);
        }

        @Override
        public Handle begin() {
            delegate.begin();
            return this;
        }

        @Override
        public Handle commit() {
            delegate.commit();
            return this;
        }

        @Override
        public Handle rollback() {
            delegate.rollback();
            return this;
        }

        @Override
        public Handle rollback(String checkpointName) {
            delegate.rollback(checkpointName);
            return this;
        }

        @Override
        public boolean isInTransaction() {
            return delegate.isInTransaction();
        }

        @Override
        public Query<Map<String, Object>> createQuery(String sql) {
            Query<Map<String, Object>> query = delegate.createQuery(sql);
            populateStatementContext((ConcreteStatementContext) query.getContext());
            return query;
        }

        private void populateStatementContext(ConcreteStatementContext context) {
            context.setSqlObjectType(sqlObjectType);
            context.setSqlObjectMethod(sqlObjectMethod);
        }

        @Override
        public Update createStatement(String sql) {
            Update update = delegate.createStatement(sql);
            populateStatementContext((ConcreteStatementContext) update.getContext());
            return update;
        }

        @Override
        public Call createCall(String callableSql) {
            Call call = delegate.createCall(callableSql);
            populateStatementContext((ConcreteStatementContext) call.getContext());
            return call;
        }

        @Override
        public int insert(String sql, Object... args) {
            return update(sql, args);
        }

        @Override
        public int update(String sql, Object... args) {
            Update stmt = createStatement(sql);
            for (int i = 0; i < args.length; i++) {
                stmt.bind(i, args[i]);
            }
            return stmt.execute();
        }

        @Override
        public PreparedBatch prepareBatch(String sql) {
            PreparedBatch batch = delegate.prepareBatch(sql);
            populateStatementContext((ConcreteStatementContext) batch.getContext());
            return batch;
        }

        @Override
        public Batch createBatch() {
            Batch batch = delegate.createBatch();
            populateStatementContext((ConcreteStatementContext) batch.getContext());
            return batch;
        }

        @Override
        public <ReturnType> ReturnType inTransaction(final TransactionCallback<ReturnType> callback) throws TransactionFailedException {
            return delegate.inTransaction(new TransactionCallback<ReturnType>() {
                @Override
                public ReturnType inTransaction(Handle conn, TransactionStatus status) throws Exception {
                    return callback.inTransaction(ContextualHandle.this, status);
                }
            });
        }

        @Override
        public void useTransaction(final TransactionConsumer callback) throws TransactionFailedException {
            delegate.useTransaction(new TransactionConsumer() {
                @Override
                public void useTransaction(Handle conn, TransactionStatus status) throws Exception {
                    callback.useTransaction(ContextualHandle.this, status);
                }
            });
        }

        @Override
        public <ReturnType> ReturnType inTransaction(TransactionIsolationLevel level, final TransactionCallback<ReturnType> callback) throws TransactionFailedException {
            return delegate.inTransaction(level, new TransactionCallback<ReturnType>() {
                @Override
                public ReturnType inTransaction(Handle conn, TransactionStatus status) throws Exception {
                    return callback.inTransaction(ContextualHandle.this, status);
                }
            });
        }

        @Override
        public void useTransaction(TransactionIsolationLevel level, final TransactionConsumer callback) throws TransactionFailedException {
            delegate.useTransaction(level, new TransactionConsumer() {
                @Override
                public void useTransaction(Handle conn, TransactionStatus status) throws Exception {
                    callback.useTransaction(ContextualHandle.this, status);
                }
            });
        }

        @Override
        public List<Map<String, Object>> select(String sql, Object... args) {
            Query<Map<String, Object>> query = this.createQuery(sql);
            int position = 0;
            for (Object arg : args) {
                query.bind(position++, arg);
            }
            return query.list();
        }

        @Override
        public void setStatementLocator(StatementLocator locator) {
            delegate.setStatementLocator(locator);
        }

        @Override
        public void setStatementRewriter(StatementRewriter rewriter) {
            delegate.setStatementRewriter(rewriter);
        }

        @Override
        public Script createScript(String name) {
            Script script = delegate.createScript(name);
            populateStatementContext((ConcreteStatementContext) script.getContext());
            return script;
        }

        @Override
        public void execute(String sql, Object... args) {
            this.update(sql, args);
        }

        @Override
        public Handle checkpoint(String name) {
            delegate.checkpoint(name);
            return this;
        }

        @Override
        public Handle release(String checkpointName) {
            delegate.release(checkpointName);
            return this;
        }

        @Override
        public void setStatementBuilder(StatementBuilder builder) {
            delegate.setStatementBuilder(builder);
        }

        @Override
        public void setSQLLog(SQLLog log) {
            delegate.setSQLLog(log);
        }

        @Override
        public void setTimingCollector(TimingCollector timingCollector) {
            delegate.setTimingCollector(timingCollector);
        }

        @Override
        public void registerMapper(ResultSetMapper mapper) {
            delegate.registerMapper(mapper);
        }

        @Override
        public void registerMapper(ResultSetMapperFactory factory) {
            delegate.registerMapper(factory);
        }

        @Override
        public void registerColumnMapper(ResultColumnMapper mapper) {
            delegate.registerColumnMapper(mapper);
        }

        @Override
        public void registerColumnMapper(ResultColumnMapperFactory factory) {
            delegate.registerColumnMapper(factory);
        }

        @Override
        public <SqlObjectType> SqlObjectType attach(Class<SqlObjectType> sqlObjectType) {
            return delegate.attach(sqlObjectType);
        }

        @Override
        public void setTransactionIsolation(TransactionIsolationLevel level) {
            delegate.setTransactionIsolation(level);
        }

        @Override
        public void setTransactionIsolation(int level) {
            delegate.setTransactionIsolation(level);
        }

        @Override
        public TransactionIsolationLevel getTransactionIsolationLevel() {
            return delegate.getTransactionIsolationLevel();
        }

        @Override
        public void registerArgumentFactory(ArgumentFactory argumentFactory) {
            delegate.registerArgumentFactory(argumentFactory);
        }

        @Override
        public void registerContainerFactory(ContainerFactory<?> factory) {
            delegate.registerContainerFactory(factory);
        }
    }
}
