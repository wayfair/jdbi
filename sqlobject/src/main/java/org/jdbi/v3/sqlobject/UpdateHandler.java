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
package org.jdbi.v3.sqlobject;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.function.Supplier;

import org.jdbi.v3.core.GeneratedKeys;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Update;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.util.GenericTypes;
import org.jdbi.v3.sqlobject.exceptions.UnableToCreateSqlObjectException;

class UpdateHandler extends CustomizingStatementHandler
{
    private final Class<?> sqlObjectType;
    private final Returner returner;

    UpdateHandler(Class<?> sqlObjectType, Method method)
    {
        super(sqlObjectType, method);
        this.sqlObjectType = sqlObjectType;

        boolean isGetGeneratedKeys = method.isAnnotationPresent(GetGeneratedKeys.class);

        Type returnType = GenericTypes.resolveType(method.getGenericReturnType(), sqlObjectType);
        if (!isGetGeneratedKeys && returnTypeIsInvalid(method.getReturnType()) ) {
            throw new UnableToCreateSqlObjectException(invalidReturnTypeMessage(method, returnType));
        }
        if (isGetGeneratedKeys) {
            final ResultReturner magic = ResultReturner.forMethod(sqlObjectType, method);
            final GetGeneratedKeys ggk = method.getAnnotation(GetGeneratedKeys.class);
            final RowMapper<?> mapper = ResultReturner.rowMapperFor(ggk, returnType);

            this.returner = (update, handle) -> {
                GeneratedKeys<?> o = update.executeAndReturnGeneratedKeys(mapper, ggk.columnName());
                return magic.result(o, handle);
            };
        }
        else {
            this.returner = (update, handle) -> update.execute();
        }
    }

    @Override
    public Object invoke(Supplier<Handle> handle, SqlObjectConfig config, Object target, Object[] args, Method method)
    {
        String sql = config.getSqlLocator().locate(sqlObjectType, method);
        Update update = handle.get().createStatement(sql);
        populateSqlObjectData(update.getContext());
        applyCustomizers(update, args);
        applyBinders(update, args);
        return this.returner.value(update, handle);
    }


    private interface Returner
    {
        Object value(Update update, Supplier<Handle> handle);
    }

    private boolean returnTypeIsInvalid(Class<?> type) {
        return !Number.class.isAssignableFrom(type) &&
                !type.equals(Integer.TYPE) &&
                !type.equals(Long.TYPE) &&
                !type.equals(Void.TYPE);
    }

    private String invalidReturnTypeMessage(Method method, Type returnType) {
        return method.getDeclaringClass().getSimpleName() + "." + method.getName() +
                " method is annotated with @SqlUpdate so should return void or Number but is returning: " +
                returnType;
    }
}
