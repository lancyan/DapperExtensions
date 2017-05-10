using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Text;
using Dapper;
using DapperExtensions.Mapper;
using DapperExtensions.Sql;
using System.Reflection;
using System.Linq.Expressions;

namespace DapperExtensions
{
    public interface IDapperImplementor
    {
        ISqlGenerator SqlGenerator { get; }
        T Get<T>(IDbConnection connection, dynamic id, IDbTransaction transaction, int? commandTimeout) where T : class;

        T Get<T>(IDbConnection connection, Expression<Func<T, bool>> exp, IDbTransaction transaction, int? commandTimeout) where T : class;

        void Insert<T>(IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction, int? commandTimeout) where T : class;

        dynamic Insert<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout) where T : class;

        bool Update<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout) where T : class;

        int Update<T>(IDbConnection connection, dynamic updateProperties, dynamic keyProperties, IDbTransaction transaction, int? commandTimeout) where T : class;

        int Update(IDbConnection connection, string sql, IDbTransaction trans, int? timeout);

        bool Delete<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout) where T : class;

        bool Delete<T>(IDbConnection connection, IPredicate predicate, IDbTransaction transaction, int? commandTimeout) where T : class;

        bool DeleteById<T>(IDbConnection connection, dynamic id, IDbTransaction transaction, int? commandTimeout) where T : class;

        IEnumerable<T> GetList<T>(IDbConnection connection, IPredicate predicate, IList<ISort> sort, IDbTransaction transaction, int? commandTimeout, bool buffered) where T : class;

        IEnumerable<T> GetPage<T>(IDbConnection connection, IPredicate predicate, IList<ISort> sort, int pageIndex, int pageSize, IDbTransaction transaction, int? commandTimeout, bool buffered) where T : class;

        IEnumerable<T> GetSet<T>(IDbConnection connection, IPredicate predicate, IList<ISort> sort, int firstResult, int maxResults, IDbTransaction transaction, int? commandTimeout, bool buffered) where T : class;

        IEnumerable<dynamic> Query(IDbConnection connection, string sql, IDbTransaction trans, CommandType? commandType, int? timeout, bool buffered);

        IEnumerable<dynamic> Query(IDbConnection connection, string sql, string orderBy, int pageIndex, int pageSize, IDbTransaction trans, CommandType? commandType, int? timeout, bool buffered);

        IEnumerable<T> Query<T>(IDbConnection connection, string sql, IDbTransaction trans, CommandType? commandType, int? timeout, bool buffered) where T : class;

        IEnumerable<T> Query<T>(IDbConnection connection, string sql, string orderBy, int pageIndex, int pageSize, IDbTransaction trans, CommandType? commandType, int? timeout, bool buffered) where T : class;

        IEnumerable<T> Where<T>(IDbConnection connection, Expression<Func<T, bool>> exp, string orderBy, IDbTransaction trans, int? timeout, bool buffered) where T : class;

        IEnumerable<T> Where<T>(IDbConnection connection, Expression<Func<T, bool>> exp, string orderBy, int pageIndex, int pageSize, IDbTransaction trans, int? timeout, bool buffered) where T : class;

        IEnumerable<T> Where<T>(IDbConnection connection, string where, string orderBy, IDbTransaction trans, int? timeout, bool buffered) where T : class;

        IEnumerable<T> Where<T>(IDbConnection connection, string where, string orderBy, int pageIndex, int pageSize, IDbTransaction trans, int? timeout, bool buffered) where T : class;

        int Count<T>(IDbConnection connection, IPredicate predicate, IDbTransaction transaction, int? commandTimeout) where T : class;

        int Count<T>(IDbConnection connection, Expression<Func<T, bool>> exp, IDbTransaction transaction, int? commandTimeout) where T : class;

        int Count<T>(IDbConnection connection, string sql, string where, IDbTransaction transaction, int? commandTimeout) where T : class;

        dynamic Execute<T>(IDbConnection connection, string pName, DynamicParameters paras, IDbTransaction trans, int? timeout, bool buffered);

        IMultipleResultReader GetMultiple(IDbConnection connection, GetMultiplePredicate predicate, IDbTransaction transaction, int? commandTimeout);
    }

    public class DapperImplementor : IDapperImplementor
    {
        public DapperImplementor(ISqlGenerator sqlGenerator)
        {
            SqlGenerator = sqlGenerator;
        }

        public ISqlGenerator SqlGenerator { get; private set; }

        public T Get<T>(IDbConnection connection, dynamic id, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            IPredicate predicate = GetIdPredicate(classMap, id);
            T result = GetList<T>(connection, classMap, predicate, null, transaction, commandTimeout, true).SingleOrDefault();
            return result;
        }

        public T Get<T>(IDbConnection connection, Expression<Func<T, bool>> exp, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            string str = SqlExpession.Where<T>(exp);
            string sql = string.IsNullOrEmpty(str) ? string.Format("select * from {0} ", typeof(T).Name) : string.Format("select * from {0} where {1}", typeof(T).Name, str);
            return connection.Query<T>(sql, null, transaction, false, commandTimeout, CommandType.Text).SingleOrDefault();
        }

        public void Insert<T>(IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            var properties = classMap.Properties.Where(p => p.KeyType != KeyType.NotAKey);

            foreach (var e in entities)
            {
                foreach (var column in properties)
                {
                    if (KeyType.Guid.Equals(column.KeyType))
                    {
                        Guid comb = SqlGenerator.Configuration.GetNextGuid();
                        column.PropertyInfo.SetValue(e, comb, null);
                    }
                }
            }

            string sql = SqlGenerator.Insert(classMap);

            connection.Execute(sql, entities, transaction, commandTimeout, CommandType.Text);
        }

        public dynamic Insert<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            List<IPropertyMap> nonIdentityKeyProperties = classMap.Properties.Where(p => p.KeyType == KeyType.Guid || p.KeyType == KeyType.Assigned).ToList();
            var identityColumn = classMap.Properties.SingleOrDefault(p => p.KeyType == KeyType.Identity);
            foreach (var column in nonIdentityKeyProperties)
            {
                if (KeyType.Guid.Equals(column.KeyType))
                {
                    Guid comb = SqlGenerator.Configuration.GetNextGuid();
                    column.PropertyInfo.SetValue(entity, comb, null);
                }
            }

            IDictionary<string, object> keyValues = new ExpandoObject();
            string sql = SqlGenerator.Insert(classMap);
            if (identityColumn != null)
            {
                int identityValue;
                if (SqlGenerator.SupportsMultipleStatements())
                {
                    sql += SqlGenerator.Configuration.Dialect.BatchSeperator + SqlGenerator.IdentitySql(classMap);
                    identityValue = connection.Query<int>(sql, entity, transaction, false, commandTimeout, CommandType.Text).Single();
                }
                else
                {
                    connection.Execute(sql, entity, transaction, commandTimeout, CommandType.Text);
                    sql = SqlGenerator.IdentitySql(classMap);
                    identityValue = connection.Query<int>(sql, entity, transaction, false, commandTimeout, CommandType.Text).Single();
                }
                keyValues.Add(identityColumn.Name, identityValue);
                identityColumn.PropertyInfo.SetValue(entity, identityValue, null);
            }
            else
            {
                connection.Execute(sql, entity, transaction, commandTimeout, CommandType.Text);
            }

            foreach (var column in nonIdentityKeyProperties)
            {
                keyValues.Add(column.Name, column.PropertyInfo.GetValue(entity, null));
            }

            if (keyValues.Count == 1)
            {
                return keyValues.First().Value;
            }

            return keyValues;
        }

        public bool Update<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            IPredicate predicate = GetKeyPredicate<T>(classMap, entity);
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            string sql = SqlGenerator.Update(classMap, predicate, parameters);
            DynamicParameters dynamicParameters = new DynamicParameters();

            var columns = classMap.Properties.Where(p => !(p.Ignored || p.IsReadOnly || p.KeyType == KeyType.Identity));
            foreach (var property in ReflectionHelper.GetObjectValues(entity).Where(property => columns.Any(c => c.Name == property.Key)))
            {
                dynamicParameters.Add(property.Key, property.Value);
            }

            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            return connection.Execute(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text) > 0;
        }

        public bool Delete<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            IPredicate predicate = GetKeyPredicate<T>(classMap, entity);
            return Delete<T>(connection, classMap, predicate, transaction, commandTimeout);
        }
        /// <summary>
        /// 删除 by id
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entity"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public bool DeleteById<T>(IDbConnection connection, object id, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            IPredicate predicate = GetIdPredicate(classMap, id);
            return Delete<T>(connection, classMap, predicate, transaction, commandTimeout);
        }

        public bool Delete<T>(IDbConnection connection, IPredicate predicate, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            IPredicate wherePredicate = GetPredicate(classMap, predicate);
            return Delete<T>(connection, classMap, wherePredicate, transaction, commandTimeout);
        }

        public IEnumerable<T> GetList<T>(IDbConnection connection, IPredicate predicate, IList<ISort> sort, IDbTransaction transaction, int? commandTimeout, bool buffered) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            IPredicate wherePredicate = GetPredicate(classMap, predicate);
            return GetList<T>(connection, classMap, wherePredicate, sort, transaction, commandTimeout, true);
        }

        public IEnumerable<T> GetPage<T>(IDbConnection connection, IPredicate predicate, IList<ISort> sort, int pageIndex, int pageSize, IDbTransaction transaction, int? commandTimeout, bool buffered) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            IPredicate wherePredicate = GetPredicate(classMap, predicate);
            return GetPage<T>(connection, classMap, wherePredicate, sort, pageIndex, pageSize, transaction, commandTimeout, buffered);
        }

        public IEnumerable<T> GetSet<T>(IDbConnection connection, IPredicate predicate, IList<ISort> sort, int firstResult, int maxResults, IDbTransaction transaction, int? commandTimeout, bool buffered) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            IPredicate wherePredicate = GetPredicate(classMap, predicate);
            return GetSet<T>(connection, classMap, wherePredicate, sort, firstResult, maxResults, transaction, commandTimeout, buffered);
        }

        public int Count<T>(IDbConnection connection, IPredicate predicate, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
            IPredicate wherePredicate = GetPredicate(classMap, predicate);
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            string sql = SqlGenerator.Count(classMap, wherePredicate, parameters);
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            return connection.Query<int>(sql, dynamicParameters, transaction, false, commandTimeout, CommandType.Text).Single();
        }

        public int Count<T>(IDbConnection connection, Expression<Func<T, bool>> exp, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            string str = SqlExpession.Where<T>(exp);
            string sql = string.IsNullOrEmpty(str) ? string.Format("select count(1) from {0} ", typeof(T).Name) : string.Format("select count(1) from {0} where {1}", typeof(T).Name, str);
            return connection.Query<int>(sql, null, transaction, false, commandTimeout, CommandType.Text).Single();
        }

        public int Count<T>(IDbConnection connection, string sql, string where, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            string tabName = typeof(T).Name;
            if (string.IsNullOrWhiteSpace(sql))
            {
                if (string.IsNullOrWhiteSpace(where))
                {
                    sql = string.Format("select count(1) from {0}", tabName);
                }
                else
                {
                    sql = string.Format("select count(1) from {0} where {1}", tabName, where);
                }
            }
            else
            {
                if (!string.IsNullOrWhiteSpace(where))
                {
                    sql = string.Format(" {0} where {1}", sql, where);
                }
                else
                {
                    sql = string.Format("select count(1) from {0} where {1}", tabName, sql);
                }
            }
            return connection.Query<int>(sql, null, transaction, false, commandTimeout, CommandType.Text).Single();
        }

        public IMultipleResultReader GetMultiple(IDbConnection connection, GetMultiplePredicate predicate, IDbTransaction transaction, int? commandTimeout)
        {
            if (SqlGenerator.SupportsMultipleStatements())
            {
                return GetMultipleByBatch(connection, predicate, transaction, commandTimeout);
            }

            return GetMultipleBySequence(connection, predicate, transaction, commandTimeout);
        }

        protected IEnumerable<T> GetList<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IList<ISort> sort, IDbTransaction transaction, int? commandTimeout, bool buffered) where T : class
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            string sql = SqlGenerator.Select(classMap, predicate, sort, parameters);
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            return connection.Query<T>(sql, dynamicParameters, transaction, buffered, commandTimeout, CommandType.Text);
        }

        protected IEnumerable<T> GetPage<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int pageIndex, int pageSize, IDbTransaction transaction, int? commandTimeout, bool buffered) where T : class
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            string sql = SqlGenerator.SelectPaged(classMap, predicate, sort, pageIndex, pageSize, parameters);
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            return connection.Query<T>(sql, dynamicParameters, transaction, buffered, commandTimeout, CommandType.Text);
        }

        protected IEnumerable<T> GetSet<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int firstResult, int maxResults, IDbTransaction transaction, int? commandTimeout, bool buffered) where T : class
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            string sql = SqlGenerator.SelectSet(classMap, predicate, sort, firstResult, maxResults, parameters);
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            return connection.Query<T>(sql, dynamicParameters, transaction, buffered, commandTimeout, CommandType.Text);
        }

        protected bool Delete<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            string sql = SqlGenerator.Delete(classMap, predicate, parameters);
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            return connection.Execute(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text) > 0;
        }

        protected IPredicate GetPredicate(IClassMapper classMap, object predicate)
        {
            IPredicate wherePredicate = predicate as IPredicate;
            if (wherePredicate == null && predicate != null)
            {
                wherePredicate = GetEntityPredicate(classMap, predicate);
            }

            return wherePredicate;
        }

        protected IPredicate GetIdPredicate(IClassMapper classMap, object id)
        {
            bool isSimpleType = ReflectionHelper.IsSimpleType(id.GetType());
            var keys = classMap.Properties.Where(p => p.KeyType != KeyType.NotAKey);
            IDictionary<string, object> paramValues = null;
            IList<IPredicate> predicates = new List<IPredicate>();
            if (!isSimpleType)
            {
                paramValues = ReflectionHelper.GetObjectValues(id);
            }

            foreach (var key in keys)
            {
                object value = id;
                if (!isSimpleType)
                {
                    value = paramValues[key.Name];
                }

                Type predicateType = typeof(FieldPredicate<>).MakeGenericType(classMap.EntityType);

                IFieldPredicate fieldPredicate = Activator.CreateInstance(predicateType) as IFieldPredicate;
                fieldPredicate.Not = false;
                fieldPredicate.Operator = Operator.Eq;
                fieldPredicate.PropertyName = key.Name;
                fieldPredicate.Value = value;
                predicates.Add(fieldPredicate);
            }

            return predicates.Count == 1
                       ? predicates[0]
                       : new PredicateGroup
                             {
                                 Operator = GroupOperator.And,
                                 Predicates = predicates
                             };
        }

        protected IPredicate GetKeyPredicate<T>(IClassMapper classMap, T entity) where T : class
        {
            var whereFields = classMap.Properties.Where(p => p.KeyType != KeyType.NotAKey);
            if (!whereFields.Any())
            {
                throw new ArgumentException("At least one Key column must be defined.");
            }

            IList<IPredicate> predicates = (from field in whereFields
                                            select new FieldPredicate<T>
                                                       {
                                                           Not = false,
                                                           Operator = Operator.Eq,
                                                           PropertyName = field.Name,
                                                           Value = field.PropertyInfo.GetValue(entity, null)
                                                       }).Cast<IPredicate>().ToList();

            return predicates.Count == 1
                       ? predicates[0]
                       : new PredicateGroup
                             {
                                 Operator = GroupOperator.And,
                                 Predicates = predicates
                             };
        }

        protected IPredicate GetEntityPredicate(IClassMapper classMap, object entity)
        {
            Type predicateType = typeof(FieldPredicate<>).MakeGenericType(classMap.EntityType);
            IList<IPredicate> predicates = new List<IPredicate>();
            foreach (var kvp in ReflectionHelper.GetObjectValues(entity))
            {
                IFieldPredicate fieldPredicate = Activator.CreateInstance(predicateType) as IFieldPredicate;
                fieldPredicate.Not = false;
                fieldPredicate.Operator = Operator.Eq;
                fieldPredicate.PropertyName = kvp.Key;
                fieldPredicate.Value = kvp.Value;
                predicates.Add(fieldPredicate);
            }

            return predicates.Count == 1
                       ? predicates[0]
                       : new PredicateGroup
                       {
                           Operator = GroupOperator.And,
                           Predicates = predicates
                       };
        }

        protected GridReaderResultReader GetMultipleByBatch(IDbConnection connection, GetMultiplePredicate predicate, IDbTransaction transaction, int? commandTimeout)
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            StringBuilder sql = new StringBuilder();
            foreach (var item in predicate.Items)
            {
                IClassMapper classMap = SqlGenerator.Configuration.GetMap(item.Type);
                IPredicate itemPredicate = item.Value as IPredicate;
                if (itemPredicate == null && item.Value != null)
                {
                    itemPredicate = GetPredicate(classMap, item.Value);
                }

                sql.AppendLine(SqlGenerator.Select(classMap, itemPredicate, item.Sort, parameters) + SqlGenerator.Configuration.Dialect.BatchSeperator);
            }

            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            SqlMapper.GridReader grid = connection.QueryMultiple(sql.ToString(), dynamicParameters, transaction, commandTimeout, CommandType.Text);
            return new GridReaderResultReader(grid);
        }

        protected SequenceReaderResultReader GetMultipleBySequence(IDbConnection connection, GetMultiplePredicate predicate, IDbTransaction transaction, int? commandTimeout)
        {
            IList<SqlMapper.GridReader> items = new List<SqlMapper.GridReader>();
            foreach (var item in predicate.Items)
            {
                Dictionary<string, object> parameters = new Dictionary<string, object>();
                IClassMapper classMap = SqlGenerator.Configuration.GetMap(item.Type);
                IPredicate itemPredicate = item.Value as IPredicate;
                if (itemPredicate == null && item.Value != null)
                {
                    itemPredicate = GetPredicate(classMap, item.Value);
                }

                string sql = SqlGenerator.Select(classMap, itemPredicate, item.Sort, parameters);
                DynamicParameters dynamicParameters = new DynamicParameters();
                foreach (var parameter in parameters)
                {
                    dynamicParameters.Add(parameter.Key, parameter.Value);
                }

                SqlMapper.GridReader queryResult = connection.QueryMultiple(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text);
                items.Add(queryResult);
            }

            return new SequenceReaderResultReader(items);
        }

        public IEnumerable<dynamic> Query(IDbConnection connection, string sql, IDbTransaction trans, CommandType? commandType, int? timeout, bool buffered)
        {
            return connection.Query(sql, null, trans, buffered, timeout, CommandType.Text);
        }

        public IEnumerable<dynamic> Query(IDbConnection connection, string sql, string orderBy, int pageIndex, int pageSize, IDbTransaction trans, CommandType? commandType, int? timeout, bool buffered)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("WITH Data_DataSet AS");
            sb.Append("(SELECT ROW_NUMBER() OVER (ORDER BY " + orderBy + ") AS Row, * FROM (");
            sb.Append(sql);
            sb.Append(")aa)");
            sb.Append("SELECT * FROM Data_DataSet");
            sb.Append(string.Format(" WHERE Row between ({0}*{1}+1) and ({1}*({0}+1))", pageIndex, pageSize));
            return connection.Query(sb.ToString(), null, trans, buffered, timeout, CommandType.Text);
        }


        public IEnumerable<K> Query<K>(IDbConnection connection, string sql, IDbTransaction trans, CommandType? commandType, int? timeout, bool buffered) where K : class
        {
            return connection.Query<K>(sql, null, trans, buffered, timeout, CommandType.Text);
        }

        public IEnumerable<K> Query<K>(IDbConnection connection, string sql, string orderBy, int pageIndex, int pageSize, IDbTransaction trans, CommandType? commandType, int? timeout, bool buffered) where K : class
        {
            int start = pageIndex * pageSize + 1;
            int end = pageSize * (pageIndex + 1);

            StringBuilder sb = new StringBuilder();
            sb.Append("WITH Data_DataSet AS");
            sb.Append("(SELECT ROW_NUMBER() OVER (ORDER BY " + orderBy + ") AS Row, * FROM (");
            sb.Append(sql);
            sb.Append(")aa)");
            sb.Append("SELECT * FROM Data_DataSet");
            sb.Append(string.Format(" WHERE Row between {0} and {1}", start, end));
            return connection.Query<K>(sb.ToString(), null, trans, buffered, timeout, CommandType.Text);
        }


        public int Update<T>(IDbConnection connection, object updateDict, object keyDict, IDbTransaction trans, int? timeout) where T : class
        {
            PropertyInfo[] d1 = updateDict.GetType().GetProperties();
            PropertyInfo[] d2 = keyDict.GetType().GetProperties();
            StringBuilder s1 = new StringBuilder();
            StringBuilder s2 = new StringBuilder();
            foreach (PropertyInfo item in d1)
            {
                var tp = item.PropertyType;
                string itemName = item.Name;
                if (d2.Count(p => p.Name.Equals(itemName, StringComparison.OrdinalIgnoreCase) && tp.Equals(p.PropertyType)) > 0)
                {
                    continue;
                }
                if (tp.Equals(typeof(string)) || tp.Equals(typeof(DateTime)))
                {
                    s1.Append(itemName + "='" + item.GetValue(updateDict, null) + "',");
                }
                else
                {
                    s1.Append(itemName + "=" + item.GetValue(updateDict, null) + ",");
                }
            }


            foreach (var item in d2)
            {
                var tp = item.PropertyType;
                if (tp.Equals(typeof(string)) || tp.Equals(typeof(DateTime)))
                {
                    s2.Append(item.Name + "='" + item.GetValue(keyDict, null) + "',");
                }
                else
                {
                    s2.Append(item.Name + "=" + item.GetValue(keyDict, null) + ",");
                }
            }

            if (s1.Length > 0)
            {
                s1.Remove(s1.Length - 1, 1);
            }
            if (s2.Length > 0)
            {
                s2.Remove(s2.Length - 1, 1);
            }
            return connection.Execute(string.Format("update {0} set {1} where {2}", typeof(T).Name, s1.ToString(), s2.ToString()), null, trans, timeout, CommandType.Text);
        }

        public int Update(IDbConnection connection, string sql, IDbTransaction trans, int? timeout)
        {
            return connection.Execute(sql, null, trans, timeout, CommandType.Text);
        }


        public IEnumerable<T> Where<T>(IDbConnection connection, Expression<Func<T, bool>> exp, string orderBy, IDbTransaction trans, int? timeout, bool buffered) where T : class
        {
            string str = SqlExpession.Where<T>(exp);
            if (string.IsNullOrWhiteSpace(orderBy))
            {
                IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
                orderBy = SqlGenerator.GetOrderBy(classMap);
            }
            if (string.IsNullOrEmpty(str))
            {
                return connection.Query<T>(string.Format("select * from {0} order by {1}", typeof(T).Name, orderBy), null, trans, buffered, timeout);
            }
            else
            {
                return connection.Query<T>(string.Format("select * from {0} where {1} order by {2}", typeof(T).Name, str, orderBy), null, trans, buffered, timeout);
            }
        }


        public IEnumerable<T> Where<T>(IDbConnection connection, Expression<Func<T, bool>> exp, string orderBy, int pageIndex, int pageSize, IDbTransaction trans, int? timeout, bool buffered) where T : class
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("WITH Data_DataSet AS");
            sb.Append("(SELECT ROW_NUMBER() OVER (ORDER BY " + orderBy + ") AS Row, * FROM (");
            string str = SqlExpession.Where<T>(exp);
            if (string.IsNullOrEmpty(str))
            {
                sb.Append(string.Format("select * from {0}", typeof(T).Name));
            }
            else
            {
                sb.Append(string.Format("select * from {0} where {1}", typeof(T).Name, str));
            }

            sb.Append(")aa)");
            sb.Append("SELECT * FROM Data_DataSet");
            sb.Append(string.Format(" WHERE Row between ({0}*{1}+1) and ({1}*({0}+1))", pageIndex, pageSize));
            return connection.Query<T>(sb.ToString(), null, trans, buffered, timeout);
        }


        public IEnumerable<T> Where<T>(IDbConnection connection, string where, string orderBy, IDbTransaction trans, int? timeout, bool buffered) where T : class
        {
            if (string.IsNullOrWhiteSpace(where))
                where = "1=1";
            if (string.IsNullOrWhiteSpace(orderBy))
            {
                IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
                orderBy = SqlGenerator.GetOrderBy(classMap);
            }
            return connection.Query<T>(string.Format("select * from {0} where {1} order by {2}", typeof(T).Name, where, orderBy), null, trans, buffered, timeout);
        }


        public IEnumerable<T> Where<T>(IDbConnection connection, string where, string orderBy, int pageIndex, int pageSize, IDbTransaction trans, int? timeout, bool buffered) where T : class
        {
            if (string.IsNullOrWhiteSpace(where))
                where = "1=1";
            if (string.IsNullOrWhiteSpace(orderBy))
            {
                IClassMapper classMap = SqlGenerator.Configuration.GetMap<T>();
                orderBy = SqlGenerator.GetOrderBy(classMap);
            }
            StringBuilder sb = new StringBuilder();
            sb.Append("WITH Data_DataSet AS");
            sb.Append("(SELECT ROW_NUMBER() OVER (ORDER BY " + orderBy + ") AS Row, * FROM (");
            sb.Append(string.Format("select * from {0} where {1}", typeof(T).Name, where));
            sb.Append(")aa)");
            sb.Append("SELECT * FROM Data_DataSet");
            sb.Append(string.Format(" WHERE Row between ({0}*{1}+1) and ({1}*({0}+1))", pageIndex, pageSize));
            return connection.Query<T>(sb.ToString(), null, trans, buffered, timeout);
        }


        public dynamic Execute<T>(IDbConnection connection, string pName, DynamicParameters paras, IDbTransaction transaction, int? commandTimeout, bool buffered) //where T : class
        {
            return connection.Query<T>(pName, paras, transaction, buffered, commandTimeout, CommandType.StoredProcedure);
        }
    }
}
