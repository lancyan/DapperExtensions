using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using DapperExtensions.Mapper;
using DapperExtensions.Sql;
using System.Linq.Expressions;
using Dapper;

namespace DapperExtensions
{
    public interface IDatabase : IDisposable
    {
        bool HasActiveTransaction { get; }
        IDbConnection Connection { get; }
        IDbTransaction BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted);
        void Commit();
        void Rollback();
        void RunInTransaction(Action action);
        T RunInTransaction<T>(Func<T> func);
        IMultipleResultReader GetMultiple(GetMultiplePredicate predicate, IDbTransaction transaction = null, int? commandTimeout = null);
        void ClearCache();
        Guid GetNextGuid();
        IClassMapper GetMap<T>() where T : class;

        T Get<T>(dynamic id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;
        T Get<T>(Expression<Func<T, bool>> exp, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;
        void Insert<T>(IEnumerable<T> entities, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;
        dynamic Insert<T>(T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;
        bool Update<T>(T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;

        int Update<T>(dynamic updateDict, dynamic keyDict, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;

        bool Delete<T>(T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;
        bool Delete<T>(IPredicate predicate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;

        bool DeleteById<T>(dynamic id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;
        IEnumerable<T> GetList<T>(IPredicate predicate, IList<ISort> sort, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = true) where T : class;

        IEnumerable<T> GetPage<T>(IPredicate predicate, IList<ISort> sort, int pageIndex, int pageSize, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = true) where T : class;

        IEnumerable<T> GetSet<T>(IPredicate predicate, IList<ISort> sort, int firstResult, int maxResults, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = true) where T : class;

        IEnumerable<dynamic> Query(string sql, CommandType? commandType, int? timeout, bool buffered, IDbTransaction trans = null);
        IEnumerable<dynamic> Query(string sql, string orderBy, int pageIndex, int pageSize, CommandType? commandType, int? timeout, bool buffered, IDbTransaction trans = null);

        IEnumerable<K> Query<K>(string sql, CommandType? commandType, int? timeout, bool buffered, IDbTransaction trans = null) where K : class;
        IEnumerable<K> Query<K>(string sql, string orderBy, int pageIndex, int pageSize, CommandType? commandType, int? timeout, bool buffered, IDbTransaction trans = null) where K : class;

        IEnumerable<T> Where<T>(Expression<Func<T, bool>> exp, string orderBy, int? timeout = null, bool buffered = true, IDbTransaction trans = null) where T : class;

        IEnumerable<T> Where<T>(Expression<Func<T, bool>> exp, string orderBy, int pageIndex, int pageSize, int? timeout = null, bool buffered = true, IDbTransaction trans = null) where T : class;

        IEnumerable<T> Where<T>(string where, string orderBy, int? timeout = null, bool buffered = true, IDbTransaction trans = null) where T : class;

        IEnumerable<T> Where<T>(string where, string orderBy, int pageIndex, int pageSize, int? timeout = null, bool buffered = true, IDbTransaction trans = null) where T : class;

        int Count<T>(IPredicate predicate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;
        int Count<T>(string sql = null, string where = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;
        int Count<T>(Expression<Func<T, bool>> exp, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;

        dynamic Execute<T>(string pName, DynamicParameters paras, IDbTransaction trans = null, int? timeout = null, bool buffered = true);

      
    }

    public class Database : IDatabase
    {
        private readonly IDapperImplementor _dapper;

        private IDbTransaction _transaction;

        public Database(IDbConnection connection, ISqlGenerator sqlGenerator)
        {
            this._dapper = new DapperImplementor(sqlGenerator);
            this.Connection = connection;

            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
            }
        }

        public bool HasActiveTransaction
        {
            get
            {
                return _transaction != null;
            }
        }

        public IDbConnection Connection { get; private set; }

        public void Dispose()
        {
            if (Connection.State != ConnectionState.Closed)
            {
                if (_transaction != null)
                {
                    _transaction.Rollback();
                }

                Connection.Close();
            }
        }

        public IDbTransaction BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            _transaction = Connection.BeginTransaction(isolationLevel);
            return _transaction;
        }

        public void Commit()
        {
            _transaction.Commit();
            _transaction = null;
        }

        public void Rollback()
        {
            _transaction.Rollback();
            _transaction = null;
        }

        public void RunInTransaction(Action action)
        {
            BeginTransaction();
            try
            {
                action();
                Commit();
            }
            catch (Exception ex)
            {
                if (HasActiveTransaction)
                {
                    Rollback();
                }

                throw;
            }
        }

        public T RunInTransaction<T>(Func<T> func)
        {
            BeginTransaction();
            try
            {
                T result = func();
                Commit();
                return result;
            }
            catch (Exception ex)
            {
                if (HasActiveTransaction)
                {
                    Rollback();
                }

                throw;
            }
        }

        public T Get<T>(dynamic id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return (T)_dapper.Get<T>(Connection, id, transaction, commandTimeout);
        }

        public T Get<T>(Expression<Func<T, bool>> exp, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return (T)_dapper.Get<T>(Connection, exp, transaction, commandTimeout);
        }

        public void Insert<T>(IEnumerable<T> entities, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            _dapper.Insert<T>(Connection, entities, transaction, commandTimeout);
        }

        public dynamic Insert<T>(T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.Insert<T>(Connection, entity, transaction, commandTimeout);
        }

        public bool Update<T>(T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.Update<T>(Connection, entity, transaction, commandTimeout);
        }

        public int Update<T>(dynamic updateDict, dynamic keyDict, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.Update<T>(Connection, updateDict, keyDict, transaction, commandTimeout);
        }

        public int Update(string sql, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            transaction = transaction ?? _transaction;
            return _dapper.Update(Connection, sql, transaction, commandTimeout);
        }

        public bool Delete<T>(T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return _dapper.Delete(Connection, entity, transaction, commandTimeout);
        }

        public bool Delete<T>(IPredicate predicate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.Delete<T>(Connection, predicate, transaction, commandTimeout);
        }
        public bool DeleteById<T>(dynamic id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.DeleteById<T>(Connection, id, _transaction, commandTimeout);
        }
        public IEnumerable<T> GetList<T>(IPredicate predicate, IList<ISort> sort, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = true) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.GetList<T>(Connection, predicate, sort, transaction, commandTimeout, buffered);
        }

        public IEnumerable<T> GetPage<T>(IPredicate predicate, IList<ISort> sort, int pageIndex, int pageSize, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = true) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.GetPage<T>(Connection, predicate, sort, pageIndex, pageSize, transaction, commandTimeout, buffered);
        }

        public IEnumerable<T> GetSet<T>(IPredicate predicate, IList<ISort> sort, int firstResult, int maxResults, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = true) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.GetSet<T>(Connection, predicate, sort, firstResult, maxResults, transaction, commandTimeout, buffered);
        }

        public int Count<T>(IPredicate predicate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.Count<T>(Connection, predicate, transaction, commandTimeout);
        }
        public int Count<T>(Expression<Func<T, bool>> exp, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.Count<T>(Connection, exp, transaction, commandTimeout);
        }

        public int Count<T>(string sql = null, string where = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            transaction = transaction ?? _transaction;
            return _dapper.Count<T>(Connection, sql, where, transaction, commandTimeout);
        }

        public IMultipleResultReader GetMultiple(GetMultiplePredicate predicate, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            transaction = transaction ?? _transaction;
            return _dapper.GetMultiple(Connection, predicate, transaction, commandTimeout);
        }

        public void ClearCache()
        {
            _dapper.SqlGenerator.Configuration.ClearCache();
        }

        public Guid GetNextGuid()
        {
            return _dapper.SqlGenerator.Configuration.GetNextGuid();
        }

        public IClassMapper GetMap<T>() where T : class
        {
            return _dapper.SqlGenerator.Configuration.GetMap<T>();
        }

        public IEnumerable<dynamic> Query(string sql, CommandType? commandType, int? timeout, bool buffered, IDbTransaction trans = null)
        {
            trans = trans ?? _transaction;
            return _dapper.Query(Connection, sql, trans, commandType, timeout, buffered);
        }

        public IEnumerable<dynamic> Query(string sql, string orderBy, int pageIndex, int pageSize, CommandType? commandType, int? timeout, bool buffered, IDbTransaction trans = null)
        {
            trans = trans ?? _transaction;
            return _dapper.Query(Connection, sql, orderBy, pageIndex, pageSize, trans, commandType, timeout, buffered);
        }

        public IEnumerable<K> Query<K>(string sql, CommandType? commandType, int? timeout, bool buffered, IDbTransaction trans = null) where K : class
        {
            trans = trans ?? _transaction;
            return _dapper.Query<K>(Connection, sql, trans, commandType, timeout, buffered);
        }

        public IEnumerable<K> Query<K>(string sql, string orderBy, int pageIndex, int pageSize, CommandType? commandType, int? timeout, bool buffered, IDbTransaction trans = null) where K : class
        {
            trans = trans ?? _transaction;
            return _dapper.Query<K>(Connection, sql, orderBy, pageIndex, pageSize, trans, commandType, timeout, buffered);
        }


        public IEnumerable<T> Where<T>(Expression<Func<T, bool>> exp, string orderBy, int? timeout = null, bool buffered = true, IDbTransaction trans = null) where T : class
        {
            return _dapper.Where<T>(Connection, exp, orderBy, trans, timeout, buffered);
        }


        public IEnumerable<T> Where<T>(string where, string orderBy, int? timeout = null, bool buffered = true, IDbTransaction trans = null) where T : class
        {
            return _dapper.Where<T>(Connection, where, orderBy, trans, timeout, buffered);
        }

        public IEnumerable<T> Where<T>(string where, string orderBy, int pageIndex, int pageSize, int? timeout = null, bool buffered = true, IDbTransaction trans = null) where T : class
        {
            return _dapper.Where<T>(Connection, where, orderBy, pageIndex, pageSize, trans, timeout, buffered);
        }

        public IEnumerable<T> Where<T>(Expression<Func<T, bool>> exp, string orderBy, int pageIndex, int pageSize, int? timeout = null, bool buffered = true, IDbTransaction trans = null) where T : class
        {
            return _dapper.Where<T>(Connection, exp, orderBy, pageIndex, pageSize, trans, timeout, buffered);
        }


        public dynamic Execute<T>(string pName, DynamicParameters paras, IDbTransaction trans = null, int? timeout = null, bool buffered = true)
        {
            trans = trans ?? _transaction;
            return _dapper.Execute<T>(Connection, pName, paras, trans, timeout, buffered);
        }
    }
}