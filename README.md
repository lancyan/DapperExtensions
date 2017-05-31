# DapperExtensions

ConnectionItem 配置多种不同的数据库连接   
ConnEnum 配置数据库的读写分离，依赖于ConnectionItem   
DBMapper 初始化读写库    
IBaseDAL、BaseDAL数据库底层操作封装类   

主要暴露的操作方法有：   
DapperExtensions sample：   
 
INSERT:   
void Insert(params T[] objs);   
dynamic Insert(T obj);   

UPDATE：   
bool Update(T entity);   
bool Update(dynamic updateDict, dynamic keyDict);   

DELETE:   
bool Delete(IPredicate predicate);   
bool Delete(T entity);   
bool DeleteById(dynamic id);   
   
COUNT:      
int Count(IPredicate predicate);   
int Count(string sql = null, string where = null);   
int Count(Expression<Func<T, bool>> predicate);   

EXISTS:      
bool Exists(IPredicate predicate);   
bool Exists(string sql = null, string where = null);   
bool Exists(Expression<Func<T, bool>> predicate);   

GET:   
T Get(dynamic id);   
T Get(Expression<Func<T, bool>> predicate);   

SELECT  for simple table:   
IEnumerable<T> Where(IPredicate predicate = null, IList<ISort> sort = null);   
IEnumerable<T> Where(IPredicate predicate, IList<ISort> sort, int pageIndex, int pageSize);   

IEnumerable<T> Where(Expression<Func<T, bool>> predicate, string orderBy = null);   
IEnumerable<T> Where(Expression<Func<T, bool>> predicate, string orderBy, int pageIndex, int pageSize);   

IEnumerable<T> Where(string where, string orderBy = null);   
IEnumerable<T> Where(string where, string orderBy, int pageIndex, int pageSize);   
 
SELECT for multiple table:   
IEnumerable<K> Query<K>(string sql, int? timeout = null, bool buffered = true) where K : class   
IEnumerable<K> Query<K>(string sql, string orderBy, int pageIndex, int pageSize, int? timeout = null, bool buffered = true) where K : class   
IEnumerable<dynamic> Query(string sql, int? timeout = null, bool buffered = true)   
IEnumerable<dynamic> Query(string sql, string orderBy, int pageIndex, int pageSize, int? timeout = null, bool buffered = true)   

Execute Procedure:   
dynamic Execute<K>(string pName, DynamicParameters paras = null, int state = 1)   









