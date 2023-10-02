//! Rizz is a query builder and migration generator for sqlite, sort of like an ORM.
//!
extern crate self as rizz;

pub use rizz_macros::{Row, Table};

#[cfg(not(target_arch = "wasm32"))]
use rusqlite::OpenFlags;
#[cfg(not(target_arch = "wasm32"))]
use serde::de::DeserializeOwned;
use serde::Deserialize;
#[cfg(not(target_arch = "wasm32"))]
use std::{marker::PhantomData, sync::Arc};

#[cfg(not(target_arch = "wasm32"))]
macro_rules! schema {
    ($($args:ident),*) => {{
        rizz::Schema {
            tables: vec![$(($args.table_name(),$args.create_table_sql()),)*],
            columns: vec![$($args.column_names(),)*],
            indices: vec![$($args.index_names(),)*]
        }
    }}
}

#[cfg(not(target_arch = "wasm32"))]
macro_rules! sql {
    ($sql:expr) => {{
        let clause = $sql;
        let params = vec![];

        rizz::Sql {
            clause: clause.into(),
            params
        }
    }};
    ($sql:expr, $($args:expr),*) => {{
        use rizz::ToSql;
        let clause = format!($sql, $($args.to_sql(),)*);
        let params = vec![
        $($args.to_sql(),)*
        ].into_iter().filter(|arg| match arg {
            Value::Text(_) => true,
            Value::Real(_) => true,
            Value::Integer(_) => true,
            Value::Blob(_) => true,
            _ => false
        }).collect::<Vec<_>>();

        rizz::Sql {
            clause: clause.into(),
            params
        }
    }};
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn connect(path: &str) -> Result<Connection, Error> {
    Connection::new(path).open().await
}

#[cfg(not(target_arch = "wasm32"))]
pub fn connection(path: &str) -> Connection {
    Connection::new(path)
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct Connection {
    path: Arc<str>,
    conn: Option<tokio_rusqlite::Connection>,
    open_flags: OpenFlags,
    pragma: Option<String>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Connection {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.into(),
            conn: None,
            open_flags: OpenFlags::default(),
            pragma: None,
        }
    }

    pub fn create_if_missing(mut self, arg: bool) -> Self {
        if !arg {
            self.open_flags = self.open_flags.difference(OpenFlags::SQLITE_OPEN_CREATE);
        }
        self
    }

    pub fn read_only(mut self, arg: bool) -> Self {
        if arg == true {
            self.open_flags = self.open_flags.union(OpenFlags::SQLITE_OPEN_READ_ONLY);
        }
        self
    }

    pub fn pragma(mut self, statement: &str) -> Self {
        let s = format!("PRAGMA {};", statement);
        match self.pragma {
            Some(ref mut p) => {
                p.push_str(&s);
            }
            None => {
                self.pragma = Some(s);
            }
        }
        self
    }

    pub fn journal_mode(mut self, mode: JournalMode) -> Self {
        let value = match mode {
            JournalMode::Delete => "DELETE",
            JournalMode::Truncate => "TRUNCATE",
            JournalMode::Persist => "PERSIST",
            JournalMode::Memory => "MEMORY",
            JournalMode::Wal => "WAL",
            JournalMode::Off => "OFF",
        };
        let s = format!("PRAGMA journal_mode = {};", value);
        match self.pragma {
            Some(ref mut p) => {
                p.push_str(&s);
            }
            None => self.pragma = Some(s),
        }
        self
    }

    pub fn synchronous(mut self, sync: Synchronous) -> Self {
        let value = match sync {
            Synchronous::Off => "OFF",
            Synchronous::Normal => "NORMAL",
            Synchronous::Full => "FULL",
            Synchronous::Extra => "EXTRA",
        };
        let s = format!("PRAGMA synchronous = {};", value);
        match self.pragma {
            Some(ref mut p) => {
                p.push_str(&s);
            }
            None => self.pragma = Some(s),
        }
        self
    }

    pub async fn open(mut self) -> Result<Self, Error> {
        let conn = tokio_rusqlite::Connection::open(self.path.as_ref()).await?;
        if let Some(p) = self.pragma.clone() {
            let _ = conn.call(move |conn| conn.execute_batch(&p)).await?;
        }
        self.conn = Some(conn);

        Ok(self)
    }

    pub fn db(self, schema: Schema) -> Database {
        Database::new(self, schema)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct Database {
    connection: tokio_rusqlite::Connection,
    schema: Schema,
}

#[cfg(not(target_arch = "wasm32"))]
impl Database {
    fn new(connection: Connection, schema: Schema) -> Self {
        Self {
            connection: connection.conn.expect("Database file not found"),
            schema,
        }
    }

    pub fn select(&self, columns: Arc<str>) -> Query {
        Query::new(self.connection.clone()).select(columns)
    }

    pub fn from(&self, table: impl Table) -> Query {
        Query::new(self.connection.clone()).from(table)
    }

    pub fn insert(&self, table: impl Table) -> Query {
        Query::new(self.connection.clone()).insert(table)
    }

    pub fn update(&self, table: impl Table) -> Query {
        Query::new(self.connection.clone()).update(table)
    }

    pub fn delete(&self, table: impl Table) -> Query {
        Query::new(self.connection.clone()).delete(table)
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let sql: Arc<str> = sql.into();
        let _ = self
            .connection
            .call(move |conn| conn.execute_batch(&sql))
            .await?;
        Ok(())
    }

    pub async fn query<T: DeserializeOwned + Send + 'static>(
        &self,
        sql: Sql,
    ) -> Result<Vec<T>, Error> {
        rows::<T>(&self.connection, sql).await
    }

    pub fn migrator(self) -> Migrator {
        Migrator::new(self)
    }
}

pub struct Migrator {
    db: Database,
    dry_run: bool,
}

impl Migrator {
    fn new(db: Database) -> Self {
        Migrator { db, dry_run: false }
    }

    fn dry_run(mut self, arg: bool) -> Self {
        self.dry_run = arg;
        self
    }

    pub async fn migrate(&self) -> Result<String, Error> {
        let migrations = vec![self.create_tables_sql().await, self.drop_tables_sql().await]
            .into_iter()
            .filter(|x| x.is_ok())
            .map(|x| x.unwrap())
            .filter(|x| !x.is_empty())
            .collect::<Vec<_>>()
            .join(";");
        if self.dry_run == true {
            Ok(migrations)
        } else {
            let _ = self.db.execute_batch(migrations.as_str()).await?;
            Ok("".into())
        }
    }

    pub async fn create_tables_sql(&self) -> Result<String, Error> {
        #[derive(Row, Deserialize)]
        struct TableName {
            name: String,
        }

        let rows: Vec<TableName> = self
            .db
            .query(sql!(
                "select name from sqlite_schema where type = {}",
                "table"
            ))
            .await?;
        let sql = self
            .db
            .schema
            .tables
            .iter()
            .filter(|(table_name, _)| {
                !rows
                    .iter()
                    .map(|r| r.name.as_str())
                    .collect::<Vec<_>>()
                    .contains(table_name)
            })
            .map(|(_, sql)| *sql)
            .collect::<Vec<_>>()
            .join(";");

        Ok(sql)
    }

    pub async fn drop_tables_sql(&self) -> Result<String, Error> {
        #[derive(Row, Deserialize, Debug)]
        struct TableName {
            name: String,
        }

        let rows: Vec<TableName> = self
            .db
            .query(sql!(
                "select name from sqlite_schema where type = {}",
                "table"
            ))
            .await?;
        let sql = rows
            .iter()
            .filter(|table_name| {
                !self
                    .db
                    .schema
                    .tables
                    .iter()
                    .map(|(name, _)| String::from(*name))
                    .collect::<Vec<_>>()
                    .contains(&table_name.name)
            })
            .map(|tn| format!(r#"drop table "{}""#, tn.name))
            .collect::<Vec<_>>()
            .join(";");

        Ok(sql)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    Delete,
    Truncate,
    Persist,
    Memory,
    #[default]
    Wal,
    Off,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Synchronous {
    Off,
    #[default]
    Normal,
    Full,
    Extra,
}

#[cfg(not(target_arch = "wasm32"))]
impl Value {
    fn to_sql(&self) -> &dyn rusqlite::ToSql {
        match self {
            Value::Text(s) => s,
            Value::Blob(b) => b,
            Value::Real(r) => r,
            Value::Integer(i) => i,
            Value::Lit(s) => s,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn execute(connection: &tokio_rusqlite::Connection, sql: Sql) -> Result<usize, Error> {
    let results = connection
        .call(move |conn| {
            let params = sql
                .params
                .iter()
                .map(|value| value.to_sql())
                .collect::<Vec<_>>();
            conn.prepare_cached(&sql.clause)?.execute(&*params)
        })
        .await?;

    Ok(results)
}

#[cfg(not(target_arch = "wasm32"))]
async fn rows<T: DeserializeOwned + Send + 'static>(
    connection: &tokio_rusqlite::Connection,
    sql: Sql,
) -> Result<Vec<T>, Error> {
    let results = connection
        .call(move |conn| {
            let params = sql
                .params
                .iter()
                .map(|value| value.to_sql())
                .collect::<Vec<_>>();
            let mut stmt = conn.prepare_cached(&sql.clause)?;
            let rows = stmt.query(&*params)?;
            let rows: Vec<T> = serde_rusqlite::from_rows::<T>(rows)
                .into_iter()
                .filter(|x| x.is_ok())
                .map(|x| x.unwrap())
                .collect();
            Ok(rows)
        })
        .await?;

    Ok(results)
}

#[cfg(not(target_arch = "wasm32"))]
async fn prepare<T: DeserializeOwned + Send + Sync + 'static>(
    connection: &tokio_rusqlite::Connection,
    sql: Sql,
) -> Result<Prep<T>, Error> {
    let cloned = connection.clone();
    let prep = connection
        .call(move |conn| {
            // this uses an internal Lru cache within rusqlite
            // and uses the sql as the key to the cache
            // not ideal but what can you do?
            let _ = conn.prepare_cached(&sql.clause)?;
            Ok(Prep {
                connection: cloned,
                sql,
                phantom: PhantomData::default(),
            })
        })
        .await?;
    Ok(prep)
}

#[cfg(not(target_arch = "wasm32"))]
impl Query {
    fn new(connection: tokio_rusqlite::Connection) -> Self {
        Self {
            connection,
            select: None,
            from: None,
            r#where: None,
            limit: None,
            insert_into: None,
            values_sql: None,
            values: vec![],
            delete: None,
            set: None,
            update: None,
            returning: None,
        }
    }

    pub fn select(mut self, columns: Arc<str>) -> Self {
        self.select = Some(format!("select {}", columns).into());

        self
    }

    pub fn from(mut self, table: impl Table) -> Self {
        self.from = Some(format!("from {}", table.table_name()).into());

        self
    }

    pub fn r#where(mut self, sql: Sql) -> Self {
        if let None = self.r#where {
            self.r#where = Some(format!("where {}", sql.clause).into())
        }
        self.values.extend(sql.params);
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(format!("limit {}", limit).into());
        self
    }

    pub fn sql(&self) -> Sql {
        Sql {
            clause: self.sql_clause(),
            params: self.values.clone(),
        }
    }

    pub fn sql_clause(&self) -> String {
        vec![
            self.select.clone(),
            self.from.clone(),
            self.insert_into.clone(),
            self.values_sql.clone(),
            self.update.clone(),
            self.set.clone(),
            self.delete.clone(),
            self.r#where.clone(),
            self.returning.clone(),
            self.limit.clone(),
        ]
        .into_iter()
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect::<Vec<_>>()
        .join(" ")
        .into()
    }

    pub async fn all<T>(self) -> Result<Vec<T>, Error>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let rows = rows(&self.connection, self.sql()).await?;
        Ok(rows)
    }

    pub async fn prepare<T>(self) -> Result<Prep<T>, Error>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let prep = prepare::<T>(&self.connection, self.sql()).await?;
        Ok(prep)
    }

    pub fn insert(mut self, table: impl Table) -> Self {
        self.insert_into = Some(table.insert_sql().into());
        self
    }

    pub fn values(mut self, row: impl Row) -> Self {
        self.values_sql = Some(row.insert_sql().into());
        self.values = row.values();
        self
    }

    pub fn update(mut self, table: impl Table) -> Self {
        self.update = Some(table.update_sql().into());
        self
    }

    pub fn set(mut self, row: impl Row) -> Self {
        self.set = Some(row.set_sql().into());
        self.values = row.values();
        self
    }

    pub fn delete(mut self, table: impl Table) -> Self {
        self.delete = Some(table.delete_sql().into());
        self
    }

    pub async fn returning<T: Row + DeserializeOwned + Send + Sync + 'static>(
        mut self,
        columns: Arc<str>,
    ) -> Result<T, Error> {
        self.returning = Some(format!("returning {}", columns).into());
        let rows = rows::<T>(&self.connection, self.sql()).await?;
        if let Some(row) = rows.into_iter().nth(0) {
            Ok(row)
        } else {
            Err(Error::InsertError(format!(
                "failed to insert {}",
                self.sql().clause
            )))
        }
    }

    pub async fn rows_affected(self) -> Result<usize, Error> {
        let rows_affected = execute(&self.connection, self.sql()).await?;
        Ok(rows_affected)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct Prep<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    connection: tokio_rusqlite::Connection,
    sql: Sql,
    phantom: PhantomData<T>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> Prep<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    async fn all(&self) -> Result<Vec<T>, Error> {
        let rows = rows::<T>(&self.connection, self.sql.clone()).await?;
        Ok(rows)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct Query {
    connection: tokio_rusqlite::Connection,
    select: Option<Arc<str>>,
    from: Option<Arc<str>>,
    r#where: Option<Arc<str>>,
    limit: Option<Arc<str>>,
    insert_into: Option<Arc<str>>,
    set: Option<Arc<str>>,
    delete: Option<Arc<str>>,
    values_sql: Option<Arc<str>>,
    returning: Option<Arc<str>>,
    values: Vec<Value>,
    update: Option<Arc<str>>,
}

#[derive(Clone, Copy)]
pub struct Text(&'static str);

#[derive(Clone, Copy)]
pub struct Blob(&'static str);

#[derive(Clone, Copy)]
pub struct Integer(&'static str);

#[derive(Clone, Copy)]
pub struct Real(&'static str);

pub trait ToColumn {
    fn to_column(&self) -> &'static str;
}

impl ToColumn for Text {
    fn to_column(&self) -> &'static str {
        self.0
    }
}

impl ToColumn for Integer {
    fn to_column(&self) -> &'static str {
        self.0
    }
}

impl ToColumn for Blob {
    fn to_column(&self) -> &'static str {
        self.0
    }
}

impl ToColumn for Real {
    fn to_column(&self) -> &'static str {
        self.0
    }
}

pub fn star() -> std::sync::Arc<str> {
    "*".into()
}

pub fn count(columns: impl ToColumn) -> std::sync::Arc<str> {
    format!("count({}) as count", columns.to_column()).into()
}

pub fn and(left: Sql, right: Sql) -> Sql {
    let mut params: Vec<Value> = vec![];
    params.extend(left.params);
    params.extend(right.params);

    Sql {
        clause: format!("({} and {})", left.clause, right.clause),
        params,
    }
}

pub fn or(left: Sql, right: Sql) -> Sql {
    let mut params: Vec<Value> = vec![];
    params.extend(left.params);
    params.extend(right.params);

    Sql {
        clause: format!("({} or {})", left.clause, right.clause),
        params,
    }
}

pub fn eq(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} = ?", left.to_column()),
        params: vec![right.into()],
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    // TODO: Null,
    Lit(&'static str),
    Text(std::sync::Arc<str>),
    Blob(Vec<u8>),
    Real(f64),
    Integer(i64),
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Text(value.into())
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Text(value.into())
    }
}

impl From<std::sync::Arc<str>> for Value {
    fn from(value: std::sync::Arc<str>) -> Self {
        Value::Text(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Integer(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Real(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Blob(value)
    }
}

pub trait Row {
    fn values(&self) -> Vec<Value>;
    fn insert_sql(&self) -> &'static str;
    fn set_sql(&self) -> &'static str;
}

#[cfg(not(target_arch = "wasm32"))]
pub trait Table {
    fn new() -> Self
    where
        Self: Sized + Clone;
    fn table_name(&self) -> &'static str;
    fn column_names(&self) -> &'static str;
    fn insert_sql(&self) -> &'static str;
    fn update_sql(&self) -> &'static str;
    fn delete_sql(&self) -> &'static str;
    fn index_names(&self) -> &'static str;
    fn create_table_sql(&self) -> &'static str;
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database connection closed")]
    ConnectionClosed,
    #[error("database connection closing: {0}")]
    Close(String),
    #[error("database error: {0}")]
    Database(String),
    #[error("missing from statement in sql query")]
    MissingFrom,
    #[error("error inserting record {0}")]
    InsertError(String),
}

#[cfg(not(target_arch = "wasm32"))]
impl From<tokio_rusqlite::Error> for Error {
    fn from(value: tokio_rusqlite::Error) -> Self {
        match value {
            tokio_rusqlite::Error::ConnectionClosed => Self::ConnectionClosed,
            tokio_rusqlite::Error::Close((_, error)) => Self::Close(error.to_string()),
            tokio_rusqlite::Error::Rusqlite(err) => Self::Database(err.to_string()),
            _ => todo!(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        match value {
            rusqlite::Error::SqliteFailure(_, _) => todo!(),
            rusqlite::Error::SqliteSingleThreadedMode => todo!(),
            rusqlite::Error::FromSqlConversionFailure(_, _, _) => todo!(),
            rusqlite::Error::IntegralValueOutOfRange(_, _) => todo!(),
            rusqlite::Error::Utf8Error(_) => todo!(),
            rusqlite::Error::NulError(_) => todo!(),
            rusqlite::Error::InvalidParameterName(_) => todo!(),
            rusqlite::Error::InvalidPath(_) => todo!(),
            rusqlite::Error::ExecuteReturnedResults => todo!(),
            rusqlite::Error::QueryReturnedNoRows => todo!(),
            rusqlite::Error::InvalidColumnIndex(_) => todo!(),
            rusqlite::Error::InvalidColumnName(_) => todo!(),
            rusqlite::Error::InvalidColumnType(_, _, _) => todo!(),
            rusqlite::Error::StatementChangedRows(_) => todo!(),
            rusqlite::Error::ToSqlConversionFailure(_) => todo!(),
            rusqlite::Error::InvalidQuery => todo!(),
            rusqlite::Error::MultipleStatement => todo!(),
            rusqlite::Error::InvalidParameterCount(_, _) => todo!(),
            _ => todo!(),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Lit(str) => f.write_str(str),
            _ => f.write_str("?"),
        }
    }
}

pub trait ToSql {
    fn to_sql(&self) -> Value;
}

impl ToSql for &'static str {
    fn to_sql(&self) -> Value {
        Value::Text(self.to_owned().into())
    }
}

impl ToSql for i64 {
    fn to_sql(&self) -> Value {
        Value::Integer(*self)
    }
}

impl ToSql for f64 {
    fn to_sql(&self) -> Value {
        Value::Real(*self)
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> Value {
        Value::Blob(self.clone())
    }
}

impl ToSql for Integer {
    fn to_sql(&self) -> Value {
        Value::Lit(self.0)
    }
}

impl ToSql for Blob {
    fn to_sql(&self) -> Value {
        Value::Lit(self.0)
    }
}

impl ToSql for Real {
    fn to_sql(&self) -> Value {
        Value::Lit(self.0)
    }
}

impl ToSql for Text {
    fn to_sql(&self) -> Value {
        Value::Lit(self.0)
    }
}

#[derive(Clone, Debug)]
pub struct Sql {
    clause: String,
    params: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct Schema {
    tables: Vec<(&'static str, &'static str)>,
    columns: Vec<&'static str>,
    indices: Vec<&'static str>,
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use rizz::{
        and, connect, count, eq, or, star, Database, Error, Integer, Row, Sql, Table, Value,
    };
    use serde::Deserialize;

    type TestResult<T> = Result<T, Error>;

    async fn test_db() -> TestResult<Database> {
        let accounts = Accounts::new();
        let schema = schema!(accounts);
        let db = connect(":memory:").await?.db(schema);

        Ok(db)
    }

    #[tokio::test]
    async fn sql_macro_works() -> TestResult<()> {
        let accounts = Accounts::new();

        let sql = sql!("select * from {} where {} = {}", accounts, accounts.id, "1");
        assert_eq!(
            sql.clause.to_string(),
            r#"select * from "accounts" where "accounts"."id" = ?"#
        );
        assert_eq!(sql.params, vec![Value::Text("1".into())]);

        let sql: Sql = sql!("select * from {} where {} = {}", accounts, accounts.id, 1);
        assert_eq!(
            sql.clause.to_string(),
            r#"select * from "accounts" where "accounts"."id" = ?"#
        );
        assert_eq!(sql.params, vec![Value::Integer(1)]);

        let sql: Sql = sql!("select * from {} where {} = {}", accounts, accounts.id, 1.0);
        assert_eq!(
            sql.clause.to_string(),
            r#"select * from "accounts" where "accounts"."id" = ?"#
        );
        assert_eq!(sql.params, vec![Value::Real(1.0)]);

        let bytes: Vec<u8> = vec![0x10];
        let sql: Sql = sql!(
            "select * from {} where {} = {}",
            accounts,
            accounts.id,
            bytes
        );
        assert_eq!(
            sql.clause.to_string(),
            r#"select * from "accounts" where "accounts"."id" = ?"#
        );
        assert_eq!(sql.params, vec![Value::Blob(vec![0x10])]);

        Ok(())
    }

    #[tokio::test]
    async fn where_clauses_work() -> TestResult<()> {
        let db = test_db().await?;
        let accounts = Accounts::new();

        let query = db.select(star()).from(accounts).r#where(or(
            and(eq(accounts.id, "1"), eq(accounts.id, "1".to_owned())),
            eq(accounts.id, 1),
        ));

        let query2 = db.select(star()).from(accounts).r#where(sql!(
            "(({} = {} and {} = {}) or {} = {})",
            accounts.id,
            "1",
            accounts.id,
            "1",
            accounts.id,
            1
        ));

        assert_eq!(query.sql_clause(), query2.sql_clause());

        assert_eq!(
            query.sql_clause(),
            r#"select * from "accounts" where (("accounts"."id" = ? and "accounts"."id" = ?) or "accounts"."id" = ?)"#
        );
        assert_eq!(
            query.values,
            vec![
                Value::Text("1".into()),
                Value::Text("1".into()),
                Value::Integer(1)
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn crud_works() -> TestResult<()> {
        let db = test_db().await?;

        let _ = db.execute_batch("create table accounts (id)").await?;

        let accounts = Accounts::new();
        let inserted: Account = db
            .insert(accounts)
            .values(Account { id: 1 })
            .returning(star())
            .await?;

        assert_eq!(inserted.id, 1);

        let new_account = Account { id: 2 };
        let updated: Account = db
            .update(accounts)
            .set(new_account)
            .r#where(eq(accounts.id, 1))
            .returning(star())
            .await?;

        assert_eq!(updated.id, 2);

        let rows: Vec<Account> = db
            .select(star())
            .from(accounts)
            .r#where(eq(accounts.id, 2))
            .all()
            .await?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows.iter().nth(0).unwrap().id, 2);

        let rows_affected = db
            .delete(accounts)
            .r#where(eq(accounts.id, 2))
            .rows_affected()
            .await?;

        assert_eq!(rows_affected, 1);

        let num_rows: Vec<RowCount> = db
            .select(count(accounts.id))
            .from(accounts)
            .r#where(eq(accounts.id, 2))
            .all()
            .await?;

        assert_ne!(num_rows.iter().nth(0), None);
        assert_eq!(num_rows.iter().nth(0).unwrap().count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn migrating_tables_works() -> TestResult<()> {
        #[derive(Deserialize)]
        struct TableName {
            name: String,
        }

        let accounts = Accounts::new();
        let schema = schema!(accounts);
        let conn = connect(":memory:").await?;
        let db = conn.clone().db(schema);
        let migrations = db.clone().migrator().dry_run(true).migrate().await?;
        assert_eq!(
            r#"create table "accounts" (id integer primary key)"#,
            migrations
        );

        let _ = db.clone().migrator().migrate().await?;
        let tables: Vec<TableName> = db
            .query(sql!("select name from sqlite_schema where type = 'table'"))
            .await?;
        let tables = tables.into_iter().map(|x| x.name).collect::<Vec<_>>();
        assert_eq!(vec!["accounts"], tables);

        let new_schema = schema!();
        let db = Database::new(conn, new_schema);
        let migrations = db.clone().migrator().dry_run(true).migrate().await?;
        assert_eq!(r#"drop table "accounts""#, migrations);

        let _ = db.clone().migrator().migrate().await?;
        let num_tables = db
            .query::<TableName>(sql!("select name from sqlite_schema where type = 'table'"))
            .await?
            .len();
        assert_eq!(0, num_tables);

        Ok(())
    }

    #[derive(Deserialize, PartialEq, Debug)]
    struct RowCount {
        count: i64,
    }

    #[derive(Row, Deserialize)]
    struct Account {
        id: i64,
    }

    #[derive(Table, Clone, Copy)]
    #[rizz(table = "accounts")]
    struct Accounts {
        #[rizz(primary_key)]
        id: Integer,
    }
}
