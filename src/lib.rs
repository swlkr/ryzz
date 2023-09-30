//! Rizz is a query builder and migration generator for sqlite. Don't call it an ORM.
//!
extern crate self as rizz;

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use rizz::{connect, db, Error, Row, Table, Text, Value};
    use serde::Deserialize;

    type TestResult<T> = Result<T, Error>;

    #[tokio::test]
    async fn it_works() -> TestResult<()> {
        let conn = connect(":memory:").await?;
        let db = db(conn);

        let _ = db
            .execute_batch("create table accounts (account_id)")
            .await?;

        let accounts = Accounts::default();
        let sql = db
            .insert(accounts)
            .values(Account {
                account_id: "1".into(),
            })
            .returning()
            .sql()?;
        assert_eq!(
            sql,
            "insert into accounts (account_id) values (?) returning *"
        );
        let _inserted: Account = db
            .insert(accounts)
            .values(Account {
                account_id: "1".into(),
            })
            .returning()
            .execute()
            .await?;
        let rows: Vec<Account> = db.select().from(accounts).all().await?;

        assert_eq!(rows.len(), 1);

        Ok(())
    }

    #[derive(Deserialize)]
    struct Account {
        account_id: String,
    }

    impl Row for Account {
        fn column_names(&self) -> std::sync::Arc<str> {
            "account_id".into()
        }

        fn insert_placeholders(&self) -> std::sync::Arc<str> {
            "?".into()
        }

        fn insert_values(&self) -> Vec<Value> {
            vec![Value::from(self.account_id.clone())]
        }
    }

    #[derive(Default, Clone, Copy)]
    struct Accounts {
        account_id: Text,
    }

    impl Table for Accounts {
        fn table_name(&self) -> std::sync::Arc<str> {
            "accounts".into()
        }

        fn column_names(&self) -> std::sync::Arc<str> {
            "account_id".into()
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{OpenFlags, ToSql};
#[cfg(not(target_arch = "wasm32"))]
use serde::de::DeserializeOwned;
#[cfg(not(target_arch = "wasm32"))]
use std::{marker::PhantomData, sync::Arc};

#[cfg(not(target_arch = "wasm32"))]
pub fn db(connection: Connection) -> Database {
    Database::new(connection)
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
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct Database {
    connection: tokio_rusqlite::Connection,
}

#[cfg(not(target_arch = "wasm32"))]
impl Database {
    fn new(connection: Connection) -> Self {
        Self {
            connection: connection.conn.expect("Database file not found"),
        }
    }

    pub fn select(&self) -> Query {
        Query::new(self.connection.clone()).select(star())
    }

    pub fn from(&self, table: impl Table) -> Query {
        Query::new(self.connection.clone()).from(table)
    }

    pub fn insert(&self, table: impl Table) -> Query {
        Query::new(self.connection.clone()).insert(table)
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let sql = sql.to_owned();
        let _ = self
            .connection
            .call(move |conn| conn.execute_batch(&sql))
            .await?;
        Ok(())
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
    fn to_sql(&self) -> &dyn ToSql {
        match self {
            Value::Text(s) => s,
            Value::Blob(b) => b,
            Value::Real(r) => r,
            Value::Integer(i) => i,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn rows<T: DeserializeOwned + Send + Sync + 'static>(
    connection: &tokio_rusqlite::Connection,
    sql: Arc<str>,
    binds: Option<Vec<Value>>,
) -> Result<Vec<T>, Error> {
    let results = connection
        .call(move |conn| {
            let mut stmt = conn.prepare_cached(&sql)?;
            let rows = match binds {
                Some(values) => {
                    let params = values
                        .iter()
                        .map(|value| value.to_sql())
                        .collect::<Vec<_>>();
                    stmt.query(&*params)?
                }
                None => stmt.query([])?,
            };
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
    sql: Arc<str>,
    binds: Option<Vec<Value>>,
) -> Result<Prep<T>, Error> {
    let cloned = connection.clone();
    let prep = connection
        .call(move |conn| {
            // this uses an internal Lru cache within rusqlite
            // and uses the sql as the key to the cache
            // not ideal but what can you do?
            let _ = conn.prepare_cached(&sql)?;
            Ok(Prep {
                connection: cloned,
                sql: sql.into(),
                params: binds,
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
            placeholders: None,
            values: None,
            returning: None,
        }
    }

    pub fn select(mut self, row: impl Row) -> Self {
        self.select = Some(format!("select {}", row.column_names()).into());

        self
    }

    pub fn from(mut self, table: impl Table) -> Self {
        self.from = Some(format!("from {}", table.table_name()).into());

        self
    }

    // pub fn r#where(mut self, param: Value) -> Self {
    //     format!("where {}", )
    //     if let Some(ref mut r#where) = self.r#where {
    //         r#where.push(bind);
    //     } else {
    //         self.r#where = Some(vec![bind]);
    //     }
    //     self
    // }

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    fn limit_sql(&self) -> Option<Arc<str>> {
        match self.limit {
            Some(lim) => Some(format!("limit {}", lim).into()),
            None => None,
        }
    }

    pub fn sql(&self) -> Result<String, Error> {
        Ok(self.to_sql()?.to_string())
    }

    fn to_sql(&self) -> Result<Arc<str>, Error> {
        let sql = vec![
            self.insert_into.clone(),
            self.placeholders.clone(),
            self.returning.clone(),
            self.select.clone(),
            self.from.clone(),
            self.where_clause(),
            self.limit_sql(),
        ]
        .into_iter()
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect::<Vec<_>>()
        .join(" ")
        .into();

        Ok(sql)
    }

    fn where_clause(&self) -> Option<Arc<str>> {
        // match self.r#where {
        //     Some(ref r#where) => {
        //         let clause = r#where
        //             .iter()
        //             .enumerate()
        //             .map(|(i, bind)| format!("{} = ?{}", bind.column, i + 1))
        //             .collect::<Vec<_>>()
        //             .join(" and ");
        //         Some(format!("where {}", clause).into())
        //     }
        //     None => None,
        // }
        None
    }

    pub async fn all<T: DeserializeOwned + Send + Sync + 'static>(self) -> Result<Vec<T>, Error>
    where
        T: Row,
    {
        let sql = self.sql()?;
        let rows = rows(&self.connection, sql.into(), self.values).await?;
        Ok(rows)
    }

    pub async fn prepare<T: DeserializeOwned + Send + Sync + 'static>(
        self,
    ) -> Result<Prep<T>, Error>
    where
        T: Row,
    {
        let sql = self.sql()?;
        let prep = prepare::<T>(&self.connection, sql.into(), self.values).await?;
        Ok(prep)
    }

    pub fn insert(mut self, table: impl Table) -> Self {
        self.insert_into = Some(
            format!(
                "insert into {} ({})",
                table.table_name(),
                table.column_names()
            )
            .into(),
        );
        self
    }

    pub fn values(mut self, row: impl Row) -> Self {
        self.placeholders = Some(format!("values ({})", row.insert_placeholders()).into());
        self.values = Some(row.insert_values());
        self
    }

    pub fn returning(mut self) -> Self {
        self.returning = Some("returning *".into());
        self
    }

    pub async fn execute<T: Row + DeserializeOwned + Send + Sync + 'static>(
        mut self,
    ) -> Result<T, Error> {
        self.returning = Some("returning *".into());
        let sql = self.to_sql()?;
        let rows = rows::<T>(&self.connection, sql.clone(), self.values).await?;
        if let Some(row) = rows.into_iter().nth(0) {
            Ok(row)
        } else {
            Err(Error::InsertError(sql.to_string()))
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct Prep<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    connection: tokio_rusqlite::Connection,
    params: Option<Vec<Value>>,
    sql: Arc<str>,
    phantom: PhantomData<T>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> Prep<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    async fn all(&self) -> Result<Vec<T>, Error> {
        let rows = rows::<T>(&self.connection, self.sql.clone(), self.params.clone()).await?;
        Ok(rows)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct Query {
    connection: tokio_rusqlite::Connection,
    select: Option<Arc<str>>,
    from: Option<Arc<str>>,
    r#where: Option<Arc<str>>,
    limit: Option<u64>,
    insert_into: Option<Arc<str>>,
    placeholders: Option<Arc<str>>,
    returning: Option<Arc<str>>,
    values: Option<Vec<Value>>,
}

pub type Text = &'static str;
pub type Blob = &'static str;
pub type Integer = &'static str;
pub type Real = &'static str;

#[derive(Default)]
pub struct All;

impl Row for All {
    fn column_names(&self) -> std::sync::Arc<str> {
        "*".into()
    }

    fn insert_placeholders(&self) -> std::sync::Arc<str> {
        "".into()
    }

    fn insert_values(&self) -> Vec<Value> {
        vec![]
    }
}

pub fn star() -> All {
    All::default()
}

#[derive(Clone, Debug)]
pub enum Value {
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
    fn column_names(&self) -> std::sync::Arc<str>;
    fn insert_placeholders(&self) -> std::sync::Arc<str>;
    fn insert_values(&self) -> Vec<Value>;
}

#[cfg(not(target_arch = "wasm32"))]
pub trait Table {
    fn table_name(&self) -> std::sync::Arc<str>;
    fn column_names(&self) -> std::sync::Arc<str>;
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
