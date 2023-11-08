//! Rizz is a query builder and migration generator for sqlite, sort of like an ORM.
//!
extern crate self as rizz;

pub use rizz_macros::Table;
use rusqlite::OpenFlags;
use serde::Serialize;
use serde::{de::DeserializeOwned, Deserialize};
use std::{marker::PhantomData, sync::Arc};

#[macro_export]
macro_rules! sql {
    ($sql:expr) => {{
        let clause = $sql;
        let params = vec![];

        rizz::Sql {
            clause: clause.into(),
            params,
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

pub fn connection(path: &str) -> Connection {
    Connection::new(path)
}

#[derive(Clone, Debug)]
pub struct Connection {
    path: Arc<str>,
    conn: Option<tokio_rusqlite::Connection>,
    open_flags: OpenFlags,
    pragma: Option<String>,
}

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

    pub fn foreign_keys(mut self, val: bool) -> Self {
        let val = match val {
            true => "ON",
            false => "OFF",
        };
        let statement = format!("PRAGMA foreign_keys = {}", val);
        match self.pragma {
            Some(ref mut p) => {
                p.push_str(statement.as_str());
            }
            None => {
                self.pragma = Some(statement);
            }
        };
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

    pub fn database(self) -> Database {
        Database::new(self)
    }
}

#[derive(Clone, Debug)]
pub struct Database {
    connection: tokio_rusqlite::Connection,
}

impl Database {
    pub fn new(connection: Connection) -> Self {
        Self {
            connection: connection.conn.expect("Database file not found"),
        }
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

    pub fn create_table(&self, table: impl Table) -> Migrator {
        Migrator::new(self.clone()).create_table(table)
    }

    pub fn create_unique_index(&self, table: impl Table, columns: Vec<impl ToColumn>) -> Migrator {
        Migrator::new(self.clone()).create_unique_index(table, columns)
    }

    pub fn add_column(&self, table: impl Table, column: impl ToColumn) -> Migrator {
        Migrator::new(self.clone()).add_column(table, column)
    }

    pub fn select(&self) -> Query {
        Query::new(self.clone()).select()
    }

    pub fn insert(&self, table: impl Table) -> Query {
        Query::new(self.clone()).insert(table)
    }

    pub fn delete_from(&self, table: impl Table) -> Query {
        Query::new(self.clone()).delete(table)
    }

    pub fn update(&self, table: impl Table) -> Query {
        Query::new(self.clone()).update(table)
    }

    pub fn count(&self) -> Query {
        Query::new(self.clone()).count()
    }

    pub async fn schema(&self) -> Result<String, Error> {
        let rows: Vec<SchemaRow> =
            rows(&self.connection, sql!("select sql from sqlite_master")).await?;
        let output = rows
            .into_iter()
            .map(|r| r.sql)
            .collect::<Vec<_>>()
            .join("\n");
        Ok(output)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SchemaRow {
    sql: String,
}

#[derive(Clone, Debug)]
pub struct Migrator {
    db: Database,
    statements: Vec<Arc<str>>,
    quiet_migrations: bool,
}

impl Migrator {
    fn new(db: Database) -> Self {
        let mut migrator = Migrator {
            db,
            statements: vec![],
            quiet_migrations: false,
        };
        let migrations = Migrations::new();
        migrator = migrator
            .create_table(migrations)
            .create_unique_index(migrations, vec![migrations.sql]);
        migrator
    }

    pub fn create_table(mut self, table: impl Table) -> Self {
        self.statements.push(table.create_table_sql().into());
        self
    }

    pub fn add_column(mut self, table: impl Table, column: impl ToColumn) -> Self {
        self.statements
            .push(table.add_column_sql(column.to_column()).into());
        self
    }

    pub fn create_unique_index(mut self, table: impl Table, columns: Vec<impl ToColumn>) -> Self {
        let column_names: Vec<&str> = columns.iter().map(|c| c.to_column()).collect();
        self.statements
            .push(table.create_index_sql(true, column_names).into());
        self
    }

    pub fn create_index(mut self, table: impl Table, columns: Vec<impl ToColumn>) -> Self {
        let column_names: Vec<&str> = columns.iter().map(|c| c.to_column()).collect();
        self.statements
            .push(table.create_index_sql(false, column_names).into());
        self
    }

    pub fn drop_index(mut self, table: impl Table, columns: Vec<impl ToColumn>) -> Self {
        let column_names: Vec<&str> = columns.iter().map(|c| c.to_column()).collect();
        self.statements
            .push(table.drop_index_sql(column_names).into());
        self
    }

    async fn migrations(&self) -> Result<Vec<Migration>, Error> {
        let migrations = Migrations::new();
        let rows: Vec<Migration> = self.db.select().from(migrations).all().await?;
        Ok(rows)
    }

    async fn insert_migration(&self, migration: Migration) -> Result<(), Error> {
        let migrations = Migrations::new();
        let _row: Migration = self
            .db
            .insert(migrations)
            .values(migration)?
            .returning()
            .await?;
        Ok(())
    }

    pub fn quiet_migrations(mut self) -> Self {
        self.quiet_migrations = true;
        self
    }

    pub async fn migrate(&self) -> Result<(), Error> {
        let applied: Vec<Arc<str>> = self
            .migrations()
            .await
            .unwrap_or_default()
            .iter()
            .map(|x| x.sql.clone().into())
            .collect();
        let statements: Vec<&Arc<str>> = self
            .statements
            .iter()
            .filter(|sql| !applied.contains(sql))
            .collect();

        if statements.len() > 0 && !self.quiet_migrations {
            println!("=== Running migrations ===");
        }

        for sql in &statements {
            if !self.quiet_migrations {
                println!("{sql}");
            }
            let _ = self.db.execute_batch(sql).await?;
            let _x = self
                .insert_migration(Migration {
                    sql: sql.to_string(),
                })
                .await?;
        }

        if statements.len() > 0 && !self.quiet_migrations {
            println!("=== Migrations finished successfully ===");
        } else if !self.quiet_migrations {
            println!("=== No pending migrations ===");
        }

        Ok(())
    }
}

#[derive(Table, Clone, Copy)]
#[rizz(table = "migrations")]
struct Migrations {
    #[rizz(not_null)]
    sql: Text,
}

#[derive(Serialize, Deserialize)]
struct Migration {
    sql: String,
}

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

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Synchronous {
    Off,
    #[default]
    Normal,
    Full,
    Extra,
}

impl Value {
    fn to_sql(&self) -> &dyn rusqlite::ToSql {
        match self {
            Value::Text(s) => s,
            Value::Blob(b) => b,
            Value::Real(r) => r,
            Value::Integer(i) => i,
            Value::Lit(s) => s,
            Value::Null => &rusqlite::types::Null,
        }
    }
}

impl rusqlite::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, ValueRef};
        match self {
            Value::Text(s) => Ok(s.as_bytes().into()),
            Value::Blob(b) => Ok(ToSqlOutput::Borrowed(b.as_slice().into())),
            Value::Real(r) => Ok(ToSqlOutput::Borrowed(ValueRef::Real(*r))),
            Value::Integer(i) => Ok(ToSqlOutput::Borrowed(ValueRef::Integer(*i))),
            Value::Lit(s) => Ok(ToSqlOutput::Borrowed(s.as_bytes().into())),
            Value::Null => Ok(ToSqlOutput::Borrowed(ValueRef::Null).into()),
        }
    }
}

impl From<rusqlite::types::ToSqlOutput<'_>> for Value {
    fn from(value: rusqlite::types::ToSqlOutput) -> Self {
        match value {
            rusqlite::types::ToSqlOutput::Borrowed(b) => match b {
                rusqlite::types::ValueRef::Null => Value::Null,
                rusqlite::types::ValueRef::Integer(i) => Value::Integer(i),
                rusqlite::types::ValueRef::Real(r) => Value::Real(r),
                rusqlite::types::ValueRef::Text(t) => Value::Text(
                    std::str::from_utf8(t)
                        .expect("Expected a utf8 encoded string")
                        .into(),
                ),
                rusqlite::types::ValueRef::Blob(b) => Value::Blob(b.into()),
            },
            rusqlite::types::ToSqlOutput::Owned(o) => match o {
                rusqlite::types::Value::Null => Value::Null,
                rusqlite::types::Value::Integer(i) => Value::Integer(i),
                rusqlite::types::Value::Real(val) => Value::Real(val),
                rusqlite::types::Value::Text(val) => Value::Text(val.into()),
                rusqlite::types::Value::Blob(val) => Value::Blob(val),
            },
            _ => unimplemented!(),
        }
    }
}

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

async fn rows<T: DeserializeOwned + Send + 'static>(
    connection: &tokio_rusqlite::Connection,
    sql: Sql,
) -> Result<Vec<T>, Error> {
    let results = connection
        .call(move |conn| {
            let Sql { clause, params } = sql;
            let params = params
                .iter()
                .map(|value| value.to_sql())
                .collect::<Vec<_>>();
            let mut stmt = conn.prepare_cached(&clause)?;
            let rows = stmt.query(&*params)?;
            let rows: Result<Vec<T>, serde_rusqlite::Error> =
                serde_rusqlite::from_rows::<T>(rows).collect();
            let rows = rows.expect("failed to deserialize rows");
            Ok(rows)
        })
        .await?;

    Ok(results)
}

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

pub fn asc(col: impl ToColumn) -> Sql {
    Sql {
        clause: format!("{} asc", col.to_column()).into(),
        params: vec![],
    }
}

pub fn desc(col: impl ToColumn) -> Sql {
    Sql {
        clause: format!("{} desc", col.to_column()).into(),
        params: vec![],
    }
}

impl Query {
    pub fn new(db: Database) -> Self {
        Self {
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
            order: None,
            db,
        }
    }

    pub fn select(mut self) -> Self {
        self.select = Some(format!("select *").into());

        self
    }

    pub fn from(mut self, table: impl Table) -> Self {
        self.from = Some(format!("from {}", table.table_name()).into());

        self
    }

    pub fn order(mut self, statements: Vec<Sql>) -> Self {
        let column_names: String = statements
            .iter()
            .map(|c| c.clause.clone())
            .collect::<Vec<_>>()
            .join(",");
        self.order = match self.order {
            Some(order) => Some(format!("{} {}", column_names, order).into()),
            None => Some(format!("order by {}", column_names).into()),
        };

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

    fn sql_statement(&self) -> Sql {
        Sql {
            clause: self.sql(),
            params: self.values.clone(),
        }
    }

    pub fn sql(&self) -> String {
        vec![
            self.select.clone(),
            self.from.clone(),
            self.insert_into.clone(),
            self.values_sql.clone(),
            self.update.clone(),
            self.set.clone(),
            self.delete.clone(),
            self.r#where.clone(),
            self.order.clone(),
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

    pub async fn all<T>(&self) -> Result<Vec<T>, Error>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let rows = rows(&self.db.connection, self.sql_statement()).await?;
        Ok(rows)
    }

    pub async fn first<T>(&self) -> Result<T, Error>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let row = rows::<T>(&self.db.connection, self.sql_statement())
            .await?
            .into_iter()
            .nth(0)
            .ok_or(Error::RowNotFound)?;
        Ok(row)
    }

    pub async fn prepare<T>(self, db: &Database) -> Result<Prep<T>, Error>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let prep = prepare::<T>(&db.connection, self.sql_statement()).await?;
        Ok(prep)
    }

    pub fn insert(mut self, table: impl Table) -> Self {
        self.insert_into = Some(format!("insert into {}", table.table_name()).into());
        self
    }

    fn row_to_named_params(row: impl Serialize) -> Result<Vec<(Arc<str>, Value)>, Error> {
        let named_params = serde_rusqlite::to_params_named(row)?;
        named_params
            .iter()
            .map(|(name, value)| -> Result<(Arc<str>, Value), Error> {
                Ok((
                    name.replacen(":", "", 1).as_str().into(),
                    value.to_sql()?.into(),
                ))
            })
            .collect()
    }

    pub fn values(mut self, row: impl Serialize) -> Result<Self, Error> {
        let named_params = Self::row_to_named_params(row)?;
        let column_names = named_params
            .iter()
            .map(|(name, _)| name.to_string())
            .collect::<Vec<_>>();
        let placeholders = named_params
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let values = named_params
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        self.insert_into = match &self.insert_into {
            Some(sql) => Some(format!("{} ({})", sql, column_names.join(",")).into()),
            None => {
                return Err(Error::Sql(
                    "no table name found when calling values. Try calling insert() first".into(),
                ))
            }
        };
        self.values = values;
        self.values_sql = Some(format!("values ({})", placeholders).into());

        Ok(self)
    }

    pub fn update(mut self, table: impl Table) -> Self {
        self.update = Some(format!("update {}", table.table_name()).into());
        self
    }

    pub fn set(mut self, row: impl Serialize) -> Result<Self, Error> {
        let named_params = Self::row_to_named_params(row)?;
        let set = named_params
            .iter()
            .map(|(name, _)| format!("{} = ?", name))
            .collect::<Vec<_>>()
            .join(",");
        self.set = Some(format!("set {}", set).into());
        self.values = named_params
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();

        Ok(self)
    }

    pub fn delete(mut self, table: impl Table) -> Self {
        self.delete = Some(format!("delete from {}", table.table_name()).into());
        self
    }

    pub async fn returning<T: Serialize + DeserializeOwned + Send + Sync + 'static>(
        mut self,
    ) -> Result<T, Error> {
        self.returning = Some("returning *".into());
        let rows = rows::<T>(&self.db.connection, self.sql_statement()).await?;
        if let Some(row) = rows.into_iter().nth(0) {
            Ok(row)
        } else {
            Err(Error::InsertError(format!(
                "failed to insert {}",
                self.sql_statement().clause
            )))
        }
    }

    pub async fn rows_affected(&self) -> Result<usize, Error> {
        let rows_affected = execute(&self.db.connection, self.sql_statement()).await?;
        Ok(rows_affected)
    }

    pub fn select_with(mut self, sql: Sql) -> Query {
        self.select = Some(format!("select {}", sql.clause).into());
        self
    }

    pub fn count(mut self) -> Query {
        self.select = Some(format!("select count(*) as count").into());
        self
    }
}

pub fn placeholder() -> &'static str {
    "?"
}

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct Prep<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    connection: tokio_rusqlite::Connection,
    sql: Sql,
    phantom: PhantomData<T>,
}

#[allow(unused)]
impl<T> Prep<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    pub async fn all(&self, params: Vec<Value>) -> Result<Vec<T>, Error> {
        let sql = Sql {
            clause: self.sql.clause.clone(),
            params,
        };
        let rows = rows::<T>(&self.connection, sql).await?;
        Ok(rows)
    }

    pub async fn one(&self, params: Vec<Value>) -> Result<T, Error> {
        let sql = Sql {
            clause: self.sql.clause.clone(),
            params,
        };
        let rows = rows::<T>(&self.connection, sql).await?;
        rows.into_iter().nth(0).ok_or(Error::RowNotFound)
    }
}

#[derive(Clone, Debug)]
pub struct Query {
    db: Database,
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
    order: Option<Arc<str>>,
}

#[derive(Clone, Copy)]
pub struct Text(pub &'static str);

#[derive(Clone, Copy)]
pub struct Blob(pub &'static str);

#[derive(Clone, Copy)]
pub struct Integer(pub &'static str);

#[derive(Clone, Copy)]
pub struct Real(pub &'static str);

pub type Index = &'static str;

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

pub fn r#in(left: impl ToColumn, right: Vec<impl Into<Value>>) -> Sql {
    Sql {
        clause: format!(
            "{} in ({})",
            left.to_column(),
            right.iter().map(|_| "?").collect::<Vec<&str>>().join(",")
        ),
        params: right
            .into_iter()
            .map(|val| val.into())
            .collect::<Vec<Value>>(),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Lit(&'static str),
    Text(std::sync::Arc<str>),
    Blob(Vec<u8>),
    Real(f64),
    Integer(i64),
    Null,
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Text(value.into())
    }
}

impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Value::Text(value.clone().into())
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

impl From<Option<u16>> for Value {
    fn from(value: Option<u16>) -> Self {
        match value {
            Some(val) => Value::Integer(val.into()),
            None => Value::Null,
        }
    }
}

pub trait Table {
    fn new() -> Self
    where
        Self: Sized + Clone;
    fn table_name(&self) -> &'static str;
    fn create_table_sql(&self) -> &'static str;
    fn add_column_sql(&self, column_name: &str) -> String;
    fn create_index_sql(&self, unique: bool, column_names: Vec<&str>) -> String;
    fn drop_index_sql(&self, column_names: Vec<&str>) -> String;
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
    #[error("error converting value {0}")]
    SqlConversion(String),
    #[error("error building sql {0}")]
    Sql(String),
    #[error("could not find the row")]
    RowNotFound,
}

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

impl From<serde_rusqlite::Error> for Error {
    fn from(value: serde_rusqlite::Error) -> Self {
        match value {
            serde_rusqlite::Error::Unsupported(_) => todo!(),
            serde_rusqlite::Error::ValueTooLarge(_) => todo!(),
            serde_rusqlite::Error::Serialization(_) => todo!(),
            serde_rusqlite::Error::Deserialization {
                column: _,
                message: _,
            } => todo!(),
            serde_rusqlite::Error::Rusqlite(_) => todo!(),
            serde_rusqlite::Error::ColumnNamesNotAvailable => todo!(),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        match value {
            rusqlite::Error::ToSqlConversionFailure(err) => Self::SqlConversion(err.to_string()),
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
    pub clause: String,
    pub params: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub tables: Vec<(&'static str, &'static str)>,
    pub indices: Vec<(&'static str, &'static str)>,
}

#[cfg(test)]
mod tests {
    use rizz::{and, eq, or, Error, Integer, Sql, Table, Value};
    use serde::{Deserialize, Serialize};

    use crate::{connection, desc, Real, Text};

    type TestResult<T> = Result<T, Error>;

    #[test]
    fn sql_macro_with_text_param_works() {
        let accounts = Accounts::new();

        let sql = sql!("select * from {} where {} = {}", accounts, accounts.id, "1");

        assert_eq!(
            sql.clause.to_string(),
            r#"select * from "accounts" where "accounts"."id" = ?"#
        );
        assert_eq!(sql.params, vec![Value::Text("1".into())]);
    }

    #[test]
    fn sql_macro_with_integer_param_works() {
        let accounts = Accounts::new();

        let sql: Sql = sql!("select * from {} where {} = {}", accounts, accounts.id, 1);
        assert_eq!(
            sql.clause.to_string(),
            r#"select * from "accounts" where "accounts"."id" = ?"#
        );
        assert_eq!(sql.params, vec![Value::Integer(1)]);
    }

    #[test]
    fn sql_macro_with_real_param_works() {
        let accounts = Accounts::new();

        let sql: Sql = sql!("select * from {} where {} = {}", accounts, accounts.id, 1.0);
        assert_eq!(
            sql.clause.to_string(),
            r#"select * from "accounts" where "accounts"."id" = ?"#
        );
        assert_eq!(sql.params, vec![Value::Real(1.0)]);
    }

    #[test]
    fn sql_macro_with_byte_param_works() {
        let accounts = Accounts::new();

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
    }

    #[tokio::test]
    async fn where_clauses_work() -> TestResult<()> {
        let db = connection(":memory:").open().await?.database();
        let accounts = Accounts::new();

        let query = db.select().from(accounts).r#where(or(
            and(eq(accounts.id, "1"), eq(accounts.id, "1".to_owned())),
            eq(accounts.id, 1),
        ));

        let query2 = db.select().from(accounts).r#where(sql!(
            "(({} = {} and {} = {}) or {} = {})",
            accounts.id,
            "1",
            accounts.id,
            "1",
            accounts.id,
            1
        ));

        assert_eq!(query.sql(), query2.sql());

        assert_eq!(
            query.sql(),
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
        let db = connection(":memory:").open().await?.database();
        let accounts = Accounts::new();
        let migrations = db.create_table(accounts).migrate().await?;
        dbg!(&migrations);
        let account = Account {
            id: 1,
            name: "inserted".into(),
        };
        let insert_query = db.insert(accounts).values(account)?;
        assert_eq!(
            "insert into \"accounts\" (id,name) values (?,?)",
            insert_query.sql()
        );
        let mut inserted: Account = insert_query.returning().await?;

        assert_eq!(inserted.id, 1);
        assert_eq!(inserted.name, "inserted");

        inserted.name = "updated".into();
        let update_query = db
            .update(accounts)
            .set(inserted)?
            .r#where(eq(accounts.id, 1));

        assert_eq!(
            update_query.sql(),
            "update \"accounts\" set id = ?,name = ? where \"accounts\".\"id\" = ?"
        );

        let updated: Account = update_query.returning().await?;

        assert_eq!(updated.id, 1);
        assert_eq!(updated.name, "updated");

        let rows: Vec<Account> = db
            .select()
            .from(accounts)
            .r#where(eq(accounts.id, 1))
            .order(vec![desc(accounts.id), desc(accounts.name)])
            .all()
            .await?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows.iter().nth(0).unwrap().id, 1);
        assert_eq!(rows.iter().nth(0).unwrap().name, "updated");

        let rows_affected = db
            .delete_from(accounts)
            .r#where(eq(accounts.id, 1))
            .rows_affected()
            .await?;

        assert_eq!(rows_affected, 1);

        let RowCount { count } = db
            .count()
            .from(accounts)
            .r#where(eq(accounts.id, 1))
            .first()
            .await?;

        assert_eq!(count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn migrate_works() -> TestResult<()> {
        #[allow(unused)]
        #[derive(Table, Clone, Copy)]
        #[rizz(table = "todos")]
        struct Todos {
            #[rizz(primary_key)]
            id: Integer,
            #[rizz(not_null)]
            created_at: Real,
        }

        let connection = connection(":memory:").open().await?;
        let db = connection.database();
        let todos = Todos::new();
        let migrations = db
            .create_table(todos)
            .create_index(todos, vec![todos.created_at]);
        let _ = migrations.migrate().await?;
        let schema = db.schema().await?;
        let rows = vec![
            "CREATE TABLE \"migrations\" (sql text not null)",
            "CREATE UNIQUE INDEX migrations_sql on migrations (sql)",
            "CREATE TABLE \"todos\" (id integer primary key,created_at real not null)",
            "CREATE INDEX todos_created_at on todos (created_at)",
        ];
        assert_eq!(schema, rows.join("\n"));

        #[allow(unused)]
        #[derive(Table, Clone, Copy)]
        #[rizz(table = "todos")]
        struct Todos1 {
            #[rizz(primary_key)]
            id: Integer,
            #[rizz(not_null)]
            created_at: Integer,
            #[rizz(not_null, r#default = "0")]
            updated_at: Integer,
        }

        let todos = Todos1::new();
        let migrations = migrations
            .clone()
            .add_column(todos, todos.updated_at)
            .drop_index(todos, vec![todos.created_at]);

        let _ = migrations.migrate().await?;

        let schema = db.schema().await?;
        let statements = vec![
            "CREATE TABLE \"migrations\" (sql text not null)",
            "CREATE UNIQUE INDEX migrations_sql on migrations (sql)",
            "CREATE TABLE \"todos\" (id integer primary key,created_at real not null, updated_at integer not null default (0))"
        ];
        assert_eq!(schema, statements.join("\n"));

        let _ = migrations
            .clone()
            .create_unique_index(todos, vec![todos.created_at])
            .migrate()
            .await?;

        let schema = db.schema().await?;
        let statements = vec![
            "CREATE TABLE \"migrations\" (sql text not null)",
            "CREATE UNIQUE INDEX migrations_sql on migrations (sql)",
            "CREATE TABLE \"todos\" (id integer primary key,created_at real not null, updated_at integer not null default (0))",
            "CREATE UNIQUE INDEX todos_created_at on todos (created_at)"
        ];
        assert_eq!(schema, statements.join("\n"));

        Ok(())
    }

    #[derive(Deserialize, PartialEq, Debug)]
    struct RowCount {
        count: u64,
    }

    #[derive(Serialize, Deserialize)]
    struct Account {
        id: u64,
        name: String,
    }

    #[allow(unused)]
    #[derive(Table, Clone, Copy)]
    #[rizz(table = "accounts")]
    struct Accounts {
        #[rizz(primary_key)]
        id: Integer,
        #[rizz(not_null)]
        name: Text,
    }
}
