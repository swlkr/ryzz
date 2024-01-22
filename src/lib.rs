//! Rizz is a query builder and migration generator for sqlite, don't call it an orm.
//!
extern crate self as rizz;

pub use rizz_db_macros::{database, row, table, Row, Table};
use rusqlite::OpenFlags;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
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

#[derive(Clone, Debug)]
pub struct Connection {
    path: Arc<str>,
    open_flags: OpenFlags,
    pragma: Option<String>,
}

impl Connection {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.into(),
            open_flags: OpenFlags::default(),
            pragma: None,
        }
    }

    pub fn default(path: &str) -> Self {
        let c = Self::new(path)
            .foreign_keys(true)
            .journal_mode(JournalMode::Wal)
            .synchronous(Synchronous::Normal);

        c
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
        let statement = format!("PRAGMA foreign_keys = {};", val);
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

    pub async fn open(&self) -> Result<tokio_rusqlite::Connection, Error> {
        let conn = tokio_rusqlite::Connection::open_with_flags(self.path.as_ref(), self.open_flags)
            .await?;
        if let Some(p) = self.pragma.clone() {
            let _ = conn.call(move |conn| conn.execute_batch(&p)).await?;
        }

        Ok(conn)
    }
}

#[row]
struct TableName {
    name: String,
}

#[table("sqlite_schema")]
struct SqliteSchema {
    name: Text,
    #[rizz(name = "type")]
    r#type: Text,
}

trait Based
where
    Self: Sized,
{
    fn tables() -> Vec<Box<dyn Table + 'static>>;
    fn connection(&self) -> &tokio_rusqlite::Connection;

    async fn new(connection: Connection) -> Result<Self, Error>;

    async fn migrate(&self) -> Result<usize, rizz::Error> {
        let sqlite_schema = SqliteSchema::new();

        let table_names: Vec<TableName> = self
            .select(())
            .from(&sqlite_schema)
            .r#where(eq(sqlite_schema.r#type, "table"))
            .all::<TableName>()
            .await?;

        let db_table_names = table_names
            .iter()
            .map(|t| t.name.as_str())
            .collect::<HashSet<_>>();

        let tables = Self::tables();
        let table_names = tables
            .iter()
            .map(|t| t.table_name())
            .collect::<HashSet<_>>();

        let tables_to_create = table_names
            .difference(&db_table_names)
            .flat_map(|x| {
                tables
                    .iter()
                    .filter(|t| t.table_name() == *x)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        if !tables_to_create.is_empty() {
            println!("=== Creating tables ===");
            let statements = tables_to_create
                .iter()
                .map(|x| x.create_table_sql())
                .collect::<Vec<_>>();
            let _ = self
                .execute_batch(&format!("BEGIN;{}COMMIT;", statements.join(";")))
                .await?;
            for statement in statements {
                println!("{}", statement);
            }
            println!("=== Create tables finished successfully ===");
        }

        let tables_to_drop = db_table_names
            .difference(&table_names)
            .flat_map(|x| {
                tables
                    .iter()
                    .filter(|t| t.table_name() == *x)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if !tables_to_drop.is_empty() {
            println!("=== Drop tables ===");
            tables_to_drop
                .iter()
                .map(|t| t.drop_table_sql())
                .collect::<Vec<_>>()
                .join(";");
            println!("=== Drop tables finished successfully ===");
        }

        Ok(tables_to_create.len() + tables_to_drop.len())
    }

    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let sql: std::sync::Arc<str> = sql.into();
        self.connection()
            .call(move |conn| conn.execute_batch(&sql))
            .await?;

        Ok(())
    }

    async fn query<T: DeserializeOwned + Send + 'static>(&self, sql: Sql) -> Result<Vec<T>, Error> {
        rows::<T>(&self.connection(), sql).await
    }

    fn select(&self, columns: impl Select) -> Query {
        Query::new(&self.connection()).select(columns)
    }

    fn insert(&self, table: &impl Table) -> Query {
        Query::new(&self.connection()).insert(table)
    }

    fn delete_from<'a>(&'a self, table: &'a dyn Table) -> Query<'a> {
        Query::new(&self.connection()).delete(table)
    }

    fn update<'a>(&'a self, table: &'a dyn Table) -> Query<'a> {
        Query::new(&self.connection()).update(table)
    }

    // fn count(&self) -> Query {
    //     Query::new(self.connection()).count()
    // }
}

pub trait Select {
    fn columns(&self) -> Vec<&'static str>;
    fn clause(&self) -> SelectClause;
}

impl Select for () {
    fn columns(&self) -> Vec<&'static str> {
        vec![]
    }

    fn clause(&self) -> SelectClause {
        SelectClause::All
    }
}

impl<A, B> Select for (A, B)
where
    A: ToColumn,
    B: ToColumn,
{
    fn columns(&self) -> Vec<&'static str> {
        vec![self.0.to_column(), self.1.to_column()]
    }

    fn clause(&self) -> SelectClause {
        let tbl = Tbl {
            table_name: None,
            column_names: self.columns(),
        };

        SelectClause::Sql(format!("select {}", json_object(&tbl, true)))
    }
}
// #[derive(serde::Serialize, serde::Deserialize)]
// struct SchemaRow {
//     sql: String,
// }

// #[derive(Clone, Debug)]
// pub struct Migrator {
//     statements: Vec<Arc<str>>,
//     quiet_migrations: bool,
// }

// impl Migrator {
//     fn new(db: Database) -> Self {
//         let mut migrator = Migrator {
//             db,
//             statements: vec![],
//             quiet_migrations: false,
//         };
//         let migrations = Migrations::new();
//         migrator = migrator
//             .create_table(migrations)
//             .create_unique_index(migrations, vec![migrations.sql]);
//         migrator
//     }

//     pub fn create_table(mut self, table: impl Table) -> Self {
//         self.statements.push(table.create_table_sql().into());
//         self
//     }

//     pub fn add_column(mut self, table: impl Table, column: impl ToColumn) -> Self {
//         self.statements
//             .push(table.add_column_sql(column.to_column()).into());
//         self
//     }

//     pub fn create_unique_index(mut self, table: impl Table, columns: Vec<impl ToColumn>) -> Self {
//         let column_names: Vec<&str> = columns.iter().map(|c| c.to_column()).collect();
//         self.statements
//             .push(table.create_index_sql(true, column_names).into());
//         self
//     }

//     pub fn create_index(mut self, table: impl Table, columns: Vec<impl ToColumn>) -> Self {
//         let column_names: Vec<&str> = columns.iter().map(|c| c.to_column()).collect();
//         self.statements
//             .push(table.create_index_sql(false, column_names).into());
//         self
//     }

//     pub fn drop_index(mut self, table: impl Table, columns: Vec<impl ToColumn>) -> Self {
//         let column_names: Vec<&str> = columns.iter().map(|c| c.to_column()).collect();
//         self.statements
//             .push(table.drop_index_sql(column_names).into());
//         self
//     }

//     async fn migrations(&self) -> Result<Vec<Migration>, Error> {
//         let migrations = Migrations::new();
//         let rows: Vec<Migration> = self.db.select().from(&migrations).all().await?;
//         Ok(rows)
//     }

//     async fn insert_migration(&self, migration: Migration) -> Result<(), Error> {
//         let migrations = Migrations::new();
//         let _row: Migration = self
//             .db
//             .insert(&migrations)
//             .values(migration)?
//             .returning()
//             .await?;
//         Ok(())
//     }

//     pub fn quiet_migrations(mut self) -> Self {
//         self.quiet_migrations = true;
//         self
//     }

//     pub async fn migrate(&self) -> Result<(), Error> {
//         let applied: Vec<Arc<str>> = self
//             .migrations()
//             .await
//             .unwrap_or_default()
//             .iter()
//             .map(|x| x.sql.clone().into())
//             .collect();
//         let statements: Vec<&Arc<str>> = self
//             .statements
//             .iter()
//             .filter(|sql| !applied.contains(sql))
//             .collect();

//         if statements.len() > 0 && !self.quiet_migrations {
//             println!("=== Running migrations ===");
//         }

//         for sql in &statements {
//             if !self.quiet_migrations {
//                 println!("{sql}");
//             }
//             let _ = self.db.execute_batch(sql).await?;
//             let _x = self
//                 .insert_migration(Migration {
//                     sql: sql.to_string(),
//                 })
//                 .await?;
//         }

//         if statements.len() > 0 && !self.quiet_migrations {
//             println!("=== Migrations finished successfully ===");
//         } else if !self.quiet_migrations {
//             println!("=== No pending migrations ===");
//         }

//         Ok(())
//     }
// }

#[table("rizz_migrations")]
struct Migrations {
    #[rizz(not_null, unique)]
    sql: Text,
}

#[row]
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
            let mut stmt = conn.prepare(&clause)?;
            // let columns = columns_from_statement(&stmt);
            let rows = stmt.query(&*params)?;
            // HACK there is only ever one column, it should always be valid json
            let rows = rows
                .mapped(|row| {
                    let x: String = row.get(0)?;
                    let y = serde_json::from_str::<T>(&x).unwrap();
                    Ok(y)
                })
                .collect::<Result<Vec<_>, rusqlite::Error>>();
            // let rows: Result<Vec<T>, serde_rusqlite::Error> =
            //     serde_rusqlite::from_rows::<T>(rows).collect();
            // let rows = rows.expect("failed to deserialize rows");
            Ok(rows)
        })
        .await??;

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

#[derive(Clone, Debug)]
struct Tbl<'a> {
    table_name: Option<&'a str>,
    column_names: Vec<&'static str>,
}

fn column_name(table_name: Option<&str>, column_name: &str) -> String {
    match table_name {
        Some(t) => format!("{}.{}", t, column_name),
        None => format!("{}", column_name),
    }
}

fn json_object(tbl: &Tbl, r#as: bool) -> String {
    let r#as = match tbl.table_name {
        Some(s) => {
            if r#as {
                format!("as {}", s)
            } else {
                "".into()
            }
        }
        None => "".into(),
    };
    format!(
        "json_object({}) {}",
        tbl.column_names
            .iter()
            .map(|col| {
                // HACK Stop qualifying column names in proc macro
                let c = col.split(".").nth(1).unwrap_or(col).replace("\"", "");
                format!(r#"'{}', {}"#, c, column_name(tbl.table_name, col))
            })
            .collect::<Vec<_>>()
            .join(","),
        r#as
    )
}

impl<'a> From<&'a dyn Table> for Tbl<'a> {
    fn from(value: &'a dyn Table) -> Self {
        Tbl {
            table_name: Some(value.table_name()),
            column_names: value.column_names(),
        }
    }
}

#[derive(Clone)]
pub enum SelectClause {
    All,
    Sql(String),
    None,
}

#[derive(Clone)]
pub struct Query<'a> {
    connection: &'a tokio_rusqlite::Connection,
    select: SelectClause,
    from: Option<Tbl<'a>>,
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
    group_by: Option<Arc<str>>,
    inner_joins: Option<String>,
    tables: Vec<Tbl<'a>>,
}

impl<'a> Query<'a> {
    pub fn new(connection: &'a tokio_rusqlite::Connection) -> Self {
        Self {
            select: SelectClause::None,
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
            group_by: None,
            inner_joins: None,
            tables: vec![],
            connection,
        }
    }

    pub fn select(mut self, columns: impl Select) -> Self {
        self.select = columns.clause();
        self
    }

    pub fn from(mut self, table: &impl Table) -> Self {
        let tbl = Tbl {
            table_name: Some(table.table_name()),
            column_names: table.column_names(),
        };
        self.from = Some(tbl);
        self.tables.push(Tbl {
            table_name: Some(table.table_name()),
            column_names: table.column_names(),
        });
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

    pub fn inner_join(mut self, table: &impl Table, sql: Sql) -> Self {
        match self.inner_joins {
            Some(ref mut inner_joins) => {
                inner_joins.push_str(&format!("inner join {} {}", table.table_name(), sql.clause))
            }
            None => {
                self.inner_joins =
                    Some(format!("inner join {} {}", table.table_name(), sql.clause).into())
            }
        }
        self.tables.push(Tbl {
            table_name: Some(table.table_name()),
            column_names: table.column_names(),
        });

        self
    }

    pub fn group_by(mut self, columns: Vec<impl ToColumn>) -> Self {
        let column_names = columns
            .iter()
            .map(|c| c.to_column())
            .collect::<Vec<_>>()
            .join(",");
        self.group_by = Some(format!("group by {}", column_names).into());
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(format!("limit {}", limit).into());
        self
    }

    fn sql_statement<T: Row>(&self) -> Sql {
        Sql {
            clause: self.sql::<T>(),
            params: self.values.clone(),
        }
    }

    pub fn sql<T: Row>(&self) -> String {
        let select = match &self.select {
            SelectClause::All => match &self.from {
                Some(tbl) => {
                    if self.tables.is_empty() {
                        Some(format!("select {}", json_object(&tbl, true)).into())
                    } else {
                        if self.tables.len() == 1 {
                            let tables = self
                                .tables
                                .iter()
                                .map(|tbl| json_object(tbl, true))
                                .collect::<Vec<_>>()
                                .join(",");
                            Some(format!("select {}", tables).into())
                        } else {
                            // TODO match up table names from self.tables
                            // TODO with field names from T struct
                            // TODO they should be in the same order?
                            let keys = T::column_names();
                            let x = keys
                                .iter()
                                .zip(&self.tables)
                                .map(|(x, y)| format!("'{}', {}", x, json_object(&y, false)))
                                .collect::<Vec<_>>()
                                .join(",");
                            Some(format!("select json_object({})", x).into())
                        }
                    }
                }
                None => None,
            },
            SelectClause::Sql(s) => Some(s.clone().into()),
            SelectClause::None => None,
        };
        let from: Option<Arc<str>> = match &self.from {
            Some(Tbl { table_name, .. }) => match table_name {
                Some(table_name) => Some(format!("from {}", table_name).into()),
                None => None,
            },
            _ => None,
        };
        let inner_joins: Option<Arc<str>> = if let Some(inner_joins) = &self.inner_joins {
            Some(inner_joins.clone().into())
        } else {
            None
        };

        vec![
            select,
            from,
            self.insert_into.clone(),
            self.values_sql.clone(),
            self.update.clone(),
            self.set.clone(),
            self.delete.clone(),
            inner_joins.clone(),
            self.r#where.clone(),
            self.group_by.clone(),
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

    pub async fn all<T>(self) -> Result<Vec<T>, Error>
    where
        T: Row + DeserializeOwned + Send + Sync + 'static,
    {
        let rows = rows(&self.connection, self.sql_statement::<T>()).await?;
        Ok(rows)
    }

    pub async fn first<T>(&self) -> Result<T, Error>
    where
        T: Row + DeserializeOwned + Send + Sync + 'static,
    {
        let row = rows::<T>(&self.connection, self.sql_statement::<T>())
            .await?
            .into_iter()
            .nth(0)
            .ok_or(Error::RowNotFound)?;
        Ok(row)
    }

    pub async fn prepare<T>(self, connection: tokio_rusqlite::Connection) -> Result<Prep<T>, Error>
    where
        T: Row + DeserializeOwned + Send + Sync + 'static,
    {
        let prep = prepare::<T>(&connection, self.sql_statement::<T>()).await?;
        Ok(prep)
    }

    pub fn insert(mut self, table: &impl Table) -> Self {
        self.insert_into = Some(format!("insert into {}", table.table_name()).into());
        self.tables.push(Tbl {
            table_name: Some(table.table_name()),
            column_names: table.column_names(),
        });
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

    pub fn update(mut self, table: &'a dyn Table) -> Self {
        self.update = Some(format!("update {}", table.table_name()).into());
        self.tables.push(table.into());
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

    pub fn delete(mut self, table: &'a dyn Table) -> Self {
        self.delete = Some(format!("delete from {}", table.table_name()).into());
        self.tables.push(table.into());
        self
    }

    pub async fn returning<T: Row + Serialize + DeserializeOwned + Send + Sync + 'static>(
        mut self,
    ) -> Result<T, Error> {
        let tables = self
            .tables
            .iter()
            .map(|tbl| json_object(tbl, true))
            .collect::<Vec<_>>()
            .join(",");
        self.returning = Some(format!("returning {}", tables).into());

        let rows = rows::<T>(&self.connection, self.sql_statement::<T>()).await?;
        if let Some(row) = rows.into_iter().nth(0) {
            Ok(row)
        } else {
            Err(Error::InsertError(format!(
                "failed to insert {}",
                self.sql_statement::<T>().clause
            )))
        }
    }

    pub async fn rows_affected(&self) -> Result<usize, Error> {
        let rows_affected = execute(&self.connection, self.sql_statement::<usize>()).await?;
        Ok(rows_affected)
    }

    // pub fn count(mut self) -> Query<'a> {
    //     self.select = Some(format!("select count(*) as count").into());
    //     self
    // }
}

impl Row for usize {
    fn column_names() -> Vec<&'static str> {
        vec![]
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

#[derive(Clone, Copy, Debug, Default)]
pub struct Text(pub &'static str);

#[derive(Clone, Copy, Debug, Default)]
pub struct Blob(pub &'static str);

#[derive(Clone, Copy, Debug, Default)]
pub struct Integer(pub &'static str);

#[derive(Clone, Copy, Debug, Default)]
pub struct Real(pub &'static str);

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

pub fn on(left: impl ToColumn, right: impl ToColumn) -> Sql {
    Sql {
        clause: format!("on {} = {}", left.to_column(), right.to_column()),
        params: vec![],
    }
}

pub fn gt(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} > ?", left.to_column()),
        params: vec![right.into()],
    }
}

pub fn lt(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} < ?", left.to_column()),
        params: vec![right.into()],
    }
}

pub fn gte(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} >= ?", left.to_column()),
        params: vec![right.into()],
    }
}

pub fn lte(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} <= ?", left.to_column()),
        params: vec![right.into()],
    }
}

pub fn like(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} like ?", left.to_column()),
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
    fn struct_name(&self) -> &'static str;
    fn column_names(&self) -> Vec<&'static str>;
    fn create_table_sql(&self) -> &'static str;
    fn drop_table_sql(&self) -> &'static str;
    fn add_column_sql(&self, column_name: &str) -> String;
    fn create_index_sql(&self, unique: bool, column_names: Vec<&str>) -> String;
    fn drop_index_sql(&self, column_names: Vec<&str>) -> String;
}

pub trait Row {
    fn column_names() -> Vec<&'static str>;
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database connection closed")]
    ConnectionClosed,
    #[error("database connection closing: {0}")]
    Close(String),
    #[error("unique constraint failed: {0}")]
    UniqueConstraint(String),
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
    #[error("could not deserialize rows {0}")]
    Deserialize(String),
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Deserialize(value.to_string())
    }
}

impl From<tokio_rusqlite::Error> for Error {
    fn from(value: tokio_rusqlite::Error) -> Self {
        match value {
            tokio_rusqlite::Error::ConnectionClosed => Self::ConnectionClosed,
            tokio_rusqlite::Error::Close((_, error)) => Self::Close(error.to_string()),
            tokio_rusqlite::Error::Rusqlite(err) => match err {
                rusqlite::Error::SqliteFailure(x, y) => match x.code {
                    rusqlite::ErrorCode::ConstraintViolation => match x.extended_code {
                        2067 => Self::UniqueConstraint(y.unwrap_or(x.to_string())),
                        _ => Self::Database(x.to_string()),
                    },
                    _ => Self::Database(x.to_string()),
                },
                _ => Self::Database(err.to_string()),
            },
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
            err => Self::Deserialize(err.to_string()),
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
    #[tokio::test]
    #[allow(unused)]
    async fn readme_works() -> Result<(), rizz::Error> {
        use rizz::*;

        #[database]
        struct Database {
            posts: Posts,
            comments: Comments,
        }

        #[table("posts")]
        struct Posts {
            #[rizz(primary_key)]
            id: Integer,

            #[rizz(not_null)]
            body: Text,
        }

        #[table("comments")]
        struct Comments {
            #[rizz(primary_key)]
            id: Integer,

            #[rizz(not_null)]
            body: Text,

            #[rizz(references = "posts(id)")]
            post_id: Integer,
        }

        #[row]
        #[derive(PartialEq, Clone)]
        struct Post {
            id: u64,
            body: String,
        }

        #[row]
        #[derive(PartialEq, Clone)]
        struct Comment {
            id: u64,
            body: String,
            #[serde(default)]
            post_id: u64,
        }

        let db = Database::new(Connection::default(":memory:")).await?;

        let Database {
            posts, comments, ..
        } = &db;

        let new_post = Post {
            id: 1,
            body: "".into(),
        };

        let mut inserted_post: Post = db
            .insert(posts)
            .values(new_post.clone())?
            .returning()
            .await?;

        assert_eq!(inserted_post, new_post);

        let updated_post: Post = db
            .update(posts)
            .set(Post {
                body: "post".into(),
                ..inserted_post
            })?
            .r#where(eq(posts.id, 1))
            .returning()
            .await?;

        inserted_post.body = "post".into();
        assert_eq!(updated_post, inserted_post);

        let deleted_post: Post = db
            .delete_from(posts)
            .r#where(eq(posts.id, 1))
            .returning()
            .await?;

        assert_eq!(deleted_post, inserted_post);

        let inserted_post: Post = db
            .insert(posts)
            .values(new_post.clone())?
            .returning()
            .await?;

        let new_comment = Comment {
            id: 1,
            body: "".into(),
            post_id: 1,
        };

        let comment: Comment = db
            .insert(comments)
            .values(new_comment.clone())?
            .returning()
            .await?;

        let rows: Vec<Comment> = db.select(()).from(comments).all().await?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows.into_iter().nth(0).unwrap(), new_comment);

        let query = db
            .select((comments.id, comments.body))
            .from(comments)
            .r#where(eq(comments.id, 1))
            .limit(1);

        let rows: Vec<Comment> = query.all().await?;

        assert_eq!(rows.len(), 1);

        let comment = rows.into_iter().nth(0).unwrap();
        assert_eq!(comment.id, new_comment.id);
        assert_eq!(comment.body, new_comment.body);
        assert_eq!(comment.post_id, 0);

        #[row]
        struct CommentWithPost {
            comment: Comment,
            post: Post,
        }

        let rows: Vec<CommentWithPost> = db
            .select(())
            .from(comments)
            .inner_join(posts, on(posts.id, comments.post_id))
            .all()
            .await?;

        assert_eq!(rows[0].comment, new_comment);
        assert_eq!(rows[0].post, new_post);

        Ok(())
    }
}
