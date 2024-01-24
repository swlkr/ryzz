//! Rizz is a query builder and migration generator for sqlite, don't call it an orm.
//!
extern crate self as rizz_db;
pub use rizz_db_macros::{database, row, table, Row, Table};
use rusqlite::OpenFlags;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Display, sync::Arc};
pub use tokio_rusqlite;

#[derive(Clone, Debug)]
pub struct Connection {
    path: std::rc::Rc<str>,
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
        Self::new(path)
            .foreign_keys(true)
            .journal_mode(JournalMode::Wal)
            .synchronous(Synchronous::Normal)
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

#[table("sqlite_schema")]
pub struct SqliteSchema {
    pub name: Text,
    #[rizz(name = "type")]
    pub r#type: Text,
}

#[table("pragma_table_info(sqlite_schema.name)")]
#[rizz(r#as = "pti")]
pub struct TableInfo {
    pub name: Text,
}

#[row]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName {
    pub name: String,
}

#[row]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ColumnName {
    pub name: String,
}

#[row]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TableInfoRow {
    table_name: TableName,
    column_name: ColumnName,
}

#[allow(unused)]
impl TableInfoRow {
    pub fn table(&self) -> String {
        self.table_name.name.clone()
    }

    pub fn column(&self) -> String {
        self.column_name.name.clone()
    }
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

impl<A> Select for A
where
    A: ToColumn,
{
    fn columns(&self) -> Vec<&'static str> {
        vec![self.to_column()]
    }

    fn clause(&self) -> SelectClause {
        let tbl = Tbl {
            table_name: None,
            column_names: self.columns(),
        };

        SelectClause::Sql(format!("select {}", json_object(&tbl, true)))
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

fn unqualify(s: &str) -> String {
    s.split(".").nth(1).unwrap_or(s).to_string()
}

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
            conn.prepare(&sql.clause)?.execute(&*params)
        })
        .await?;

    Ok(results)
}

pub async fn rows<T: DeserializeOwned + Send + 'static>(
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
            let rows = stmt.query(&*params)?;
            // HACK there is only ever one column, it should always be valid json
            rows.mapped(|row| {
                let x: String = row.get(0)?;
                let y = serde_json::from_str::<T>(&x).unwrap();
                Ok(y)
            })
            .collect::<Result<Vec<_>, rusqlite::Error>>()
        })
        .await?;

    Ok(results)
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
                let c = unqualify(&col);
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

pub enum JoinType {
    Left,
    Right,
    Full,
    Cross,
    Inner,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match &self {
            JoinType::Left => "left",
            JoinType::Right => "right",
            JoinType::Full => "full",
            JoinType::Cross => "cross",
            JoinType::Inner => "inner",
        })
    }
}

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
    joins: Option<String>,
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
            joins: None,
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

    pub fn join(mut self, join_type: JoinType, outer: bool, table: &impl Table, sql: Sql) -> Self {
        let clause = format!(
            "{} {} join {} {} on {}",
            join_type,
            if outer { "outer" } else { "" },
            if let Some(alias) = table.table_alias() {
                alias
            } else {
                table.table_name()
            },
            if let Some(_) = table.table_alias() {
                table.table_name()
            } else {
                ""
            },
            sql.clause
        );
        match self.joins {
            Some(ref mut joins) => joins.push_str(&clause),
            None => self.joins = Some(clause.into()),
        }
        self.tables.push(Tbl {
            table_name: Some(table.table_name()),
            column_names: table.column_names(),
        });

        self
    }

    pub fn inner_join(self, table: &impl Table, sql: Sql) -> Self {
        self.join(JoinType::Inner, false, table, sql)
    }

    pub fn left_outer_join(self, table: &impl Table, sql: Sql) -> Self {
        self.join(JoinType::Left, true, table, sql)
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

    pub fn limit(mut self, limit: i64) -> Self {
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
        let inner_joins: Option<Arc<str>> = if let Some(inner_joins) = &self.joins {
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

    pub async fn prepare<T: Row + DeserializeOwned + Send + Sync + 'static>(
        self,
    ) -> Result<Self, Error> {
        let sql = self.sql_statement::<T>();
        self.connection
            .call(move |conn| {
                // this uses an internal Lru cache within rusqlite
                // and uses the sql as the key to the cache
                // not ideal but what can you do?
                let _ = conn.prepare_cached(&sql.clause)?;

                Ok(())
            })
            .await?;

        Ok(self)
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
            .map(|(_, value)| Value::from(value))
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
    let value = right.into();
    let op = match value {
        Value::Null => "is",
        _ => "=",
    };
    Sql {
        clause: format!("{} {} {}", left.to_column(), op, value.to_column()),
        params: params_from_value(value),
    }
}

impl ToColumn for Value {
    fn to_column(&self) -> &'static str {
        match self {
            Value::Lit(x) => x,
            Value::Null => "null",
            _ => "?",
        }
    }
}

fn params_from_value(value: Value) -> Vec<Value> {
    match value {
        Value::Lit(_) | Value::Null => vec![],
        _ => vec![value],
    }
}

pub fn ne(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    let value: Value = right.into();
    let op = match value {
        Value::Null => "is not",
        _ => "!=",
    };
    Sql {
        clause: format!("{} {} {}", left.to_column(), op, value.to_column()),
        params: params_from_value(value),
    }
}

pub fn gt(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} > ?", left.to_column()),
        params: params_from_value(right.into()),
    }
}

pub fn lt(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} < ?", left.to_column()),
        params: params_from_value(right.into()),
    }
}

pub fn gte(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} >= ?", left.to_column()),
        params: params_from_value(right.into()),
    }
}

pub fn lte(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} <= ?", left.to_column()),
        params: params_from_value(right.into()),
    }
}

pub fn like(left: impl ToColumn, right: impl Into<Value>) -> Sql {
    Sql {
        clause: format!("{} like ?", left.to_column()),
        params: params_from_value(right.into()),
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

impl From<Text> for Value {
    fn from(value: Text) -> Self {
        Value::Lit(value.0)
    }
}

impl From<Integer> for Value {
    fn from(value: Integer) -> Self {
        Value::Lit(value.0)
    }
}

impl From<Real> for Value {
    fn from(value: Real) -> Self {
        Value::Lit(value.0)
    }
}

impl From<Blob> for Value {
    fn from(value: Blob) -> Self {
        Value::Lit(value.0)
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
        Self: Sized + Clone + Send + Sync;
    fn table_name(&self) -> &'static str;
    fn table_alias(&self) -> Option<&'static str>;
    fn struct_name(&self) -> &'static str;
    fn column_names(&self) -> Vec<&'static str>;
    fn create_table_sql(&self) -> &'static str;
    fn add_column_sql(&self, column_name: &str) -> String;
}

pub trait Row
where
    Self: DeserializeOwned + Send + Sync,
{
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

pub struct Sql {
    pub clause: String,
    pub params: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub tables: Vec<(&'static str, &'static str)>,
    pub indices: Vec<(&'static str, &'static str)>,
}

pub struct Index<'a> {
    unique: bool,
    name: &'a str,
    table: &'a str,
    columns: String,
}

impl<'a> Index<'a> {
    fn new(name: &'a str) -> Self {
        Self {
            unique: false,
            name,
            table: "",
            columns: "".into(),
        }
    }

    pub fn to_create_sql(&self) -> String {
        format!(
            "create {}index if not exists {} on {} ({});",
            if self.unique { "unique " } else { "" },
            self.name,
            self.table,
            self.columns
        )
    }

    pub fn to_drop_sql(&self) -> String {
        format!("drop index if exists {};", self.name)
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;

        self
    }

    pub fn on(mut self, table: &'a impl Table, columns: impl Select) -> Self {
        self.columns = columns
            .columns()
            .into_iter()
            .map(unqualify)
            .collect::<Vec<_>>()
            .join(",");
        self.table = table.table_name();

        self
    }
}

pub fn index<'a>(name: &'a str) -> Index<'a> {
    Index::new(name)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[allow(unused)]
    async fn readme_works() -> Result<(), rizz_db::Error> {
        use rizz_db::*;

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
            post_id: u64,
        }

        let db = Database::new(":memory:").await?;

        let Database { posts, comments } = &db;

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
            .inner_join(posts, eq(posts.id, comments.post_id))
            .all()
            .await?;

        assert_eq!(rows[0].comment, new_comment);
        assert_eq!(rows[0].post, new_post);

        let query = db.select(()).from(comments);

        // prepare the query
        let prepared = query.prepare::<Comment>().await?;

        // execute the prepared query later
        let rows: Vec<Comment> = prepared.all().await?;

        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[0].body, "");
        assert_eq!(rows[0].post_id, 1);

        Ok(())
    }

    #[allow(unused)]
    #[tokio::test]
    async fn indexes_works() -> Result<(), rizz_db::Error> {
        use rizz_db::*;

        #[database]
        struct Database {
            links: Links,
        }

        #[table("links")]
        struct Links {
            #[rizz(primary_key)]
            id: Integer,

            #[rizz(not_null)]
            url: Text,
        }

        #[row]
        struct Link {
            id: i64,
            url: String,
        }

        let db = Database::new(":memory:").await?;
        let Database { links } = &db;

        let links_url_ix = index("links_url_ix").unique().on(links, links.url);

        db.create(&links_url_ix).await?;

        let rows = db
            .insert(links)
            .values(Link {
                id: 1,
                url: "".into(),
            })?
            .rows_affected()
            .await?;

        assert_eq!(rows, 1);

        let result = db
            .insert(links)
            .values(Link {
                id: 2,
                url: "".into(),
            })?
            .rows_affected()
            .await;

        assert!(result.is_err());

        db.drop(&links_url_ix).await?;

        let result = db
            .insert(links)
            .values(Link {
                id: 2,
                url: "".into(),
            })?
            .rows_affected()
            .await;

        assert!(result.is_ok());

        let rows = db.select(()).from(links).all::<Link>().await?;

        assert_eq!(rows.len(), 2);

        Ok(())
    }

    #[allow(unused)]
    #[tokio::test]
    async fn migrate_works() -> Result<(), rizz_db::Error> {
        use rizz_db::*;

        let sqlite_file = "test.sqlite3";

        std::fs::remove_file(sqlite_file).unwrap_or_default();

        let conn = Connection::default(sqlite_file);

        {
            #[database]
            struct Database {
                links: Links,
            }

            #[table("links")]
            struct Links {
                #[rizz(primary_key)]
                id: Integer,

                #[rizz(not_null)]
                url: Text,
            }

            let db = Database::connect(conn.clone()).await?;

            let changes = db.migrate().await?;

            assert_eq!(changes, 1);

            let changes = db.migrate().await?;

            assert_eq!(changes, 0);
        }

        {
            #[database]
            struct Database {
                links: Links,
            }

            #[table("links")]
            struct Links {
                #[rizz(primary_key)]
                id: Integer,

                #[rizz(not_null)]
                url: Text,

                test: Text,
            }

            let db = Database::connect(conn).await?;

            let changes = db.migrate().await?;

            assert_eq!(changes, 1);

            let changes = db.migrate().await?;

            assert_eq!(changes, 0);
        }

        Ok(())
    }
}
