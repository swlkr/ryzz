//! Ryzz is a query builder and migration generator for sqlite, don't call it an orm.
//!
extern crate self as ryzz;
pub use rusqlite;
pub use rusqlite::types::Value;
pub use rusqlite::ToSql;
use rusqlite::{params_from_iter, OpenFlags};
pub use ryzz_macros::{row, table, Row, Table};
use serde::{de::DeserializeOwned, Serialize};
use serde_rusqlite::NamedParamSlice;
use std::{fmt::Display, sync::Arc};
pub use tokio_rusqlite;

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
    pub name: String,
    #[ryzz(name = "type")]
    pub r#type: String,
    pub sql: String,
}

#[table("pragma_table_info(sqlite_schema.name)")]
#[ryzz(as_ = "pti")]
pub struct TableInfo {
    pub name: String,
}

#[row]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName {
    pub name: String,
}

#[row]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnName {
    pub name: String,
}

#[row]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableInfoRow {
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

macro_rules! impl_select {
    ($max:expr) => {
        seq_macro::seq!(N in 0..=$max {
            impl<#(T~N,)*> Select for (#(T~N,)*)
            where
                #(T~N: ToColumn,)*
            {
                fn columns(&self) -> Vec<&'static str> {
                    vec![#(self.N.to_column(),)*]
                }

                fn clause(&self) -> SelectClause {
                    let tbl = Tbl {
                        table_name: None,
                        column_names: self.columns(),
                    };

                    SelectClause::Sql(format!("select {}", json_object(&tbl, true)))
                }
            }
        });
    };
}

seq_macro::seq!(N in 0..16 {
    impl_select!(N);
});

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

fn unqualify(s: &str) -> String {
    s.split(".").nth(1).unwrap_or(s).to_string()
}

#[table("ryzz_migrations")]
struct Migration {
    #[ryzz(unique)]
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

async fn execute(connection: &tokio_rusqlite::Connection, sql: Sql) -> Result<usize, Error> {
    let params = params_from_iter(sql.params.into_iter());
    let clause = sql.clause;
    let results = connection
        .call(move |conn| conn.prepare(&clause)?.execute(params))
        .await?;

    Ok(results)
}

pub async fn rows<T: DeserializeOwned + Send + 'static>(
    connection: &tokio_rusqlite::Connection,
    sql: Sql,
) -> Result<Vec<T>, Error> {
    let params = params_from_iter(sql.params.into_iter());
    let clause = sql.clause;
    let results = connection
        .call(move |conn| {
            let mut stmt = conn.prepare(&clause)?;
            let rows = stmt.query(params)?;
            // HACK there is only ever one column, it should always be valid json
            rows.mapped(|row| {
                let json: String = row.get(0)?;
                Ok(json)
            })
            .collect::<Result<Vec<_>, rusqlite::Error>>()
        })
        .await?;

    results
        .into_iter()
        .map(|json| Ok(serde_json::from_str::<T>(&json)?))
        .collect::<Result<Vec<T>, Error>>()
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
    default_values: Option<Arc<str>>,
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
            default_values: None,
            connection,
        }
    }

    pub fn select(mut self, columns: impl Select) -> Self {
        self.select = columns.clause();
        self
    }

    pub fn from(mut self, table: impl Table) -> Self {
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

    #[deprecated(since = "0.1.0", note = "please use `where_` instead")]
    pub fn r#where(mut self, sql: Sql) -> Self {
        if let None = self.r#where {
            self.r#where = Some(format!("where {}", sql.clause).into())
        }
        self.values.extend(sql.params);
        self
    }

    pub fn where_(mut self, sql: Sql) -> Self {
        if let None = self.r#where {
            self.r#where = Some(format!("where {}", sql.clause).into())
        }
        self.values.extend(sql.params);
        self
    }

    pub fn join(mut self, join_type: JoinType, outer: bool, table: impl Table, sql: Sql) -> Self {
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

    pub fn inner_join(self, table: impl Table, sql: Sql) -> Self {
        self.join(JoinType::Inner, false, table, sql)
    }

    pub fn left_outer_join(self, table: impl Table, sql: Sql) -> Self {
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
            self.default_values.clone(),
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

    pub async fn prep<T: Row + DeserializeOwned + Send + Sync + 'static>(
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

    pub fn insert(mut self, table: impl Table) -> Self {
        self.insert_into = Some(format!("insert into {}", table.table_name()).into());
        self.tables.push(Tbl {
            table_name: Some(table.table_name()),
            column_names: table.column_names(),
        });
        self
    }

    pub fn default_values(mut self) -> Self {
        self.default_values = Some("default values".into());
        self
    }

    fn row_to_named_params(row: impl Serialize) -> Result<NamedParamSlice, Error> {
        Ok(serde_rusqlite::to_params_named(row)?)
    }

    pub fn values(mut self, row: impl Serialize) -> Result<Self, Error> {
        let named_params = Self::row_to_named_params(row)?;

        let column_names = named_params
            .iter()
            .map(|(name, _)| name.replacen(":", "", 1).to_string())
            .collect::<Vec<_>>();
        let placeholders = named_params
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let values: Result<Vec<_>, Error> = named_params
            .iter()
            .map(|(_, to_sql)| {
                Ok(match to_sql.to_sql()? {
                    rusqlite::types::ToSqlOutput::Borrowed(value_ref) => value_ref.into(),
                    rusqlite::types::ToSqlOutput::Owned(value) => value,
                    _ => unimplemented!(),
                })
            })
            .collect();

        self.insert_into = match &self.insert_into {
            Some(sql) => Some(format!("{} ({})", sql, column_names.join(",")).into()),
            None => {
                return Err(Error::Sql(
                    "no table name found when calling values. Try calling insert() first".into(),
                ))
            }
        };

        self.values = values?;
        self.values_sql = Some(format!("values ({})", placeholders).into());

        Ok(self)
    }

    pub fn update(mut self, table: impl Table) -> Self {
        self.update = Some(format!("update {}", table.table_name()).into());
        self.tables.push(Tbl {
            table_name: Some(table.table_name()),
            column_names: table.column_names(),
        });
        self
    }

    pub fn set(mut self, row: impl Serialize) -> Result<Self, Error> {
        let named_params = Self::row_to_named_params(row)?;

        let column_names = named_params
            .iter()
            .map(|(name, _)| name.to_string())
            .collect::<Vec<_>>();
        let set = column_names
            .iter()
            .map(|name| format!("{} = ?", name.replacen(":", "", 1)))
            .collect::<Vec<_>>()
            .join(",");
        self.set = Some(format!("set {}", set).into());
        let values: Result<Vec<_>, Error> = named_params
            .iter()
            .map(|(_, to_sql)| {
                Ok(match to_sql.to_sql()? {
                    rusqlite::types::ToSqlOutput::Borrowed(value_ref) => value_ref.into(),
                    rusqlite::types::ToSqlOutput::Owned(value) => value,
                    _ => unimplemented!(),
                })
            })
            .collect();
        self.values = values?;

        Ok(self)
    }

    pub fn delete(mut self, table: impl Table) -> Self {
        self.delete = Some(format!("delete from {}", table.table_name()).into());
        self.tables.push(Tbl {
            table_name: Some(table.table_name()),
            column_names: table.column_names(),
        });
        self
    }

    async fn delete_row<T: Row + Serialize + DeserializeOwned + Send + Sync + 'static>(
        mut self,
        row: T,
    ) -> Result<T, Error> {
        self.delete = Some(format!("delete from {}", T::table_name()).into());
        self.tables.push(Tbl {
            table_name: Some(T::table_name()),
            column_names: T::column_names(),
        });
        self = self.where_(Sql {
            clause: format!(
                "{} = ?",
                T::pk_column().ok_or(Error::MissingPrimaryKey(T::table_name().into()))?
            ),
            params: vec![row.pk().into()],
        });

        self.returning::<T>().await
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

    async fn save<T: Row + Serialize + DeserializeOwned + Send + Sync + 'static>(
        mut self,
        row: T,
    ) -> Result<T, Error> {
        self.insert_into = Some(format!("insert into {}", T::table_name()).into());
        self.tables.push(Tbl {
            table_name: Some(T::table_name()),
            column_names: T::column_names(),
        });
        let named_params = Self::row_to_named_params(row)?;
        let column_names = named_params
            .iter()
            .map(|(name, _)| name.replacen(":", "", 1).to_string())
            .collect::<Vec<_>>();
        let placeholders = named_params
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let values: Result<Vec<_>, Error> = named_params
            .iter()
            .map(|(_, to_sql)| {
                Ok(match to_sql.to_sql()? {
                    rusqlite::types::ToSqlOutput::Borrowed(value_ref) => value_ref.into(),
                    rusqlite::types::ToSqlOutput::Owned(value) => value,
                    _ => unimplemented!(),
                })
            })
            .collect();

        self.insert_into = match &self.insert_into {
            Some(sql) => Some(format!("{} ({})", sql, column_names.join(",")).into()),
            None => {
                return Err(Error::Sql(
                    "no table name found when calling values. Try calling insert() first".into(),
                ))
            }
        };

        self.values = values?.clone();
        self.values_sql = Some(format!("values ({})", placeholders).into());
        let set = column_names
            .iter()
            .map(|name| format!("{} = ?", name.replacen(":", "", 1)))
            .collect::<Vec<_>>()
            .join(",");
        self.set = Some(format!("on conflict do update set {}", set).into());
        self.values.extend(self.values.clone());

        self.returning::<T>().await
    }
}

impl Row for usize {
    fn column_names() -> Vec<&'static str> {
        vec![]
    }

    fn pk(&self) -> Option<Value> {
        None
    }

    fn pk_column() -> Option<&'static str> {
        None
    }

    fn table_name() -> &'static str {
        ""
    }
}

pub fn placeholder() -> &'static str {
    "?"
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Null<T: ToColumn>(T);

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

impl<T: ToColumn> ToColumn for Null<T> {
    fn to_column(&self) -> &'static str {
        self.0.to_column()
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

pub trait ToValueColumn {
    fn to_value(&self) -> Option<Value>;
    fn to_column(&self) -> Option<&'static str>;

    fn to_params(&self) -> Vec<Value> {
        match self.to_value() {
            Some(value) => match value {
                Value::Null => vec![],
                value => vec![value],
            },
            None => vec![],
        }
    }

    fn to_placeholder(&self) -> &'static str {
        match self.to_value() {
            Some(value) => match value {
                Value::Null => "null",
                _ => "?",
            },
            None => self.to_column().unwrap_or(""),
        }
    }
}

impl<T: ToColumn> ToValueColumn for Null<T> {
    fn to_value(&self) -> Option<Value> {
        None
    }

    fn to_column(&self) -> Option<&'static str> {
        Some(self.0.to_column())
    }
}

impl ToValueColumn for Value {
    fn to_value(&self) -> Option<Value> {
        Some(self.clone())
    }

    fn to_column(&self) -> Option<&'static str> {
        Some("?")
    }
}

impl ToValueColumn for String {
    fn to_value(&self) -> Option<Value> {
        Some(Value::Text(self.clone()))
    }

    fn to_column(&self) -> Option<&'static str> {
        Some("?")
    }
}

impl ToValueColumn for &str {
    fn to_value(&self) -> Option<Value> {
        Some(Value::Text(self.to_string()))
    }

    fn to_column(&self) -> Option<&'static str> {
        Some("?")
    }
}

impl ToValueColumn for i64 {
    fn to_value(&self) -> Option<Value> {
        Some(Value::Integer(*self))
    }

    fn to_column(&self) -> Option<&'static str> {
        Some("?")
    }
}

impl ToValueColumn for f64 {
    fn to_value(&self) -> Option<Value> {
        Some(Value::Real(*self))
    }

    fn to_column(&self) -> Option<&'static str> {
        Some("?")
    }
}

impl ToValueColumn for Vec<u8> {
    fn to_value(&self) -> Option<Value> {
        Some(Value::Blob(self.to_vec()))
    }

    fn to_column(&self) -> Option<&'static str> {
        Some("?")
    }
}

impl ToValueColumn for &[u8] {
    fn to_value(&self) -> Option<Value> {
        Some(Value::Blob(self.to_vec()))
    }

    fn to_column(&self) -> Option<&'static str> {
        Some("?")
    }
}

impl ToValueColumn for Text {
    fn to_value(&self) -> Option<Value> {
        None
    }

    fn to_column(&self) -> Option<&'static str> {
        Some(self.0)
    }
}

impl ToValueColumn for Integer {
    fn to_value(&self) -> Option<Value> {
        None
    }

    fn to_column(&self) -> Option<&'static str> {
        Some(self.0)
    }
}

impl ToValueColumn for Blob {
    fn to_value(&self) -> Option<Value> {
        None
    }

    fn to_column(&self) -> Option<&'static str> {
        Some(self.0)
    }
}

impl ToValueColumn for Real {
    fn to_value(&self) -> Option<Value> {
        None
    }

    fn to_column(&self) -> Option<&'static str> {
        Some(self.0)
    }
}

pub fn eq(left: impl ToColumn, right: impl ToValueColumn) -> Sql {
    let value = right.to_value();
    let op = match value {
        Some(val) => match val {
            Value::Null => "is",
            _ => "=",
        },
        None => "=",
    };
    let params = right.to_params();
    let clause = format!("{} {} {}", left.to_column(), op, right.to_placeholder());
    Sql { clause, params }
}

pub fn ne(left: impl ToColumn, right: impl ToValueColumn) -> Sql {
    let value = right.to_value();
    let op = if let Some(value) = value {
        match value {
            Value::Null => "is not",
            _ => "!=",
        }
    } else {
        "!="
    };
    let params = right.to_params();
    Sql {
        clause: format!("{} {} {}", left.to_column(), op, right.to_placeholder()),
        params,
    }
}

pub fn gt(left: impl ToColumn, right: impl ToValueColumn) -> Sql {
    Sql {
        clause: format!("{} > ?", left.to_column()),
        params: right.to_params(),
    }
}

pub fn lt(left: impl ToColumn, right: impl ToValueColumn) -> Sql {
    Sql {
        clause: format!("{} < ?", left.to_column()),
        params: right.to_params(),
    }
}

pub fn gte(left: impl ToColumn, right: impl ToValueColumn) -> Sql {
    Sql {
        clause: format!("{} >= ?", left.to_column()),
        params: right.to_params(),
    }
}

pub fn lte(left: impl ToColumn, right: impl ToValueColumn) -> Sql {
    Sql {
        clause: format!("{} <= ?", left.to_column()),
        params: right.to_params(),
    }
}

pub fn like(left: impl ToColumn, right: impl ToValueColumn) -> Sql {
    Sql {
        clause: format!("{} like ?", left.to_column()),
        params: right.to_params(),
    }
}

#[deprecated(since = "0.1.0", note = "please use `in_` instead")]
pub fn r#in(left: impl ToColumn, right: Vec<impl ToValueColumn>) -> Sql {
    Sql {
        clause: format!(
            "{} in ({})",
            left.to_column(),
            right.iter().map(|_| "?").collect::<Vec<&str>>().join(",")
        ),
        params: right
            .into_iter()
            .filter_map(|val| val.to_value())
            .collect::<Vec<Value>>(),
    }
}

pub fn in_(left: impl ToColumn, right: Vec<impl ToValueColumn>) -> Sql {
    Sql {
        clause: format!(
            "{} in ({})",
            left.to_column(),
            right.iter().map(|_| "?").collect::<Vec<&str>>().join(",")
        ),
        params: right
            .into_iter()
            .filter_map(|val| val.to_value())
            .collect::<Vec<Value>>(),
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
    fn pk(&self) -> Option<Value>;
    fn pk_column() -> Option<&'static str>;
    fn table_name() -> &'static str;
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
    TokioRusqlite(#[from] tokio_rusqlite::Error),
    #[error("database error: {0}")]
    Rusqlite(#[from] rusqlite::Error),
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
    Deserialize(#[from] serde_json::Error),
    #[error("serialize error: {0}")]
    Serialize(#[from] serde_rusqlite::Error),
    #[error("missing pk column in table: {0}")]
    MissingPrimaryKey(String),
}

#[derive(Debug)]
pub struct Sql {
    pub clause: String,
    pub params: Vec<Value>,
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

    pub fn on(mut self, table: impl Table, columns: impl Select) -> Self {
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

#[derive(Clone, Debug)]
pub struct Database {
    pub connection: tokio_rusqlite::Connection,
}

impl Database {
    pub async fn new(path: &str) -> Result<Self, Error> {
        Ok(Self {
            connection: Connection::default(path).open().await?,
        })
    }

    pub async fn with(connection: Connection) -> Result<Self, Error> {
        Ok(Self {
            connection: connection.open().await?,
        })
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let sql: Arc<str> = sql.into();
        self.connection
            .call(move |conn| conn.execute_batch(&sql))
            .await?;

        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<usize, Error> {
        let sql: Arc<str> = sql.into();
        let affected = self
            .connection
            .call(move |conn| conn.execute(&sql, ()))
            .await?;

        Ok(affected)
    }

    pub async fn query<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        sql: Sql,
    ) -> Result<Vec<T>, Error> {
        ryzz::rows::<T>(&self.connection, sql).await
    }

    pub fn select(&self, columns: impl Select) -> Query {
        Query::new(&self.connection).select(columns)
    }

    pub fn insert(&self, table: impl Table) -> Query {
        Query::new(&self.connection).insert(table)
    }

    pub fn delete_from(&self, table: impl Table) -> Query {
        Query::new(&self.connection).delete(table)
    }

    pub fn update(&self, table: impl Table) -> Query {
        Query::new(&self.connection).update(table)
    }

    pub async fn create<'a>(&'a self, index: &'a Index<'a>) -> Result<(), Error> {
        let sql = index.to_create_sql();
        let rows = self.execute(&sql).await?;
        if rows > 0 {
            println!("{}", sql);
        }
        Ok(())
    }

    pub async fn drop<'a>(&'a self, index: &'a Index<'a>) -> Result<(), Error> {
        let sql = index.to_drop_sql();
        let rows = self.execute(&sql).await?;
        if rows > 0 {
            println!("{}", sql);
        }
        Ok(())
    }

    pub async fn schema(&self) -> Result<String, Error> {
        let schema_table = SqliteSchemaTable::new();
        let rows = self
            .select(())
            .from(schema_table)
            .all::<SqliteSchema>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| row.sql.to_lowercase())
            .collect::<Vec<_>>()
            .join("\n"))
    }

    pub async fn delete<T: Row + Serialize + DeserializeOwned + Send + Sync + 'static>(
        &self,
        row: T,
    ) -> Result<T, Error> {
        Query::new(&self.connection).delete_row(row).await
    }

    pub async fn save<T: Row + Serialize + DeserializeOwned + Send + Sync + 'static>(
        &self,
        row: T,
    ) -> Result<T, Error> {
        Query::new(&self.connection).save(row).await
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[allow(unused)]
    async fn readme_works() -> Result<(), ryzz::Error> {
        use ryzz::*;

        #[table("posts")]
        struct Post {
            #[ryzz(pk)]
            id: i64,
            title: Option<String>,
            body: String,
        }

        #[table]
        struct Comment {
            #[ryzz(pk)]
            id: i64,
            body: String,
            #[ryzz(fk = "posts(id)")]
            post_id: i64,
        }

        let db = Database::new(":memory:").await?;
        let posts = Post::table(&db).await?;
        let comments = Comment::table(&db).await?;

        let post = Post {
            id: 1,
            title: None,
            body: "".into(),
        };

        // insert into posts (id, body) values (?, ?) returning *
        let mut post: Post = db.insert(posts).values(post)?.returning().await?;

        assert_eq!(post.id, 1);
        assert_eq!(post.body, "");
        assert_eq!(post.title, None);

        post.body = "post".into();

        // update posts set body = ?, id = ? where id = ? returning *
        let post: Post = db
            .update(posts)
            .set(post)?
            .where_(and(eq(posts.id, 1), eq(posts.title, Value::Null)))
            .returning()
            .await?;

        assert_eq!(post.id, 1);
        assert_eq!(post.body, "post");

        // delete from posts where id = ? returning *
        let post: Post = db
            .delete_from(posts)
            .where_(eq(posts.id, 1))
            .returning()
            .await?;

        assert_eq!(post.id, 1);
        assert_eq!(post.body, "post");

        let rows: Vec<Post> = db.select(()).from(posts).all().await?;

        assert_eq!(rows.len(), 0);

        let inserted: Post = db
            .insert(posts)
            .values(Post {
                title: None,
                id: 1,
                body: "".into(),
            })?
            .returning()
            .await?;

        assert_eq!(rows.len(), 0);

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
        let row = &rows[0];
        assert_eq!(row.id, new_comment.id);
        assert_eq!(row.body, new_comment.body);
        assert_eq!(row.post_id, new_comment.post_id);

        let query = db
            .select((comments.id, comments.body))
            .from(comments)
            .where_(eq(comments.id, 1))
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

        assert_eq!(rows[0].comment.id, new_comment.id);
        assert_eq!(rows[0].comment.body, new_comment.body);
        assert_eq!(rows[0].comment.post_id, new_comment.post_id);
        assert_eq!(rows[0].post.id, inserted.id);
        assert_eq!(rows[0].post.title, inserted.title);
        assert_eq!(rows[0].post.body, inserted.body);

        let query = db.select(()).from(comments);

        // prepare the query
        let prepared = query.prep::<Comment>().await?;

        // execute the prepared query later
        let rows: Vec<Comment> = prepared.all().await?;

        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[0].body, "");
        assert_eq!(rows[0].post_id, 1);

        Ok(())
    }

    #[allow(unused)]
    #[tokio::test]
    async fn indexes_works() -> Result<(), ryzz::Error> {
        use ryzz::*;

        #[table]
        struct Link {
            #[ryzz(pk)]
            id: i64,
            url: String,
        }

        let db = Database::new(":memory:").await?;
        let links = Link::table(&db).await?;

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
    async fn migrate_works() -> Result<(), ryzz::Error> {
        use ryzz::*;

        #[table("links")]
        struct Link {
            #[ryzz(pk)]
            id: i64,
            url: String,
        }

        let db = Database::new(":memory:").await?;
        let links = Link::table(&db).await?;
        let schema = db.schema().await?;

        assert_eq!(
            schema,
            "create table links (id integer not null primary key,url text not null)"
        );

        let links = Link::table(&db).await?;
        let schema = db.schema().await?;

        assert_eq!(
            schema,
            "create table links (id integer not null primary key,url text not null)"
        );

        #[table("links")]
        struct Link1 {
            #[ryzz(pk)]
            id: i64,
            url: String,
            test: Option<String>,
        }

        let links = Link1::table(&db).await?;
        let schema = db.schema().await?;

        assert_eq!(
            schema,
            "create table links (id integer not null primary key,url text not null, test text)"
        );

        Ok(())
    }

    #[allow(unused)]
    #[tokio::test]
    async fn null_value_in_where_clause_works() -> Result<(), ryzz::Error> {
        use ryzz::*;

        #[table]
        struct Chat {
            #[ryzz(pk)]
            id: i64,
            msg: Option<String>,
        }

        let db = Database::new(":memory:").await?;
        let chats = Chat::table(&db).await?;

        let chat = Chat { id: 1, msg: None };
        let chat2 = Chat {
            id: 2,
            msg: Some("".into()),
        };

        let chat: Chat = db.insert(chats).values(chat)?.returning().await?;
        let chat2: Chat = db.insert(chats).values(chat2)?.returning().await?;

        let rows: Vec<Chat> = db
            .select(())
            .from(chats)
            .where_(eq(chats.msg, Value::Null))
            .all()
            .await?;

        assert_eq!(rows.len(), 1);

        let rows: Vec<Chat> = db
            .select(())
            .from(chats)
            .where_(eq(chats.msg, "".to_string()))
            .all()
            .await?;

        assert_eq!(rows.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn more_complicated_where_clause_works() -> Result<(), ryzz::Error> {
        use ryzz::*;

        #[table]
        struct Glyph {
            image: String,
            aspect: i64,
        }

        let db = Database::new(":memory:").await?;
        let glyphs = Glyph::table(&db).await?;

        let query = db
            .select(glyphs.image)
            .from(glyphs)
            .where_(or(in_(glyphs.aspect, vec![1, 2]), like(glyphs.image, "A%")));

        let sql = query.sql_statement::<Glyph>();

        assert_eq!(
            "select json_object('image', Glyph.image)  from Glyph where (Glyph.aspect in (?,?) or Glyph.image like ?)",
            sql.clause
        );

        assert_eq!(
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Text("A%".into())
            ],
            sql.params
        );

        Ok(())
    }

    #[tokio::test]
    async fn default_works() -> Result<(), ryzz::Error> {
        use ryzz::*;

        #[table("defaults")]
        struct Default {
            #[ryzz(default_ = "'hello'")]
            im_default: Option<String>,
        }

        let db = Database::new(":memory:").await?;
        let defaults = Default::table(&db).await?;

        assert_eq!(
            "create table defaults (im_default text default 'hello')",
            db.schema().await?
        );

        let query = db.insert(defaults).default_values();
        let sql = query.sql_statement::<Default>();

        assert_eq!(sql.clause, "insert into defaults default values");
        assert_eq!(sql.params, vec![]);

        let row: Default = query.returning().await?;

        assert_eq!(row.im_default, Some("hello".into()));

        Ok(())
    }

    #[tokio::test]
    async fn delete_works() -> Result<(), ryzz::Error> {
        use ryzz::*;

        #[table]
        struct User {
            #[ryzz(pk)]
            id: i64,
        }

        let db = Database::new(":memory:").await?;
        let users = User::table(&db).await?;

        let user = User { id: 1 };
        let user: User = db.insert(users).values(user)?.returning().await?;
        assert_eq!(user.id, 1);

        let user = db.delete(user).await?;
        assert_eq!(user.id, 1);

        let rows: Vec<User> = db
            .select(())
            .from(users)
            .where_(eq(users.id, 1))
            .all()
            .await?;

        assert_eq!(rows.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn save_works() -> Result<(), ryzz::Error> {
        use ryzz::*;

        #[table]
        struct User {
            #[ryzz(pk)]
            id: i64,
        }

        let db = Database::new(":memory:").await?;
        let users = User::table(&db).await?;

        let user = User { id: 1 };
        let user: User = db.save(user).await?;
        assert_eq!(user.id, 1);

        let rows: Vec<User> = db
            .select(())
            .from(users)
            .where_(eq(users.id, 1))
            .all()
            .await?;

        assert_eq!(rows.len(), 1);

        Ok(())
    }
}
