extern crate self as rizz;

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use rizz::{
        sqlite::{db, JournalMode, Synchronous},
        RizzleError,
    };

    type TestResult<T> = Result<T, RizzleError>;

    #[tokio::test]
    async fn it_works() -> TestResult<()> {
        let db = db(":memory:")
            .journal_mode(JournalMode::Wal)
            .synchronous(Synchronous::Normal)
            .connect()
            .await?;

        assert!(true);

        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod sqlite {
    use crate::RizzleError;
    use rusqlite::OpenFlags;
    use std::sync::Arc;
    use tokio_rusqlite::Connection;

    pub fn db(path: &str) -> Database {
        Database::new().path(path)
    }

    #[derive(Clone, Debug)]
    pub struct Database {
        connection: Option<Connection>,
        path: Arc<str>,
        open_flags: OpenFlags,
        pragma: String,
    }

    impl Database {
        pub fn new() -> Self {
            Self {
                path: ":memory:".into(),
                open_flags: OpenFlags::default(),
                connection: None,
                pragma: "".into(),
            }
        }

        pub fn pragma(mut self, statement: &str) -> Self {
            self.pragma
                .push_str(format!("PRAGMA {};", statement).as_str());
            self
        }

        pub fn create_if_missing(mut self, arg: bool) -> Self {
            if !arg {
                self.open_flags = self.open_flags.difference(OpenFlags::SQLITE_OPEN_CREATE);
            }
            self
        }

        pub fn path(mut self, arg: &str) -> Self {
            self.path = arg.into();
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
            self.pragma
                .push_str(format!("PRAGMA journal_mode = {};", value).as_str());
            self
        }

        pub fn synchronous(mut self, sync: Synchronous) -> Self {
            let value = match sync {
                Synchronous::Off => "OFF",
                Synchronous::Normal => "NORMAL",
                Synchronous::Full => "FULL",
                Synchronous::Extra => "EXTRA",
            };
            self.pragma
                .push_str(format!("PRAGMA synchronous = {};", value).as_str());
            self
        }

        pub async fn connect(mut self) -> Result<Self, RizzleError> {
            let connection =
                Connection::open_with_flags(self.path.as_ref(), self.open_flags).await?;
            let pragma = self.pragma.clone();
            let _ = connection
                .call(move |conn| conn.execute_batch(&pragma))
                .await?;
            self.connection = Some(connection);

            Ok(self)
        }
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
}

#[derive(thiserror::Error, Debug)]
pub enum RizzleError {
    #[error("database connection closed")]
    ConnectionClosed,
    #[error("database connection closing: {0}")]
    Close(String),
    #[error("database error: {0}")]
    Database(String),
    #[error("missing from statement in sql query")]
    MissingFrom,
}

#[cfg(not(target_arch = "wasm32"))]
impl From<tokio_rusqlite::Error> for RizzleError {
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
impl From<rusqlite::Error> for RizzleError {
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
