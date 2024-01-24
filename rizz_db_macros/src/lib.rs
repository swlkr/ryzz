use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{
    parse::Parse, parse_macro_input, Attribute, DeriveInput, Expr, ExprAssign, ExprLit, ExprPath,
    Field, ItemStruct, Lit, LitStr, PathSegment, Result,
};

#[proc_macro_attribute]
pub fn row(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input1 = input.clone();
    let mut item_struct = parse_macro_input!(input1 as ItemStruct);
    for field in &mut item_struct.fields {
        if field.attrs.iter().any(|attr| {
            let ident = if let Some(segment) = attr.path.segments.last() {
                segment.ident.to_string()
            } else {
                "".into()
            };
            ident == "serde" && attr.tokens.to_string() == "(default)"
        }) {
        } else {
            let attr: Attribute = syn::parse_quote! { #[serde(default)] };
            field.attrs.push(attr);
        }
    }
    let output = quote! {
        #[derive(Row, Default, serde::Serialize, serde::Deserialize, Debug)]
        #item_struct
    };

    output.into()
}

#[proc_macro_derive(Row, attributes(rizz))]
pub fn row_derive(s: TokenStream) -> TokenStream {
    let input = parse_macro_input!(s as DeriveInput);
    match row_derive_macro(input) {
        Ok(s) => s.to_token_stream().into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn row_derive_macro(input: DeriveInput) -> Result<TokenStream2> {
    let struct_name = input.ident;
    let attrs = match input.data {
        syn::Data::Struct(ref data) => data
            .fields
            .iter()
            .map(|field| {
                (
                    field
                        .ident
                        .as_ref()
                        .expect("Struct fields should have names"),
                    &field.ty,
                    &field.attrs,
                )
            })
            .collect::<Vec<_>>(),
        _ => unimplemented!(),
    };
    let columns = attrs
        .iter()
        .map(|(ident, _, attrs)| {
            let rizz_attr = if let Some(attr) = attrs.iter().nth(0) {
                attr.parse_args::<RizzAttr>().ok()
            } else {
                None
            };

            match rizz_attr {
                Some(attr) => match attr.name {
                    Some(name) => name.value(),
                    None => ident.to_string(),
                },
                None => ident.to_string(),
            }
        })
        .collect::<Vec<_>>();

    Ok(quote! {
        impl rizz_db::Row for #struct_name {
            fn column_names() -> Vec<&'static str> {
                vec![#(#columns,)*]
            }
        }
    })
}

struct Args {
    name: LitStr,
}

impl Parse for Args {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let name = input.parse::<LitStr>()?;

        Ok(Self { name })
    }
}

#[proc_macro_attribute]
pub fn database(_args: TokenStream, input: TokenStream) -> TokenStream {
    let item_struct = parse_macro_input!(input as ItemStruct);
    let table_idents = item_struct
        .fields
        .iter()
        .map(|Field { ty, .. }| ty)
        .collect::<Vec<_>>();
    let initialize_lines = item_struct
        .fields
        .iter()
        .map(|Field { ty, ident, .. }| {
            quote! { #ident: #ty::new() }
        })
        .collect::<Vec<_>>();

    let ident = &item_struct.ident;

    let output = quote! {
        static RIZZ_CONNECTION: std::sync::OnceLock<tokio_rusqlite::Connection> = std::sync::OnceLock::new();

        #[derive(Debug, Clone)]
        #item_struct

        impl #ident {
            async fn new(s: &str) -> core::result::Result<Self, rizz_db::Error> {
                let connection = rizz_db::Connection::default(s);

                let db = Self::connect(connection).await?;

                db.migrate().await?;

                Ok(db)
            }

            async fn connect(c: rizz_db::Connection) -> core::result::Result<Self, rizz_db::Error> {
                match RIZZ_CONNECTION.get() {
                    Some(conn) => {},
                    None => {
                        let connection = c.open().await?;
                        RIZZ_CONNECTION.set(connection).expect("Could not store connection");
                        RIZZ_CONNECTION.get().expect("Could not retrieve connection");
                    }
                };

                let db = Self {
                    #(#initialize_lines,)*
                };

                Ok(db)
            }

            async fn with(c: rizz_db::Connection) -> core::result::Result<Self, rizz_db::Error> {
                let db = Self::connect(c).await?;

                db.migrate().await?;

                Ok(db)
            }

            fn tables() -> Vec<Box<dyn rizz_db::Table + Send + Sync + 'static>> {
                vec![#(Box::new(#table_idents::new()),)*]
            }

            fn connection(&self) -> &'static tokio_rusqlite::Connection {
                RIZZ_CONNECTION.get().expect("Failed to acquire connection")
            }

            async fn migrate(&self) -> core::result::Result<usize, rizz_db::Error> {
                let sqlite_schema = rizz_db::SqliteSchema::new();

                let db_table_names = self
                    .select(())
                    .from(&sqlite_schema)
                    .r#where(eq(sqlite_schema.r#type, "table"))
                    .all::<rizz_db::TableName>()
                    .await?
                    .into_iter()
                    .map(|x| rizz_db::TableName { name: x.name })
                    .collect::<std::collections::HashSet<_>>();

                let tables = Self::tables();
                let table_names = tables
                    .iter()
                    .map(|t| rizz_db::TableName { name: t.table_name().to_owned() })
                    .collect::<std::collections::HashSet<_>>();

                let difference = table_names.difference(&db_table_names);

                let tables_to_create = difference
                    .into_iter()
                    .filter_map(|x| {
                        tables
                            .iter()
                            .find(|t| t.table_name() == x.name)
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
                    println!("=== Tables created successfully ===");
                }

                let pti = rizz_db::TableInfo::new();

                let db_columns = self
                    .select(())
                    .from(&sqlite_schema)
                    .left_outer_join(&pti, ne(pti.name, sqlite_schema.name))
                    .r#where(eq(sqlite_schema.r#type, "table"))
                    .all::<rizz_db::TableInfoRow>()
                    .await?
                    .into_iter()
                    .map(|x| (x.table(), x.column()))
                    .collect::<std::collections::HashSet<_>>();

                let columns = Self::tables()
                    .iter()
                    .flat_map(|t| {
                        let column_names = t.column_names();
                        t.column_names()
                        .into_iter()
                        .map(|c| (t.table_name().to_string(), c.to_string()))
                        .collect::<Vec<_>>()
                    })
                    .collect::<std::collections::HashSet<_>>();

                let difference = columns.difference(&db_columns);
                let columns_to_add = difference
                    .into_iter()
                    .filter_map(|x| {
                        match tables.iter().find(|t| t.table_name() == x.0) {
                            Some(table) => Some(table.add_column_sql(x.1.as_str())),
                            None => None
                        }
                    })
                    .collect::<Vec<_>>();

                if !columns_to_add.is_empty() {
                    println!("=== Adding columns ===");
                    let statements = &columns_to_add;
                    let _ = self
                        .execute_batch(&format!("BEGIN;{}COMMIT;", statements.join(";")))
                        .await?;
                    for statement in statements {
                        println!("{}", statement);
                    }
                    println!("=== Columns added successfully ===");
                }

                Ok(tables_to_create.len() + columns_to_add.len())
            }

            pub async fn execute_batch(&self, sql: &str) -> core::result::Result<(), rizz_db::Error> {
                let sql: std::sync::Arc<str> = sql.into();
                self.connection()
                    .call(move |conn| conn.execute_batch(&sql))
                    .await?;

                Ok(())
            }

            pub async fn query<T: serde::de::DeserializeOwned + Send + 'static>(&self, sql: Sql) -> core::result::Result<Vec<T>, rizz_db::Error> {
                rizz_db::rows::<T>(&self.connection(), sql).await
            }

            pub fn select(&self, columns: impl Select) -> Query {
                Query::new(&self.connection()).select(columns)
            }

            pub fn insert(&self, table: &impl Table) -> Query {
                Query::new(&self.connection()).insert(table)
            }

            pub fn delete_from<'a>(&'a self, table: &'a dyn Table) -> Query<'a> {
                Query::new(&self.connection()).delete(table)
            }

            pub fn update<'a>(&'a self, table: &'a dyn Table) -> Query<'a> {
                Query::new(&self.connection()).update(table)
            }

            pub async fn create<'a>(&'a self, index: &'a rizz_db::Index<'a>) -> core::result::Result<(), rizz_db::Error> {
                let sql = index.to_create_sql();
                println!("=== Creating index ===");
                println!("{}", sql);
                self.execute_batch(&sql).await?;
                println!("=== Successfully created index ===");
                Ok(())
            }

            pub async fn drop<'a>(&'a self, index: &'a rizz_db::Index<'a>) -> core::result::Result<(), rizz_db::Error> {
                let sql = index.to_drop_sql();
                println!("=== Dropping index ===");
                println!("{}", sql);
                self.execute_batch(&sql).await?;
                println!("=== Successfully dropped index ===");
                Ok(())
            }

        }
    };

    output.into()
}

#[proc_macro_attribute]
pub fn table(args: TokenStream, input: TokenStream) -> TokenStream {
    let Args { name: table_name } = parse_macro_input!(args as Args);
    let input: TokenStream2 = input.into();

    let output = quote! {
        #[allow(dead_code)]
        #[derive(Debug, Clone, Copy, Table, Default)]
        #[rizz(table = #table_name)]
        #input
    };

    output.into()
}

#[proc_macro_derive(Table, attributes(rizz))]
pub fn table_derive(s: TokenStream) -> TokenStream {
    let input = parse_macro_input!(s as DeriveInput);
    match table_derive_macro(input) {
        Ok(s) => s.to_token_stream().into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn table_derive_macro(input: DeriveInput) -> Result<TokenStream2> {
    let table_str = input
        .attrs
        .iter()
        .filter_map(|attr| attr.parse_args::<RizzAttr>().ok())
        .filter_map(|ra| ra.table_name)
        .last()
        .expect("define #![rizz(table = \"your table name here\")] on struct");
    let table_alias = input
        .attrs
        .iter()
        .filter_map(|attr| attr.parse_args::<RizzAttr>().ok())
        .filter_map(|ra| ra.r#as)
        .last();

    let (table_str, table_alias) = match table_alias {
        Some(x) => (x, quote! { Some(#table_str) }),
        None => (table_str, quote! { None }),
    };

    let struct_name = input.ident;
    let table_name = format!("{}", table_str.value());
    let attrs = match input.data {
        syn::Data::Struct(ref data) => data
            .fields
            .iter()
            .map(|field| {
                (
                    field
                        .ident
                        .as_ref()
                        .expect("Struct fields should have names"),
                    &field.ty,
                    &field.attrs,
                )
            })
            .collect::<Vec<_>>(),
        _ => unimplemented!(),
    };
    let column_fields = attrs.iter().collect::<Vec<_>>();
    let column_names = attrs
        .iter()
        .map(|(ident, _, attrs)| {
            let rizz_attr = if let Some(attr) = attrs.iter().nth(0) {
                attr.parse_args::<RizzAttr>().ok()
            } else {
                None
            };

            match rizz_attr {
                Some(attr) => match attr.name {
                    Some(name) => name.value(),
                    None => ident.to_string(),
                },
                None => ident.to_string(),
            }
        })
        .collect::<Vec<_>>();

    let column_defs = column_fields
        .iter()
        .filter_map(|(ident, ty, attrs)| {
            let rizz_attr = if let Some(attr) = attrs.iter().nth(0) {
                attr.parse_args::<RizzAttr>().ok()
            } else {
                None
            };
            let data_type = ty.to_token_stream().to_string().to_lowercase();
            let mut parts = vec![Some(ident.to_string()), Some(data_type)];
            if let Some(rizz_attr) = rizz_attr {
                let not_null = match rizz_attr.not_null {
                    true => Some("not null".into()),
                    false => None,
                };
                let primary_key = match rizz_attr.primary_key {
                    true => Some("primary key".into()),
                    false => None,
                };
                let unique = match rizz_attr.unique {
                    true => Some("unique".into()),
                    false => None,
                };
                let default_value = match &rizz_attr.default_value {
                    Some(s) => Some(format!("default ({})", s.value())),
                    None => None,
                };
                let references = match &rizz_attr.references {
                    Some(rf) => Some(format!("references {}", rf.value())),
                    None => None,
                };
                parts.extend(vec![
                    primary_key,
                    unique,
                    not_null,
                    default_value,
                    references,
                ]);
            }
            Some(
                parts
                    .into_iter()
                    .filter_map(|s| s)
                    .collect::<Vec<_>>()
                    .join(" "),
            )
        })
        .collect::<Vec<_>>();
    let column_def_sql = column_defs.join(",");
    let attrs = attrs
        .iter()
        .map(|(ident, ty, attrs)| {
            let rizz_attr = if let Some(attr) = attrs.iter().nth(0) {
                attr.parse_args::<RizzAttr>().ok()
            } else {
                None
            };

            let name = match rizz_attr {
                Some(attr) => match attr.name {
                    Some(name) => name.value(),
                    None => ident.to_string(),
                },
                None => ident.to_string(),
            };

            let value = format!("{}.{}", table_name, name);
            match ty.into_token_stream().to_string().as_str() {
                "Integer" => quote! { #ident: Integer(#value) },
                "Blob" => quote! { #ident: Blob(#value) },
                "Real" => quote! { #ident: Real(#value) },
                "Text" => quote! { #ident: Text(#value) },
                _ => unimplemented!(),
            }
        })
        .collect::<Vec<_>>();
    let create_table_sql = format!(
        "create table if not exists {} ({});",
        table_name, column_def_sql
    );
    let struct_string = struct_name.to_string();
    Ok(quote! {
        impl rizz_db::Table for #struct_name {
            fn new() -> Self {
                Self {
                    #(#attrs,)*
                }
            }

            fn struct_name(&self) -> &'static str {
                #struct_string
            }

            fn table_name(&self) -> &'static str {
                #table_name
            }

            fn table_alias(&self) -> Option<&'static str> {
                #table_alias
            }

            fn create_table_sql(&self) -> &'static str {
                #create_table_sql
            }

            fn column_names(&self) -> Vec<&'static str> {
                vec![#(#column_names,)*]
            }

            fn add_column_sql(&self, column_name: &str) -> String {
                let column_defs: Vec<String> = vec![#(#column_defs.to_string(),)*];
                if let Some(column_def) = column_defs.iter().filter(|c| if let Some(name) = &c.split(" ").nth(0) { if name == &column_name{ true } else { false } } else { false }).nth(0) {
                    format!("alter table {} add column {};", #table_name, column_def)
                } else {
                    panic!("column {} on table {} doesnt exist", column_name, #table_name);
                }
            }
        }

        impl rizz_db::ToSql for #struct_name {
            fn to_sql(&self) -> rizz_db::Value {
                rizz_db::Value::Lit(self.table_name())
            }
        }
    })
}

impl Parse for RizzAttr {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let mut rizz_attr = RizzAttr::default();
        let args_parsed =
            syn::punctuated::Punctuated::<Expr, syn::Token![,]>::parse_terminated(input)?;
        for expr in args_parsed.iter() {
            match expr {
                Expr::Assign(ExprAssign { left, right, .. }) => match (&**left, &**right) {
                    (Expr::Path(ExprPath { path, .. }), Expr::Lit(ExprLit { lit, .. })) => {
                        if let (Some(PathSegment { ident, .. }), Lit::Str(lit_str)) =
                            (path.segments.last(), lit)
                        {
                            match ident.to_string().as_ref() {
                                "table" => {
                                    rizz_attr.table_name = Some(lit_str.clone());
                                }
                                "r#default" => {
                                    rizz_attr.default_value = Some(lit_str.clone());
                                }
                                "columns" => {
                                    rizz_attr.columns = Some(lit_str.clone());
                                }
                                "references" => {
                                    rizz_attr.references = Some(lit_str.clone());
                                }
                                "many" => {
                                    rizz_attr.rel = Some(Rel::Many(lit_str.clone()));
                                }
                                "from" => {
                                    rizz_attr.from = Some(lit_str.clone());
                                }
                                "to" => {
                                    rizz_attr.to = Some(lit_str.clone());
                                }
                                "one" => {
                                    rizz_attr.rel = Some(Rel::One(lit_str.clone()));
                                }
                                "name" => {
                                    rizz_attr.name = Some(lit_str.clone());
                                }
                                "r#as" => {
                                    rizz_attr.r#as = Some(lit_str.clone());
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                },
                Expr::Path(path) => match path.path.segments.len() {
                    1 => match path
                        .path
                        .segments
                        .first()
                        .unwrap()
                        .ident
                        .to_string()
                        .as_ref()
                    {
                        "not_null" => rizz_attr.not_null = true,
                        "primary_key" => rizz_attr.primary_key = true,
                        "unique" => rizz_attr.unique = true,
                        _ => {}
                    },
                    _ => {}
                },
                _ => {}
            }
        }

        Ok(rizz_attr)
    }
}

enum Rel {
    One(LitStr),
    Many(LitStr),
}

#[derive(Default)]
struct RizzAttr {
    table_name: Option<LitStr>,
    primary_key: bool,
    not_null: bool,
    unique: bool,
    default_value: Option<LitStr>,
    columns: Option<LitStr>,
    references: Option<LitStr>,
    from: Option<LitStr>,
    to: Option<LitStr>,
    rel: Option<Rel>,
    name: Option<LitStr>,
    r#as: Option<LitStr>,
}
