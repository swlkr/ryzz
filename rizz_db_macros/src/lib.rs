use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{
    parse::Parse, parse::Parser, parse_macro_input, DeriveInput, Expr, ExprAssign, ExprLit,
    ExprPath, Field, ItemStruct, Lit, LitStr, PathSegment, Result,
};

#[proc_macro_attribute]
pub fn row(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input: TokenStream2 = input.into();
    let output = quote! {
        #[derive(Row, Serialize, Deserialize, Debug)]
        #input
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
        impl rizz::Row for #struct_name {
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
    let mut item_struct = parse_macro_input!(input as ItemStruct);
    let item_struct1 = item_struct.clone();
    let table_idents = item_struct1
        .fields
        .iter()
        .map(|Field { ty, .. }| ty)
        .collect::<Vec<_>>();
    let initialize_lines = item_struct1
        .fields
        .iter()
        .map(|Field { ty, ident, .. }| {
            quote! { #ident: #ty::new() }
        })
        .collect::<Vec<_>>();
    match item_struct.fields {
        syn::Fields::Named(ref mut fields) => fields.named.push(
            Field::parse_named
                .parse2(quote! { connection: tokio_rusqlite::Connection })
                .unwrap(),
        ),
        _ => {}
    };

    let ident = &item_struct.ident;

    let output = quote! {
        #[derive(Debug, Clone)]
        #item_struct

        impl Based for #ident {
            async fn new(connection: rizz::Connection) -> Result<Self, rizz::Error> {
                let connection = connection.open().await?;

                let db = Self {
                    connection,
                    #(#initialize_lines,)*
                };

                db.migrate().await?;

                Ok(db)
            }

            fn tables() -> Vec<Box<dyn rizz::Table + 'static>> {
                vec![#(Box::new(#table_idents::new()),)*]
            }

            fn connection(&self) -> &tokio_rusqlite::Connection {
                &self.connection
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
        .last()
        .expect("define #![rizz(table = \"your table name here\")] on struct")
        .table_name
        .unwrap();
    let struct_name = input.ident;
    let table_name = format!(r#""{}""#, table_str.value());
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
    let column_fields = attrs
        .iter()
        .filter(|(_, ty, _)| ty.to_token_stream().to_string() != "Index")
        .collect::<Vec<_>>();
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

            let value = format!(r#"{}."{}""#, table_name, name);
            match ty.into_token_stream().to_string().as_str() {
                "Integer" => quote! { #ident: Integer(#value) },
                "Blob" => quote! { #ident: Blob(#value) },
                "Real" => quote! { #ident: Real(#value) },
                "Text" => quote! { #ident: Text(#value) },
                "Index" => quote! { #ident: "" },
                _ => unimplemented!(),
            }
        })
        .collect::<Vec<_>>();
    let create_table_sql = format!(
        "create table if not exists {} ({});",
        table_name, column_def_sql
    );
    let drop_table_sql = format!("drop table if exists {};", table_name);
    let struct_string = struct_name.to_string();
    Ok(quote! {
        impl rizz::Table for #struct_name {
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

            fn create_table_sql(&self) -> &'static str {
                #create_table_sql
            }

            fn drop_table_sql(&self) -> &'static str {
                #drop_table_sql
            }

            fn column_names(&self) -> Vec<&'static str> {
                vec![#(#column_names,)*]
            }

            fn add_column_sql(&self, column_name: &str) -> String {
                let unqualified_column_name = column_name.split(".").nth(1).expect("column name must be qualified: table.column").replace("\"", "");
                let column_defs: Vec<String> = vec![#(#column_defs.to_string(),)*];
                if let Some(column_def) = column_defs.iter().filter(|c| if let Some(name) = &c.split(" ").nth(0) { if name == &unqualified_column_name { true } else { false } } else { false }).nth(0) {
                    format!("alter table {} add column {};", #table_name, column_def)
                } else {
                    panic!("column {} on table {} doesnt exist", unqualified_column_name, #table_name);
                }
            }

            fn create_index_sql(&self, unique: bool, column_names: Vec<&str>) -> String {
                let bare_column_names = column_names.iter().map(|name| name.split(".").nth(1).expect("column name must be qualified: table.column").replace("\"", "")).collect::<Vec<_>>();
                let bare_table_name = self.table_name().replace("\"", "");
                let index_name = format!("{}_{}", &bare_table_name, bare_column_names.join("_"));
                let sql = format!("create{}index {} on {} ({});", if unique == true { " unique " } else { " " }, index_name, &bare_table_name, bare_column_names.join(","));

                sql
            }

            fn drop_index_sql(&self, column_names: Vec<&str>) -> String {
                let bare_column_names = column_names.iter().map(|name| name.split(".").nth(1).expect("column name must be qualified: table.column").replace("\"", "")).collect::<Vec<_>>();
                let bare_table_name = self.table_name().replace("\"", "");
                let index_name = format!("{}_{}", bare_table_name, bare_column_names.join("_"));
                let sql = format!("drop index {};", index_name);

                sql
            }
        }

        impl rizz::ToSql for #struct_name {
            fn to_sql(&self) -> rizz::Value {
                rizz::Value::Lit(self.table_name())
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
}
