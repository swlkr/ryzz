use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{
    parse::Parse, parse_macro_input, Attribute, DeriveInput, Error, Expr, ExprAssign, ExprLit,
    ExprPath, Ident, ItemStruct, Lit, LitStr, PathSegment, Result, Type, TypePath,
};

#[proc_macro_attribute]
pub fn row(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    match row_macro(input) {
        Ok(s) => s.to_token_stream().into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn row_macro(mut item_struct: ItemStruct) -> Result<TokenStream2> {
    for field in &mut item_struct.fields {
        if field.attrs.iter().any(|attr| {
            let ident = if let Some(segment) = attr.path.segments.last() {
                segment.ident.to_string()
            } else {
                "".into()
            };
            // HACK check for serde(default) on fields so there aren't duplicates
            ident == "serde" && attr.tokens.to_string() == "(default)"
        }) {
        } else {
            // HACK put serde(default) on all fields
            let attr: Attribute = syn::parse_quote! { #[serde(default)] };
            field.attrs.push(attr);
        }
    }

    Ok(quote! {
        #[derive(Row, Default, serde::Serialize, serde::Deserialize, Debug, Clone)]
        #item_struct
    })
}

#[proc_macro_derive(Row, attributes(ryzz))]
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
            let ryzz_attr = if let Some(attr) = attrs.iter().nth(0) {
                attr.parse_args::<RyzzAttr>().ok()
            } else {
                None
            };

            match ryzz_attr {
                Some(attr) => match attr.name {
                    Some(name) => name.value(),
                    None => ident.to_string(),
                },
                None => ident.to_string(),
            }
        })
        .collect::<Vec<_>>();

    Ok(quote! {
        impl ryzz::Row for #struct_name {
            fn column_names() -> Vec<&'static str> {
                vec![#(#columns,)*]
            }
        }
    })
}

struct Args {
    name: Option<LitStr>,
}

impl Parse for Args {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let name = input.parse::<LitStr>().ok();

        Ok(Self { name })
    }
}

#[proc_macro_attribute]
pub fn table(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as Args);
    let input = parse_macro_input!(input as ItemStruct);
    match table_macro(args, input) {
        Ok(s) => s.to_token_stream().into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// this creates a table struct and derives row on the given struct
fn table_macro(args: Args, mut row_struct: ItemStruct) -> Result<TokenStream2> {
    let Args { name } = args;
    let row_ident = &row_struct.ident;
    let name = if let Some(name) = name {
        name
    } else {
        LitStr::new(&row_ident.to_string(), row_ident.span())
    };
    let row_attrs = row_struct.attrs;
    let table_struct_name = format!("{}Table", row_ident);
    let table_struct_ident = Ident::new(&table_struct_name, row_ident.span());
    let table_fields = row_struct
        .fields
        .iter()
        .map(|field| {
            let ident = field
                .ident
                .as_ref()
                .ok_or(Error::new(row_ident.span(), "Named fields only"))?;
            let attrs = &field.attrs;
            let ty_string = &field.ty.to_token_stream().to_string();
            let vis = &field.vis;
            let span = if let Some(ty_ident) = type_ident(&field.ty) {
                ty_ident.span()
            } else {
                ident.span()
            };
            match ty_string.as_str() {
                "i64" | "Option < i64 >" => Ok(quote! { #(#attrs)* #vis #ident: ryzz::Integer }),
                "f64" | "Option < f64 > " => Ok(quote! { #(#attrs)* #vis #ident: ryzz::Real }),
                "Vec<u8>" | "Option < Vec < u8 > >" => {
                    Ok(quote! { #(#attrs)* #vis #ident: ryzz::Blob })
                }
                "String" => Ok(quote! { #(#attrs)* #vis #ident: ryzz::Text }),
                "Option < String >" => Ok(quote! { #(#attrs)* #vis #ident: ryzz::NullText }),
                _ => Err(Error::new(
                    span,
                    "T must be i64, String, f64, Vec<u8> or Option<T>",
                )),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let table_alias = match row_attrs
        .iter()
        .filter_map(|attr| attr.parse_args::<RyzzAttr>().ok())
        .find(|attr| attr.r#as.is_some())
    {
        Some(RyzzAttr { r#as, .. }) => quote! { #[ryzz(r#as = #r#as)] },
        None => quote! {},
    };

    // strip ryzz attrs from row_struct itself
    row_struct.attrs = row_attrs
        .iter()
        .filter_map(|attr| match attr.parse_args::<RyzzAttr>() {
            Ok(_) => None,
            Err(_) => Some(attr.clone()),
        })
        .collect::<Vec<_>>();

    // strip ryzz attrs from row_struct fields
    for field in &mut row_struct.fields {
        field.attrs = field
            .attrs
            .iter()
            .filter_map(|x| match x.parse_args::<RyzzAttr>() {
                Ok(_) => None,
                Err(_) => Some(x.clone()),
            })
            .collect::<Vec<_>>();
    }

    Ok(quote! {
        #[row]
        #(#row_attrs)*
        #row_struct

        #[derive(ryzz::Table, Clone, Copy, Debug, Default)]
        #table_alias
        #[ryzz(table = #name)]
        pub struct #table_struct_ident {
            #(#table_fields,)*
        }

        impl #row_ident {
            pub async fn table(db: &ryzz::Database) -> core::result::Result<#table_struct_ident, ryzz::Error> {
                let table = #table_struct_ident::new();
                // create tables
                let sql = table.create_table_sql();
                let affected = db.execute(sql).await?;
                if affected > 0 {
                    println!("{}", sql);
                }

                let sqlite_schema = ryzz::SqliteSchemaTable::new();
                let pti = ryzz::TableInfoTable::new();

                // add columns
                let db_columns = db
                    .select(())
                    .from(sqlite_schema)
                    .left_outer_join(pti, ryzz::ne(pti.name, sqlite_schema.name))
                    .r#where(ryzz::and(ryzz::eq(sqlite_schema.r#type, "table"), ryzz::eq(sqlite_schema.name, table.table_name())))
                    .all::<ryzz::TableInfoRow>()
                    .await?
                    .into_iter()
                    .map(|x| x.column())
                    .collect::<std::collections::HashSet<_>>();

                let columns = table
                    .column_names()
                    .into_iter()
                    .map(|c| c.to_string())
                    .collect::<std::collections::HashSet<_>>();

                let difference = columns.difference(&db_columns);
                let statements = difference
                    .into_iter()
                    .map(|col| {
                        table.add_column_sql(&col)
                    })
                    .collect::<Vec<_>>();

                if !statements.is_empty() {
                    println!("=== Adding columns ===");
                    let _ = db
                        .execute_batch(&format!("BEGIN;{}COMMIT;", statements.join(";")))
                        .await?;
                    for statement in statements {
                        println!("{}", statement);
                    }
                    println!("=== Columns added successfully ===");
                }

                Ok(table)
            }
        }
    })
}

#[proc_macro_derive(Table, attributes(ryzz))]
pub fn table_derive(s: TokenStream) -> TokenStream {
    let input = parse_macro_input!(s as DeriveInput);
    match table_derive_macro(input) {
        Ok(s) => s.to_token_stream().into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn table_derive_macro(input: DeriveInput) -> Result<TokenStream2> {
    let struct_name = input.ident;
    let table_str = input
        .attrs
        .iter()
        .filter_map(|attr| attr.parse_args::<RyzzAttr>().ok())
        .filter_map(|ra| ra.table_name)
        .last();
    let table_name = format!(
        "{}",
        match table_str {
            Some(s) => s.value(),
            None => struct_name.to_string(),
        }
    );
    let table_alias = input
        .attrs
        .iter()
        .filter_map(|attr| attr.parse_args::<RyzzAttr>().ok())
        .filter_map(|ra| ra.r#as)
        .last();

    let (table_name, table_alias) = match table_alias {
        Some(x) => (x.value(), quote! { Some(#table_name) }),
        None => (table_name, quote! { None }),
    };

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
            let ryzz_attr = if let Some(attr) = attrs.iter().nth(0) {
                attr.parse_args::<RyzzAttr>().ok()
            } else {
                None
            };

            match ryzz_attr {
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
            let ryzz_attr = if let Some(attr) = attrs.iter().nth(0) {
                attr.parse_args::<RyzzAttr>().ok()
            } else {
                None
            };
            let name = if let Some(ref attr) = ryzz_attr {
                if let Some(name) = &attr.name {
                    name.value()
                } else {
                    ident.to_string()
                }
            } else {
                ident.to_string()
            };
            let type_ident = type_ident(&ty)?.to_string().to_ascii_lowercase();
            let mut parts = vec![
                Some(name),
                Some(type_ident.replace("null", "")),
                match type_ident.contains("null") {
                    true => None,
                    false => Some("not null".into()),
                },
            ];
            if let Some(ryzz_attr) = ryzz_attr {
                let primary_key = match ryzz_attr.pk {
                    true => Some("primary key".into()),
                    false => None,
                };
                let unique = match ryzz_attr.unique {
                    true => Some("unique".into()),
                    false => None,
                };
                let default_value = match &ryzz_attr.default_value {
                    Some(s) => Some(format!("default ({})", s.value())),
                    None => None,
                };
                let references = match &ryzz_attr.references {
                    Some(rf) => Some(format!("references {}", rf.value())),
                    None => None,
                };
                parts.extend(vec![primary_key, unique, default_value, references]);
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
    let new_fields = attrs
        .iter()
        .map(|(ident, ty, attrs)| {
            let ryzz_attr = if let Some(attr) = attrs.iter().nth(0) {
                attr.parse_args::<RyzzAttr>().ok()
            } else {
                None
            };

            let name = match ryzz_attr {
                Some(attr) => match attr.name {
                    Some(name) => name.value(),
                    None => ident.to_string(),
                },
                None => ident.to_string(),
            };

            let value = format!("{}.{}", table_name, name);
            Ok(quote! { #ident: #ty(#value) })
        })
        .collect::<Result<Vec<_>>>()?;
    let create_table_sql = format!(
        "create table if not exists {} ({});",
        table_name, column_def_sql
    );
    let struct_string = struct_name.to_string();
    Ok(quote! {
        impl ryzz::Table for #struct_name {
            fn new() -> Self {
                Self {
                    #(#new_fields,)*
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
    })
}

impl Parse for RyzzAttr {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let mut ryzz_attr = RyzzAttr::default();
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
                                    ryzz_attr.table_name = Some(lit_str.clone());
                                }
                                "def" => {
                                    ryzz_attr.default_value = Some(lit_str.clone());
                                }
                                "columns" => {
                                    ryzz_attr.columns = Some(lit_str.clone());
                                }
                                "refs" => {
                                    ryzz_attr.references = Some(lit_str.clone());
                                }
                                "from" => {
                                    ryzz_attr.from = Some(lit_str.clone());
                                }
                                "to" => {
                                    ryzz_attr.to = Some(lit_str.clone());
                                }
                                "name" => {
                                    ryzz_attr.name = Some(lit_str.clone());
                                }
                                "r#as" => {
                                    ryzz_attr.r#as = Some(lit_str.clone());
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
                        "pk" => ryzz_attr.pk = true,
                        "unique" => ryzz_attr.unique = true,
                        _ => {}
                    },
                    _ => {}
                },
                _ => {}
            }
        }

        Ok(ryzz_attr)
    }
}

fn type_ident<'a>(ty: &'a Type) -> Option<&'a Ident> {
    match &ty {
        syn::Type::Path(TypePath { path, .. }) => {
            if let Some(seg) = path.segments.last() {
                Some(&seg.ident)
            } else {
                None
            }
        }
        _ => None,
    }
}

#[derive(Default)]
struct RyzzAttr {
    table_name: Option<LitStr>,
    pk: bool,
    unique: bool,
    default_value: Option<LitStr>,
    columns: Option<LitStr>,
    references: Option<LitStr>,
    from: Option<LitStr>,
    to: Option<LitStr>,
    name: Option<LitStr>,
    r#as: Option<LitStr>,
}
