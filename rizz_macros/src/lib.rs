use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{
    parse::Parse, parse_macro_input, DeriveInput, Expr, ExprAssign, ExprLit, ExprPath, Lit, LitStr,
    PathSegment, Result,
};

#[proc_macro_derive(Table, attributes(rizz))]
pub fn table(s: TokenStream) -> TokenStream {
    let input = parse_macro_input!(s as DeriveInput);
    match table_macro(input) {
        Ok(s) => s.to_token_stream().into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn table_macro(input: DeriveInput) -> Result<TokenStream2> {
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
    let column_defs = attrs
        .iter()
        .filter_map(|(ident, ty, attrs)| {
            let rizz_attr = if let Some(attr) = attrs.iter().nth(0) {
                attr.parse_args::<RizzAttr>().ok()
            } else {
                None
            };
            if let Some(rizz_attr) = rizz_attr {
                let not_null = match rizz_attr.not_null {
                    true => Some("not null".into()),
                    false => None,
                };
                let primary_key = match rizz_attr.primary_key {
                    true => Some("primary key".into()),
                    false => None,
                };
                let data_type = ty.to_token_stream().to_string().to_lowercase();
                let default_value = match &rizz_attr.default_value {
                    Some(s) => Some(format!("default {}", s.value())),
                    None => None,
                };
                let references = match &rizz_attr.references {
                    Some(rf) => Some(format!("references {}", rf.value())),
                    None => None,
                };
                Some(
                    vec![
                        Some(ident.to_string()),
                        Some(data_type),
                        primary_key,
                        not_null,
                        default_value,
                        references,
                    ]
                    .into_iter()
                    .filter_map(|s| s)
                    .collect::<Vec<_>>()
                    .join(" "),
                )
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let column_names = attrs
        .iter()
        .map(|(ident, _, _)| ident.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let index_names = attrs
        .iter()
        .filter(
            |(_, ty, _)| match ty.to_token_stream().to_string().as_ref() {
                "Index" | "UniqueIndex" => true,
                _ => false,
            },
        )
        .filter_map(|(_, _, attrs)| attrs.iter().nth(0))
        .filter_map(|attr| attr.parse_args::<RizzAttr>().ok())
        .filter_map(|attr| match attr.columns {
            Some(col) => Some(col.value()),
            None => None,
        })
        .collect::<Vec<_>>()
        .join(",");
    let attrs = attrs
        .iter()
        .map(|(ident, ty, _)| {
            let value = format!(r#"{}."{}""#, table_name, ident.to_string());
            match ty.into_token_stream().to_string().as_str() {
                "Integer" => quote! { #ident: Integer(#value) },
                "Blob" => quote! { #ident: Blob(#value) },
                "Real" => quote! { #ident: Real(#value) },
                "Text" => quote! { #ident: Text(#value) },
                _ => unimplemented!(),
            }
        })
        .collect::<Vec<_>>();
    let insert_sql = format!("insert into {} ({})", table_name, column_names);
    let update_sql = format!("update {}", table_name);
    let delete_sql = format!("delete from {}", table_name);
    let create_table_sql = format!("create table {} ({})", table_name, column_defs);

    Ok(quote! {
        impl rizz::Table for #struct_name {
            fn new() -> Self {
                Self {
                    #(#attrs,)*
                }
            }

            fn table_name(&self) -> &'static str {
                #table_name
            }

            fn column_names(&self) -> &'static str {
                #column_names
            }

            fn insert_sql(&self) -> &'static str {
                #insert_sql
            }

            fn update_sql(&self) -> &'static str {
                #update_sql
            }

            fn delete_sql(&self) -> &'static str {
                #delete_sql
            }

            fn index_names(&self) -> &'static str {
                #index_names
            }

            fn create_table_sql(&self) -> &'static str {
                #create_table_sql
            }
        }

        impl rizz::ToSql for #struct_name {
            fn to_sql(&self) -> Value {
                Value::Lit(self.table_name())
            }
        }
    })
}

#[proc_macro_derive(Row, attributes(rizz))]
pub fn row(s: TokenStream) -> TokenStream {
    let input = parse_macro_input!(s as DeriveInput);
    match row_macro(input) {
        Ok(s) => s.to_token_stream().into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn row_macro(input: DeriveInput) -> Result<TokenStream2> {
    let struct_name = input.ident;
    let col_idents = match input.data {
        syn::Data::Struct(ref data) => data
            .fields
            .iter()
            .map(|field| {
                field
                    .ident
                    .as_ref()
                    .expect("Struct fields should have names")
            })
            .collect::<Vec<_>>(),
        _ => unimplemented!(),
    };
    let insert_sql = col_idents.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let insert_sql = format!("values ({})", insert_sql);
    let values = col_idents
        .iter()
        .map(|ident| {
            quote! {
                rizz::Value::from(self.#ident.clone())
            }
        })
        .collect::<Vec<_>>();
    let set_sql = col_idents
        .iter()
        .map(|ident| format!("{} = ?", ident.to_string()))
        .collect::<Vec<_>>()
        .join(",");
    let set_sql = format!("set {}", set_sql);
    Ok(quote! {
        impl rizz::Row for #struct_name {
            fn values(&self) -> Vec<rizz::Value> {
                vec![#(#values,)*]
            }

            fn insert_sql(&self) -> &'static str {
                #insert_sql
            }

            fn set_sql(&self) -> &'static str {
                #set_sql
            }
        }
    })
}

impl Parse for RizzAttr {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let mut rizzle_attr = RizzAttr::default();
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
                                    rizzle_attr.table_name = Some(lit_str.clone());
                                }
                                "r#default" => {
                                    rizzle_attr.default_value = Some(lit_str.clone());
                                }
                                "columns" => {
                                    rizzle_attr.columns = Some(lit_str.clone());
                                }
                                "references" => {
                                    rizzle_attr.references = Some(lit_str.clone());
                                }
                                "many" => {
                                    rizzle_attr.rel = Some(Rel::Many(lit_str.clone()));
                                }
                                "from" => {
                                    rizzle_attr.from = Some(lit_str.clone());
                                }
                                "to" => {
                                    rizzle_attr.to = Some(lit_str.clone());
                                }
                                "one" => {
                                    rizzle_attr.rel = Some(Rel::One(lit_str.clone()));
                                }
                                _ => unimplemented!(),
                            }
                        }
                    }
                    _ => unimplemented!(),
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
                        "not_null" => rizzle_attr.not_null = true,
                        "primary_key" => rizzle_attr.primary_key = true,
                        _ => unimplemented!(),
                    },
                    _ => {}
                },
                _ => {}
            }
        }

        Ok(rizzle_attr)
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
    default_value: Option<LitStr>,
    columns: Option<LitStr>,
    references: Option<LitStr>,
    from: Option<LitStr>,
    to: Option<LitStr>,
    rel: Option<Rel>,
}
