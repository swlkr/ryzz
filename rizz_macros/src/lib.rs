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
    let col_pairs = match input.data {
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
                )
            })
            .collect::<Vec<_>>(),
        _ => unimplemented!(),
    };
    let column_names = col_pairs
        .iter()
        .map(|(ident, _)| ident.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let attrs = col_pairs
        .iter()
        .map(|(ident, ty)| {
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
                Value::from(self.#ident.clone())
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
            fn values(&self) -> Vec<Value> {
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
