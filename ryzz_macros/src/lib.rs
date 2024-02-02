use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{quote, ToTokens};
use syn::{
    parse::Parse, parse_macro_input, Attribute, DeriveInput, Error, Expr, ExprAssign, ExprLit,
    ExprPath, Field, Ident, ItemStruct, Lit, LitStr, PathSegment, Result, Type, TypePath,
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
    let struct_name = &input.ident;
    let fields = ryzz_fields(&input)?;
    let columns: Vec<_> = fields.iter().map(ryzz_field_name).collect();
    let (pk_column, pk) = fields
        .into_iter()
        .filter_map(|RyzzField { ident, attrs, .. }| {
            if let Some(_) = attrs.iter().find(|attr| attr.pk) {
                let name = ident.to_string();
                Some((quote! { Some(#name) }, quote! { Some(self.#ident.into()) }))
            } else {
                None
            }
        })
        .last()
        .unwrap_or((quote! { None }, quote! { None }));
    let table_name = match input
        .attrs
        .iter()
        .filter_map(ryzz_attr)
        .filter_map(|attr| attr.table_name)
        .last()
    {
        Some(s) => s.value(),
        None => struct_name.to_string(),
    };

    Ok(quote! {
        impl ryzz::Row for #struct_name {
            fn column_names() -> Vec<&'static str> {
                vec![#(#columns,)*]
            }

            fn pk_column() -> Option<&'static str> {
                #pk_column
            }

            fn pk(&self) -> Option<Value> {
                #pk
            }

            fn table_name() -> &'static str {
                #table_name
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
fn table_macro(args: Args, row_struct: ItemStruct) -> Result<TokenStream2> {
    let Args { name } = args;
    let row_ident = &row_struct.ident;
    let name = if let Some(name) = name {
        name
    } else {
        LitStr::new(&row_ident.to_string(), row_ident.span())
    };
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
            // let ty_string = &field.ty.to_token_stream().to_string();
            let type_col = type_col(&field.ty);
            let vis = &field.vis;
            let span = if let Some(col) = &type_col {
                col.ident.span()
            } else {
                ident.span()
            };
            let ty = match type_col {
                Some(col) => match (col.null, col.ident.to_string().as_str()) {
                    (false, "i64") => quote! { ryzz::Integer },
                    (false, "f64") => quote! { ryzz::Real },
                    (false, "Vec<u8>") => quote! { ryzz::Blob },
                    (false, "String") => quote! { ryzz::Text },
                    (true, "i64") => quote! { ryzz::Null<ryzz::Integer> },
                    (true, "f64") => quote! { ryzz::Null<ryzz::Real> },
                    (true, "Vec<u8>") => quote! { ryzz::Null<ryzz::Blob> },
                    (true, "String") => quote! { ryzz::Null<ryzz::Text> },
                    _ => {
                        return Err(Error::new(
                            span,
                            "T must be i64, String, f64, Vec<u8> or Option<T>",
                        ))
                    }
                },
                None => {
                    return Err(Error::new(
                        span,
                        "T must be i64, String, f64, Vec<u8> or Option<T>",
                    ))
                }
            };

            Ok(quote! { #(#attrs)* #vis #ident: #ty })
        })
        .collect::<Result<Vec<_>>>()?;

    let table_alias = match &row_struct
        .attrs
        .iter()
        .filter_map(ryzz_attr)
        .find(|attr| attr.r#as.is_some())
    {
        Some(RyzzAttr { r#as, .. }) => quote! { #[ryzz(r#as = #r#as)] },
        None => quote! {},
    };

    Ok(quote! {
        #[row]
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
                    .where_(ryzz::and(ryzz::eq(sqlite_schema.r#type, "table".to_owned()), ryzz::eq(sqlite_schema.name, table.table_name().to_owned())))
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
    let struct_name = &input.ident;
    let input_attrs: Vec<RyzzAttr> = input
        .attrs
        .iter()
        .filter_map(|attr| attr.parse_args::<RyzzAttr>().ok())
        .collect();
    let table_name = match input_attrs
        .iter()
        .filter_map(|attr| attr.table_name.as_ref())
        .last()
    {
        Some(s) => s.value(),
        None => struct_name.to_string(),
    };
    let table_alias = input_attrs
        .iter()
        .filter_map(|attr| attr.r#as.as_ref())
        .last();

    let (table_name, table_alias) = match table_alias {
        Some(x) => (x.value(), quote! { Some(#table_name) }),
        None => (table_name, quote! { None }),
    };

    let fields = ryzz_fields(&input)?;
    let column_names: Vec<_> = fields.iter().map(ryzz_field_name).collect();
    let column_defs = fields.iter().map(column_def).collect::<Vec<_>>();
    let new_fields = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let name = ryzz_field_name(&f);
            let value = format!("{}.{}", table_name, name);
            let ty = &f.ty;
            let col = type_col(&ty);
            Ok(match col {
                Some(c) => match c.null {
                    true => {
                        let ty_ident = c.ident;
                        quote! { #ident: ryzz::Null(ryzz::#ty_ident(#value)) }
                    }
                    false => quote! { #ident: #ty(#value) },
                },
                None => quote! { #ident: #ty(#value) },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let create_table_sql = format!(
        "create table if not exists {} ({});",
        table_name,
        column_defs.join(",")
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
                                "r#default" | "default_" => {
                                    ryzz_attr.default_value = Some(lit_str.clone());
                                }
                                "fk" => {
                                    ryzz_attr.references = Some(lit_str.clone());
                                }
                                "name" => {
                                    ryzz_attr.name = Some(lit_str.clone());
                                }
                                "r#as" | "as_" => {
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

struct Col<'a> {
    null: bool,
    ident: &'a Ident,
}

fn type_col<'a>(ty: &'a Type) -> Option<Col> {
    match &ty {
        syn::Type::Path(TypePath { path, .. }) => {
            if let Some(seg) = path.segments.last() {
                match &seg.arguments {
                    syn::PathArguments::None => Some(Col {
                        null: false,
                        ident: &seg.ident,
                    }),
                    syn::PathArguments::AngleBracketed(args) => match args.args.last() {
                        Some(arg) => match arg {
                            syn::GenericArgument::Type(ty) => Some(Col {
                                null: true,
                                ident: type_ident(&ty)?,
                            }),
                            _ => unimplemented!(),
                        },
                        None => unimplemented!(),
                    },
                    syn::PathArguments::Parenthesized(_) => unimplemented!(),
                }
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
    references: Option<LitStr>,
    name: Option<LitStr>,
    r#as: Option<LitStr>,
}

struct RyzzField {
    ident: Ident,
    ty: Type,
    attrs: Vec<RyzzAttr>,
}

fn ryzz_field(field: &Field) -> Result<RyzzField> {
    let attrs = field.attrs.iter().filter_map(ryzz_attr).collect::<Vec<_>>();
    match &field.ident {
        Some(ident) => Ok(RyzzField {
            ident: ident.clone(),
            ty: field.ty.clone(),
            attrs,
        }),
        None => Err(Error::new(
            Span::call_site(),
            "Only named fields are supported",
        )),
    }
}

fn ryzz_attr(attr: &Attribute) -> Option<RyzzAttr> {
    attr.parse_args::<RyzzAttr>().ok()
}

fn ryzz_field_name(field: &RyzzField) -> String {
    match field
        .attrs
        .iter()
        .filter_map(|attr| attr.name.as_ref())
        .last()
    {
        Some(name) => name.value(),
        None => field.ident.to_string(),
    }
}

fn ryzz_fields(input: &DeriveInput) -> Result<Vec<RyzzField>> {
    match &input.data {
        syn::Data::Struct(ds) => ds.fields.iter().map(ryzz_field).collect(),
        _ => unimplemented!(),
    }
}

fn column_def(field: &RyzzField) -> String {
    let name = ryzz_field_name(&field);
    let Some(type_col) = type_col(&field.ty) else {
        return "".into();
    };
    vec![
        Some(name),
        Some(type_col.ident.to_string()),
        match type_col.null {
            true => None,
            false => Some("not null".into()),
        },
        pk(field),
        unique(field),
        r#default(field),
        fk(field),
    ]
    .into_iter()
    .filter_map(|s| s)
    .collect::<Vec<_>>()
    .join(" ")
}

fn pk(field: &RyzzField) -> Option<String> {
    match field.attrs.iter().find(|attr| attr.pk == true) {
        Some(_) => Some("primary key".into()),
        None => None,
    }
}

fn unique(field: &RyzzField) -> Option<String> {
    match field.attrs.iter().find(|attr| attr.unique == true) {
        Some(_) => Some("unique".into()),
        None => None,
    }
}

fn r#default(field: &RyzzField) -> Option<String> {
    match field
        .attrs
        .iter()
        .filter_map(|attr| attr.default_value.as_ref())
        .last()
    {
        Some(r#default) => Some(format!("default {}", r#default.value())),
        None => None,
    }
}

fn fk(field: &RyzzField) -> Option<String> {
    match field
        .attrs
        .iter()
        .filter_map(|attr| attr.references.as_ref())
        .last()
    {
        Some(fk) => Some(format!("references {}", fk.value())),
        None => None,
    }
}
