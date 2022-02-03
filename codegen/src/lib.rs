use proc_macro::{self};
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields, Ident};

#[proc_macro_derive(RowBinary)]
pub fn derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let column_names = match &input.data {
        Data::Struct(data) => columns(data),
        _ => panic!("`RowBinary` can only be derived for structs"),
    };

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let num = column_names.len();

    let derived_impl = quote! {
        impl #impl_generics ::simple_clickhouse::RowBinary for #name #ty_generics #where_clause {
            fn encode(&self, buf: &mut impl ::std::io::Write) -> ::std::io::Result<()> {
                #(self.#column_names.encode(buf)?;)*
                Ok(())
            }

            fn size_hint(&self) -> usize {
                #(self.#column_names.size_hint())+*
            }
        }

        impl #impl_generics ::simple_clickhouse::ColumnNames<#num> for #name #ty_generics #where_clause {
            const NAMES: [&'static str; #num] = [#(stringify!(#column_names)),*];
        }
    };

    derived_impl.into()
}

fn columns(data: &DataStruct) -> Vec<Ident> {
    match &data.fields {
        Fields::Named(fields) => fields
            .named
            .iter()
            .map(|z| z.ident.clone().unwrap())
            .collect(),
        Fields::Unnamed(fields) => (0..fields.unnamed.len())
            .map(|x| x.to_string())
            .map(|x| Ident::new(&x, Span::call_site()))
            .collect(),
        Fields::Unit => panic!("`RowBinary` cannot be derived for unit structs"),
    }
}
