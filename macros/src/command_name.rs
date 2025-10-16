use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    DeriveInput, Generics, Ident,
};

pub struct DeriveCommandName {
    ident: Ident,
    generics: Generics,
}

impl Parse for DeriveCommandName {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let ident = input.ident;
        let generics = input.generics;

        Ok(DeriveCommandName { ident, generics })
    }
}

impl ToTokens for DeriveCommandName {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self { ident, generics } = self;
        let ident_str = ident.to_string();
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::kameo_es_core::CommandName for #ident #ty_generics #where_clause {
                #[inline(always)]
                fn command_name() -> &'static str {
                    #ident_str
                }
            }
        });
    }
}
