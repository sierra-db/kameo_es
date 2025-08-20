use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
    Data, DeriveInput, Generics, Ident, Token, Variant,
};

pub struct DeriveEventType {
    ident: Ident,
    generics: Generics,
    variants: Punctuated<Variant, Token![,]>,
}

impl Parse for DeriveEventType {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let span = input.span();
        let ident = input.ident;
        let generics = input.generics;
        let variants = match input.data {
            Data::Struct(_) => {
                return Err(syn::Error::new(
                    span,
                    "structs are not supported with EventType, use an enum instead",
                ));
            }
            Data::Enum(data_enum) => data_enum.variants,
            Data::Union(_) => {
                return Err(syn::Error::new(
                    span,
                    "union types are not supported with EventType",
                ));
            }
        };

        Ok(DeriveEventType {
            ident,
            generics,
            variants,
        })
    }
}

impl ToTokens for DeriveEventType {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self {
            ident,
            generics,
            variants,
        } = self;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let match_arms = variants.iter().map(|variant| {
            let variant_ident = &variant.ident;
            let event_type_str = variant_ident.to_string();
            let catch_fields = match &variant.fields {
                syn::Fields::Named(_) => quote! { { .. } },
                syn::Fields::Unnamed(fields_unnamed) => {
                    let underscores = fields_unnamed.unnamed.iter().map(|_| quote! { _ });
                    quote! { ( #( #underscores ),* ) }
                }
                syn::Fields::Unit => quote! {},
            };
            quote! {
                #ident :: #variant_ident #catch_fields => #event_type_str
            }
        });

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::kameo_es_core::EventType for #ident #ty_generics #where_clause {
                #[inline(always)]
                fn event_type(&self) -> &'static str {
                    match self {
                        #( #match_arms, )*
                    }
                }
            }
        });
    }
}
