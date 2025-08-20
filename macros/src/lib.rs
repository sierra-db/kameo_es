use proc_macro::TokenStream;
use quote::ToTokens;
use syn::parse_macro_input;

use crate::command_name::DeriveCommandName;
use crate::event_type::DeriveEventType;

mod command_name;
mod event_type;

#[proc_macro_derive(EventType)]
pub fn derive_event_type(input: TokenStream) -> TokenStream {
    let derive_event_type = parse_macro_input!(input as DeriveEventType);
    TokenStream::from(derive_event_type.into_token_stream())
}

#[proc_macro_derive(CommandName)]
pub fn derive_command_name(input: TokenStream) -> TokenStream {
    let derive_command_name = parse_macro_input!(input as DeriveCommandName);
    TokenStream::from(derive_command_name.into_token_stream())
}
