use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, parse_macro_input};

#[proc_macro_derive(WorkerRegistry)]
pub fn worker_registry(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Ensure we are working with an Enum
    let variants = match input.data {
        Data::Enum(ref data_enum) => &data_enum.variants,
        _ => panic!("SupervisorDispatcher can only be derived for enums"),
    };

    // Generate match arms for name(&self)
    let name_arms = variants.iter().map(|variant| {
        let v_name = &variant.ident;
        quote! {
            #name::#v_name(worker) => worker.name(),
        }
    });

    // Generate match arms for init(&self)
    let init_arms = variants.iter().map(|variant| {
        let v_name = &variant.ident;
        quote! {
            #name::#v_name(worker) => worker.init().await,
        }
    });

    // Generate match arms for run(&mut self)
    let run_arms = variants.iter().map(|variant| {
        let v_name = &variant.ident;
        quote! {
            #name::#v_name(worker) => worker.run().await,
        }
    });

    // Generate match arms for name(&self)
    let reset_arms = variants.iter().map(|variant| {
        let v_name = &variant.ident;
        quote! {
            #name::#v_name(worker) => worker.reset().await,
        }
    });

    let expanded = quote! {
        impl WorkerDispatcher for #name {
            fn name(&self) -> &str {
                match self {
                    #(#name_arms)*
                }
            }
            async fn init(&mut self) -> anyhow::Result<()> {
                match self {
                    #(#init_arms)*
                }
            }

            async fn run(&mut self) -> anyhow::Result<()> {
                match self {
                    #(#run_arms)*
                }
            }

            async fn reset(&mut self) -> anyhow::Result<()> {
                match self {
                    #(#reset_arms)*
                }
            }
        }
    };

    TokenStream::from(expanded)
}
