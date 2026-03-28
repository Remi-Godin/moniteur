use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse_macro_input};

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

#[proc_macro_derive(ConfigRegistry)]
pub fn derive_supervised_worker(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let ast = parse_macro_input!(input as DeriveInput);
    let enum_name = &ast.ident;

    // Generate the Config enum name (e.g., GatewayWorker -> GatewayConfig)
    let mut config_name_str = enum_name.to_string();
    config_name_str.push_str("Config");
    let config_name = format_ident!("{}", config_name_str);

    // Extract the variants from the enum
    let variants = match ast.data {
        Data::Enum(ref e) => &e.variants,
        _ => panic!("SupervisedWorker can only be derived on enums"),
    };

    let mut variant_idents = Vec::new();
    let mut inner_types = Vec::new();

    for variant in variants {
        variant_idents.push(&variant.ident);

        // Extract the inner type (e.g., TagReaderWorker from Reader(TagReaderWorker))
        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                inner_types.push(&fields.unnamed.first().unwrap().ty);
            }
            _ => panic!(
                "All variants must have exactly one unnamed field (e.g., Variant(WorkerType))"
            ),
        }
    }

    // Generate the output Rust code
    let expanded = quote! {
        // 1. Generate the Sibling Config Enum
        #[derive(Clone, PartialEq)]
        pub enum #config_name {
            #( #variant_idents(<#inner_types as Worker>::Config) ),*
        }

        #(
            impl From<<#inner_types as Worker>::Config> for #config_name {
                fn from(inner: <#inner_types as Worker>::Config) -> Self {
                    #config_name::#variant_idents(inner)
                }
            }
        )*

        // 3. Implement the Worker Trait for the Enum
        impl Worker for #enum_name {
            type Config = #config_name;

            fn name(config: &Self::Config) -> &str {
                match config {
                    #( #config_name::#variant_idents(c) => <#inner_types as Worker>::name(c), )*
                }
            }

            // Using async fn to satisfy your 'impl Future' trait definition
            async fn init(config: &Self::Config) -> anyhow::Result<Self> {
                match config {
                    #( #config_name::#variant_idents(c) => {
                        let worker = <#inner_types as Worker>::init(c).await?;
                        Ok(Self::#variant_idents(worker))
                    })*
                }
            }

            async fn run(&mut self) -> anyhow::Result<()> {
                match self {
                    #( Self::#variant_idents(w) => w.run().await, )*
                }
            }
        }
    };

    TokenStream::from(expanded)
}
