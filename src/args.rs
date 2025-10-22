use crate::JoinResult;
use clap::{
    //ArgAction,
    Command,
    CommandFactory,
    Parser,
};
use serde::{Deserialize, Serialize};
use std::{default, path::PathBuf};

// https://stackoverflow.com/questions/74068168/clap-rs-not-printing-colors-during-help
pub fn get_styles() -> clap::builder::Styles {
    let cyan = anstyle::Color::Ansi(anstyle::AnsiColor::Cyan);
    let green = anstyle::Color::Ansi(anstyle::AnsiColor::Green);
    let yellow = anstyle::Color::Ansi(anstyle::AnsiColor::Yellow);

    clap::builder::Styles::styled()
        .placeholder(anstyle::Style::new().fg_color(Some(yellow)))
        .usage(anstyle::Style::new().fg_color(Some(cyan)).bold())
        .header(
            anstyle::Style::new()
                .fg_color(Some(cyan))
                .bold()
                .underline(),
        )
        .literal(anstyle::Style::new().fg_color(Some(green)))
}

/*
https://www.perrygeo.com/getting-started-with-application-configuration-in-rust.html
https://stackoverflow.com/questions/55133351/is-there-a-way-to-get-clap-to-use-default-values-from-a-file
https://rust-cli.github.io/book/in-depth/config-files.html
https://docs.rs/confy/latest/confy/index.html

How to Set Environment Variables in Linux:
export DELIMITER_FILE1='|'

How to Print Environment Variables in Linux:
printenv DELIMITER_FILE1
or
echo $DELIMITER_FILE1

Removing shell variable and values:
unset DELIMITER_FILE1
*/

/// Read command line arguments with priority order:
/// 1. command line arguments
/// 2. environment
/// 3. config file
/// 4. defaults
///
/// At the end add or update config file.
#[derive(Debug, Clone, PartialEq, Parser, Serialize, Deserialize)]
#[command( // Read from `Cargo.toml`
    author, version, about,
    long_about = None,
    next_line_help = true,
    styles=get_styles(),
)]
pub struct Arguments {
    /// The first file with CSV format.
    ///
    /// Left side in DataFrame
    #[arg(short('1'), long, required = true)]
    pub file1: Option<PathBuf>,

    /// The second file with CSV format.
    ///
    /// Right side in DataFrame
    #[arg(short('2'), long, required = true)]
    pub file2: Option<PathBuf>,

    /// Enter the delimiter for the first input csv file.
    ///
    /// The default delimiter is `|`
    #[arg(short('a'), long, env("DELIMITER_INPUT_1"), required = false)]
    pub delimiter_input_1: Option<char>,

    /// Enter the delimiter for the second input csv file.
    ///
    /// The default delimiter is `;`
    #[arg(short('b'), long, env("DELIMITER_INPUT_2"), required = false)]
    pub delimiter_input_2: Option<char>,

    /// Enter the delimiter for the output csv file.
    ///
    /// The default is `;`
    #[arg(short('d'), long, env("DELIMITER_OUTPUT"), required = false)]
    pub delimiter_output: Option<char>,

    /// Calculation Period: start date (format: yyyymm)
    ///
    /// Período de Apuração Inicial
    ///
    /// Exemple: 202001
    #[arg(short('s'), long, required = false)]
    pub date_start: Option<u32>,

    /// Calculation Period: final date (format: yyyymm)
    ///
    /// Período de Apuração Final
    ///
    /// Exemple: 202312
    #[arg(short('f'), long, required = false)]
    pub date_final: Option<u32>,

    /// Apply filter: Retain only credit entries (50 <= CST <= 66)
    ///
    /// Reter apenas operações de crédito
    ///
    /// Ou seja, imprimir nos arquivos finais itens de operações com alguna
    ///
    /// Natureza da Base de Cálculo.
    #[arg(short, long, required = false)]
    pub operacoes_de_creditos: Option<bool>,

    /// Print CSV files
    #[arg(short('c'), long, required = false)]
    pub print_csv: Option<bool>,

    /// Print Excel files
    #[arg(short('e'), long, required = false, default_value = "true")]
    pub print_excel: Option<bool>,

    /// Print PARQUET files
    #[arg(short('p'), long, required = false)]
    pub print_parquet: Option<bool>,

    /// Eliminate columns that contain only null values.
    // #[arg(short('r'), long, required = false, action=ArgAction::SetTrue)]
    #[arg(short('r'), long, required = false)]
    pub remove_null_columns: Option<bool>,

    /// Print additional information in the terminal
    #[arg(short('v'), long, required = false)]
    verbose: Option<bool>,
}

/// confy needs to implement the default Arguments.
impl default::Default for Arguments {
    fn default() -> Self {
        Arguments {
            file1: None,
            file2: None,
            delimiter_input_1: Some('|'),
            delimiter_input_2: Some(';'),
            delimiter_output: Some(';'),
            date_start: None,
            date_final: None,
            operacoes_de_creditos: Some(false),
            print_csv: Some(false),
            print_excel: Some(true),
            print_parquet: Some(false),
            remove_null_columns: Some(true),
            verbose: Some(true),
        }
    }
}

impl Arguments {
    /// Build Arguments struct
    pub fn build() -> JoinResult<Self> {
        let app: Command = Arguments::command();
        let app_name: &str = app.get_name();

        let args: Arguments = Arguments::parse()
            .get_config_file(app_name)?
            .set_config_file(app_name)?
            .print_config_file(app_name)?;

        Ok(args)
    }

    /// Get configuration file.
    ///
    /// A new configuration file is created with default values if none exists.
    fn get_config_file(mut self, app_name: &str) -> JoinResult<Self> {
        let config_file: Arguments = confy::load(app_name, None)?;

        self.file1 = self.file1.or(config_file.file1);
        self.file2 = self.file2.or(config_file.file2);
        self.delimiter_input_1 = self.delimiter_input_1.or(config_file.delimiter_input_1);
        self.delimiter_input_2 = self.delimiter_input_2.or(config_file.delimiter_input_2);
        self.delimiter_output = self.delimiter_output.or(config_file.delimiter_output);
        self.date_start = self.date_start.or(config_file.date_start);
        self.date_final = self.date_final.or(config_file.date_final);
        self.remove_null_columns = self.remove_null_columns.or(config_file.remove_null_columns);
        self.verbose = self.verbose.or(config_file.verbose);

        Ok(self)
    }

    /// Save changes made to a configuration object
    fn set_config_file(self, app_name: &str) -> JoinResult<Self> {
        confy::store(app_name, None, self.clone())?;
        Ok(self)
    }

    /// Print configuration file path and its contents
    ///
    /// ~/.config/join_with_assignments/default-config.toml
    fn print_config_file(self, app_name: &str) -> JoinResult<Self> {
        if self.verbose.unwrap_or(true) {
            let file_path: PathBuf = confy::get_configuration_file_path(app_name, None)?;
            println!("Configuration file: '{}'", file_path.display());

            let toml: String = toml::to_string_pretty(&self)?;
            println!("\t{}", toml.replace('\n', "\n\t"));
        }

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::Arguments;

    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --show-output

    #[test]
    /// `cargo test -- --show-output teste_de_logica`
    fn teste_de_logica() -> JoinResult<()> {
        let args: Arguments = Arguments {
            file1: None,
            file2: None,
            delimiter_input_1: Some('|'),
            delimiter_input_2: Some(';'),
            delimiter_output: Some(';'),
            date_start: None,
            date_final: None,
            operacoes_de_creditos: Some(false),
            print_csv: Some(false),
            print_excel: Some(true),
            print_parquet: Some(false),
            remove_null_columns: Some(true),
            verbose: Some(true),
        };

        let config_file: Arguments = Arguments {
            //file1: None,
            file1: Some("file2".into()),
            file2: None,
            date_start: None,
            date_final: None,
            delimiter_input_1: Some('|'),
            delimiter_input_2: Some(';'),
            delimiter_output: Some(';'),
            operacoes_de_creditos: Some(false),
            print_csv: Some(false),
            print_excel: Some(true),
            print_parquet: Some(false),
            remove_null_columns: Some(true),
            verbose: Some(true),
        };

        let mut args1 = Arguments::default();
        let mut args2 = Arguments::default();

        if args1.file1.is_none() && config_file.file1.is_some() {
            args1.file1 = config_file.clone().file1;
        }

        args2.file1 = args2.file1.or(config_file.clone().file1);

        println!("args:        {args:?}");
        println!("config_file: {config_file:?}\n");
        println!("args1:       {args1:?}");
        println!("args2:       {args2:?}");

        assert_eq!(args1, args2);

        Ok(())
    }
}
