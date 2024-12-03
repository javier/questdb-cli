// src/completer.rs

use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{Validator, ValidationResult, ValidationContext};
use rustyline::{Context};
use rustyline::error::ReadlineError;

#[derive(Clone)]
pub struct SQLCompleter {
    pub keywords: Vec<String>,
    pub meta_commands: Vec<String>,
    pub tables: Vec<String>,
}

impl SQLCompleter {
    pub fn new() -> Self {
        Self {
            keywords: vec![
                "SELECT".to_string(),
                "FROM".to_string(),
                "WHERE".to_string(),
                "INSERT".to_string(),
                "UPDATE".to_string(),
                "DELETE".to_string(),
                "LIMIT".to_string(),
                "JOIN".to_string(),
                "ON".to_string(),
                // Add more SQL keywords as needed
            ],
            meta_commands: vec![
                "\\help".to_string(),
                "\\q".to_string(),
                "\\format".to_string(),
                "\\dt".to_string(),
                "\\dwal".to_string(),
                "\\dstorage".to_string(),
                "\\refresh".to_string(),
                // Add more meta commands as needed
            ],
            tables: vec![], // Will be populated dynamically
        }
    }

    pub async fn update_tables(&mut self, client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
        let rows = client.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';", &[]).await?;
        self.tables = rows.iter()
            .filter_map(|row| row.get::<_, Option<String>>(0))
            .collect();
        Ok(())
    }
}

impl Completer for SQLCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let start = line[..pos].rfind(|c: char| c.is_whitespace()).map_or(0, |idx| idx + 1);
        let word = &line[start..pos].to_uppercase();
        let candidates = self.keywords.iter()
            .filter(|kw| kw.starts_with(word))
            .chain(self.meta_commands.iter().filter(|cmd| cmd.starts_with(&line[start..pos])))
            .chain(self.tables.iter().filter(|tbl| tbl.to_uppercase().starts_with(word)))
            .map(|kw| Pair {
                display: kw.clone(),
                replacement: kw.clone(),
            })
            .collect();
        Ok((start, candidates))
    }
}

impl Hinter for SQLCompleter {
    type Hint = String;
}

impl Validator for SQLCompleter {
    fn validate(&self, _ctx: &mut ValidationContext<'_>) -> Result<ValidationResult, ReadlineError> {
        Ok(ValidationResult::Valid(None))
    }
}

impl Highlighter for SQLCompleter {}

impl rustyline::Helper for SQLCompleter {}
