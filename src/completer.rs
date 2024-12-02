// src/completer.rs
use anyhow::Result;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper};
use tokio_postgres::Client;

pub struct SQLCompleter {
    pub keywords: Vec<String>,
    pub meta_commands: Vec<String>,
    pub tables: Vec<String>,
}

impl SQLCompleter {
    pub fn new() -> Self {
        SQLCompleter {
            keywords: vec![
                "SELECT".to_string(),
                "FROM".to_string(),
                "WHERE".to_string(),
                "INSERT".to_string(),
                "UPDATE".to_string(),
                "DELETE".to_string(),
                "CREATE".to_string(),
                "DROP".to_string(),
                "ALTER".to_string(),
                "JOIN".to_string(),
                // Add more SQL keywords as needed
            ],
            meta_commands: vec![
                "\\help".to_string(),
                "\\q".to_string(),
                "\\dt".to_string(),
                "\\dwal".to_string(),
                "\\dstorage".to_string(),
                "\\refresh".to_string(),
            ],
            tables: Vec::new(),
        }
    }

    pub async fn update_tables(&mut self, client: &Client) -> Result<()> {
        let rows = client
            .query("SELECT table_name FROM tables()", &[])
            .await?;

        self.tables = rows
            .iter()
            .filter_map(|row| row.get::<_, Option<String>>(0))
            .collect();

        Ok(())
    }
}

impl Helper for SQLCompleter {}

impl Completer for SQLCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let start = line[..pos]
            .rfind(|c: char| c.is_whitespace())
            .map_or(0, |i| i + 1);
        let word = &line[start..pos];
        let word_upper = word.to_uppercase();
        let mut matches = Vec::new();

        // Match keywords
        for kw in &self.keywords {
            if kw.starts_with(&word_upper) {
                matches.push(Pair {
                    display: kw.clone(),
                    replacement: kw.clone(),
                });
            }
        }

        // Match meta commands
        for cmd in &self.meta_commands {
            if cmd.starts_with(word) {
                matches.push(Pair {
                    display: cmd.clone(),
                    replacement: cmd.clone(),
                });
            }
        }

        // Match table names
        for table in &self.tables {
            if table.starts_with(word) {
                matches.push(Pair {
                    display: table.clone(),
                    replacement: table.clone(),
                });
            }
        }

        Ok((start, matches))
    }
}

impl Hinter for SQLCompleter {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        None
    }
}

impl Highlighter for SQLCompleter {}

impl Validator for SQLCompleter {
    fn validate(&self, _context: &mut ValidationContext<'_>) -> rustyline::Result<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

