// src/output.rs

use tokio_postgres::{Row, types::Type, Column};
use serde::Serialize;
use prettytable::{Table, Row as PrettyRow, Cell};
use csv::WriterBuilder;
use anyhow::Result;
use chrono::NaiveDateTime;

#[derive(Serialize)]
struct JsonRow {
    columns: Vec<String>,
    values: Vec<Option<serde_json::Value>>,
}

pub enum OutputFormat {
    Table,
    Csv,
    Json,
    Vertical,
    Record,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "table" => OutputFormat::Table,
            "csv" => OutputFormat::Csv,
            "json" => OutputFormat::Json,
            "vertical" => OutputFormat::Vertical,
            "record" => OutputFormat::Record,
            _ => OutputFormat::Table,
        }
    }
}

pub async fn print_query_results(rows: Vec<Row>, format: &str) -> Result<()> {
    let output_format = OutputFormat::from_str(format);

    match output_format {
        OutputFormat::Table => print_table(&rows).await?,
        OutputFormat::Csv => print_csv(&rows).await?,
        OutputFormat::Json => print_json(&rows).await?,
        OutputFormat::Vertical => print_vertical(&rows).await?,
        OutputFormat::Record => print_record(&rows).await?,
    }

    Ok(())
}

async fn print_table(rows: &[Row]) -> Result<()> {
    if rows.is_empty() {
        println!("(No rows returned)");
        return Ok(());
    }

    let columns = rows[0].columns();
    let headers: Vec<&str> = columns.iter().map(|col| col.name()).collect();

    let mut table = Table::new();
    table.add_row(PrettyRow::new(
        headers.iter().map(|h| Cell::new(h)).collect(),
    ));

    for row in rows {
        let mut table_row = PrettyRow::new(Vec::new());
        for (i, column) in row.columns().iter().enumerate() {
            let value = get_column_value(row, column, i).await?;
            table_row.add_cell(Cell::new(&value));
        }
        table.add_row(table_row);
    }

    table.printstd();
    Ok(())
}

async fn print_csv(rows: &[Row]) -> Result<()> {
    if rows.is_empty() {
        println!("(No rows returned)");
        return Ok(());
    }

    let columns = rows[0].columns();
    let headers: Vec<&str> = columns.iter().map(|col| col.name()).collect();

    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_writer(std::io::stdout());

    wtr.write_record(&headers)?;

    for row in rows {
        let mut record = Vec::new();
        for (i, column) in row.columns().iter().enumerate() {
            let value = get_column_value(row, column, i).await?;
            record.push(value);
        }
        wtr.write_record(&record)?;
    }

    wtr.flush()?;
    Ok(())
}

async fn print_json(rows: &[Row]) -> Result<()> {
    if rows.is_empty() {
        println!("[]");
        return Ok(());
    }

    let columns = rows[0].columns();
    let mut json_rows = Vec::new();

    for row in rows {
        let mut json_row = serde_json::Map::new();
        for (i, col) in columns.iter().enumerate() {
            let value = match *col.type_() {
                Type::TIMESTAMP | Type::TIMESTAMPTZ => {
                    row.try_get::<_, Option<NaiveDateTime>>(i)?
                        .map(|v| serde_json::Value::String(v.format("%Y-%m-%d %H:%M:%S").to_string()))
                        .unwrap_or(serde_json::Value::Null)
                },
                Type::FLOAT8 => {
                    row.try_get::<_, Option<f64>>(i)?
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null)
                },
                Type::VARCHAR => {
                    row.try_get::<_, Option<String>>(i)?
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null)
                },
                Type::BOOL => {
                    row.try_get::<_, Option<bool>>(i)?
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null)
                },
                Type::INT4 => {
                    row.try_get::<_, Option<i32>>(i)?
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null)
                },
                _ => serde_json::Value::String("Unsupported Type".to_string()),
            };
            json_row.insert(col.name().to_string(), value);
        }
        json_rows.push(serde_json::Value::Object(json_row));
    }

    let serialized = serde_json::to_string_pretty(&json_rows)?;
    println!("{}", serialized);
    Ok(())
}

async fn print_vertical(rows: &[Row]) -> Result<()> {
    if rows.is_empty() {
        println!("(No rows returned)");
        return Ok(());
    }

    let columns = rows[0].columns();

    for (row_num, row) in rows.iter().enumerate() {
        println!("Row {}:", row_num + 1);
        for (i, col) in columns.iter().enumerate() {
            let value = get_column_value(row, col, i).await?;
            println!("  {}: {}", col.name(), value);
        }
        println!();
    }

    Ok(())
}

async fn print_record(rows: &[Row]) -> Result<()> {
    if rows.is_empty() {
        println!("(No rows returned)");
        return Ok(());
    }

    let columns = rows[0].columns();

    for row in rows {
        let mut record = Vec::new();
        for (i, column) in columns.iter().enumerate() {
            let value = get_column_value(row, column, i).await?;
            record.push(format!("{}: {}", column.name(), value));
        }
        println!("{}", record.join(", "));
    }

    Ok(())
}

// Helper function to retrieve and format column values
async fn get_column_value(row: &Row, column: &Column, i: usize) -> Result<String> {
    let value = match *column.type_() {
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            row.try_get::<_, Option<NaiveDateTime>>(i)?
                .map(|v| v.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
                .unwrap_or_else(|| "NULL".to_string())
        },
        Type::FLOAT8 => {
            row.try_get::<_, Option<f64>>(i)?
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string())
        },
        Type::VARCHAR => {
            row.try_get::<_, Option<String>>(i)?
                .unwrap_or_else(|| "NULL".to_string())
        },
        Type::BOOL => {
            row.try_get::<_, Option<bool>>(i)?
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string())
        },
        Type::INT4 => {
            row.try_get::<_, Option<i32>>(i)?
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string())
        },
        _ => "Unsupported Type".to_string(),
    };
    Ok(value)
}
