#[macro_use]
extern crate log;

use anyhow::{anyhow, Result};
use clap::Parser;
use http_body_util::{BodyExt, Either};
use hyper::body::Buf;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

#[derive(Parser)]
#[command(version)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:30030")]
    bind: SocketAddr,

    #[arg(short, long)]
    to: String,

    #[arg(short, long)]
    modify_records_path: PathBuf
}

#[derive(Deserialize)]
enum Action {
    Insert {
        data: Map<String, Value>
    }
}

#[derive(Deserialize)]
struct ModifyRecord {
    path_prefix: String,
    action: Action
}

struct Context {
    records: Vec<ModifyRecord>,
    dst: String,
}

fn body_insert(body: &mut Map<String, Value>, record_data: &Map<String, Value>) {
    for (k, v) in record_data.iter() {
        match body.get_mut(k) {
            None => {
                body.insert(k.clone(), v.clone());
            },
            Some(Value::Object(body)) => {
                if let Some(v) = v.as_object() {
                    body_insert(body, v);
                }
            }
            _ => {}
        }
    }
}

async fn proxy(
    ctx: &Arc<Context>,
    req: Request<Incoming>
) -> Result<Response<Either<Incoming, String>>> {
    let (mut parts, body) = req.into_parts();

    let path = parts.uri.path();
    let body = body.collect().await?.aggregate();
    let body = body.reader();

    let mut body = match serde_json::from_reader::<_, Map<String, Value>>(body) {
        Ok(v) => v,
        Err(_) => {
            let ret = Response::builder()
                .status(400)
                .body(Either::Right(String::from("Invalid JSON Body")))?;

            return Ok(ret);
        }
    };

    for record in ctx.records.iter() {
        if path.starts_with(&record.path_prefix) {
            match &record.action {
                Action::Insert {
                    data: record_data
                } => {
                    body_insert(&mut body, record_data);
                }
            }
        }
    }

    let stream = TcpStream::connect(&ctx.dst).await?;
    let stream = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(stream).await?;
    tokio::spawn(conn);

    parts.headers.insert("host", ctx.dst.parse()?);
    println!("parts: {:?}, body: {:?}", parts, body);
    let req = Request::from_parts(parts, serde_json::to_string(&body)?);
    println!("before send_request");
    let resp = match sender.send_request(req).await {
        Ok(resp) => resp,
        Err(e) => {
            error!("{}", e);
            return Err(anyhow!(e))
        }
    };
    println!("after send_request");
    Ok(resp.map(|v| Either::Left(v)))
}

fn logger_init() -> Result<()> {
    let pattern = if cfg!(debug_assertions) {
        "[{d(%Y-%m-%d %H:%M:%S)}] {h({l})} {f}:{L} - {m}{n}"
    } else {
        "[{d(%Y-%m-%d %H:%M:%S)}] {h({l})} {t} - {m}{n}"
    };

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build();

    let config = log4rs::Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(
            Root::builder()
                .appender("stdout")
                .build(LevelFilter::from_str(
                    &std::env::var("MODIFY_HTTP_MID_LOG").unwrap_or_else(|_| String::from("INFO")),
                )?),
        )?;

    log4rs::init_config(config)?;
    Ok(())
}

fn launch(args: Args) -> Result<()> {
    logger_init()?;
    let rt = Runtime::new()?;
    let records: Vec<ModifyRecord> = serde_json::from_reader(std::fs::File::open(args.modify_records_path)?)?;

    rt.block_on(async {
        let listener = TcpListener::bind(&args.bind).await?;
        info!("api listening on http://{}", args.bind);

        let ctx = Context {
            records,
            dst: args.to,
        };

        let ctx = Arc::new(ctx);

        loop {
            let (stream, _) = listener.accept().await?;
            let stream = hyper_util::rt::TokioIo::new(stream);
            let ctx = ctx.clone();

            tokio::spawn(async move {
                let ctx = &ctx;

                let res = http1::Builder::new()
                    .serve_connection(
                        stream,
                        service_fn(move |req| {
                            proxy(ctx, req)
                        }),
                    )
                    .await;

                if let Err(e) = res {
                    warn!("error serving connection: {:?}", e);
                }
            });
        }
    })
}

fn main() -> ExitCode {
    let args: Args = Args::parse();

    match launch(args) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{}", e);
            ExitCode::FAILURE
        }
    }
}
