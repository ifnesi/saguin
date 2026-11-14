// Compiled beside the pinned suite, with its modules declared by prepare.py.
// This entry point selects tests; the upstream test bodies and report are used
// unchanged. Selecting by description distinguishes repeated normative IDs.
#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 7, "module description tcp-port tls-port ca-file ws-port");
    let addr = format!("127.0.0.1:{}", args[3]);
    let tls_addr = format!("127.0.0.1:{}", args[4]);
    let ws_addr = format!("127.0.0.1:{}", args[6]);
    let tls = client::TlsConfig::build(Some(std::path::Path::new(&args[5])), false, "127.0.0.1")
        .expect("TLS fixture");
    let config = types::TestConfig {
        addr: &addr,
        recv_timeout: std::time::Duration::from_secs(5),
        tls_info: Some((&tls_addr, &tls)),
        ws_info: Some((&ws_addr, "127.0.0.1", "/mqtt")),
    };
    let runner = registry(&args[1], config).select(&args[2]);
    assert_eq!(runner.count(), 1, "selection must identify exactly one test");
    let pb = indicatif::ProgressBar::hidden();
    let mut report = report::Report::new();
    report.capabilities = capabilities::BrokerCapabilities::probe(&addr, config.recv_timeout).await.ok();
    let start = std::time::Instant::now();
    report.add(runner.run(&pb).await);
    report.print(true, report::ReportOrder::Suite, false, start.elapsed());
}
