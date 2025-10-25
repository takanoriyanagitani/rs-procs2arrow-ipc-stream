use rs_procs2arrow_ipc_stream::SystemInfo;

fn main() {
    if let Err(e) = SystemInfo::procs2ipc2stdout(1024) {
        eprintln!("Error: {}", e);
    }
}
