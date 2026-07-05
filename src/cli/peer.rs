use crate::api::schema::{EmptyParams, Method, PeerTarget, Request};

pub(super) fn run_peer_command(args: &[String]) -> std::io::Result<i32> {
    match args.first().map(|arg| arg.as_str()) {
        Some("list") if args.len() == 1 => peer_list(),
        Some("health") => peer_health(&args[1..]),
        Some("unregister") => peer_unregister(&args[1..]),
        Some("help" | "--help" | "-h") => {
            print_peer_help();
            Ok(0)
        }
        _ => {
            print_peer_help();
            Ok(2)
        }
    }
}

fn peer_list() -> std::io::Result<i32> {
    super::print_response(&super::send_request(&Request {
        id: "cli:peer:list".into(),
        method: Method::PeerList(EmptyParams::default()),
    })?)
}

fn peer_health(args: &[String]) -> std::io::Result<i32> {
    let Some(peer_id) = args.first() else {
        eprintln!("usage: herdr peer health <peer_id>");
        return Ok(2);
    };
    if args.len() != 1 {
        eprintln!("usage: herdr peer health <peer_id>");
        return Ok(2);
    }
    super::print_response(&super::send_request(&Request {
        id: "cli:peer:health".into(),
        method: Method::PeerHealth(PeerTarget {
            peer_id: peer_id.clone(),
        }),
    })?)
}

fn peer_unregister(args: &[String]) -> std::io::Result<i32> {
    let Some(peer_id) = args.first() else {
        eprintln!("usage: herdr peer unregister <peer_id>");
        return Ok(2);
    };
    if args.len() != 1 {
        eprintln!("usage: herdr peer unregister <peer_id>");
        return Ok(2);
    }
    super::print_response(&super::send_request(&Request {
        id: "cli:peer:unregister".into(),
        method: Method::PeerUnregister(PeerTarget {
            peer_id: peer_id.clone(),
        }),
    })?)
}

fn print_peer_help() {
    eprintln!("herdr peer commands:");
    eprintln!("  herdr peer list                  list registered peers");
    eprintln!("  herdr peer health <peer_id>      check peer reachability");
    eprintln!("  herdr peer unregister <peer_id>  remove a peer");
}
