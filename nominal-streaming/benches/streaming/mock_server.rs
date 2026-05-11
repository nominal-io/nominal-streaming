use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::UNIX_EPOCH;

const HTTP_SIMULATED_LATENCY: Duration = Duration::from_millis(20);

pub fn spawn_mock_http_server(latency_samples: Arc<Mutex<Vec<u64>>>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            let samples = Arc::clone(&latency_samples);
            std::thread::spawn(move || serve_connection(conn, samples));
        }
    });
    port
}

fn serve_connection(stream: TcpStream, latency_samples: Arc<Mutex<Vec<u64>>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;
    loop {
        let mut content_length: usize = 0;
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                _ => {}
            }
            if line.to_ascii_lowercase().starts_with("content-length:") {
                content_length = line["content-length:".len()..].trim().parse().unwrap_or(0);
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
        }
        let mut body = vec![0u8; content_length];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
        let arrived_ns = UNIX_EPOCH.elapsed().unwrap().as_nanos() as u64;
        if let Some(enqueue_ns) = decode_enqueue_ns(&body) {
            if arrived_ns >= enqueue_ns {
                latency_samples
                    .lock()
                    .unwrap()
                    .push(arrived_ns - enqueue_ns);
            }
        }
        std::thread::sleep(HTTP_SIMULATED_LATENCY);
        if writer
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .is_err()
        {
            return;
        }
    }
}

fn read_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *buf.get(*pos)?;
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

fn skip_proto_field(buf: &[u8], pos: &mut usize, wire_type: u8) -> Option<()> {
    match wire_type {
        0 => {
            read_varint(buf, pos)?;
        }
        1 => {
            *pos = pos.checked_add(8)?;
        }
        2 => {
            let n = read_varint(buf, pos)? as usize;
            *pos = pos.checked_add(n)?;
        }
        5 => {
            *pos = pos.checked_add(4)?;
        }
        _ => return None,
    }
    Some(())
}

fn proto_descend(buf: &[u8], target_field: u32) -> Option<&[u8]> {
    let mut pos = 0usize;
    while pos < buf.len() {
        let tag = read_varint(buf, &mut pos)? as u32;
        let field_num = tag >> 3;
        let wire_type = (tag & 0x7) as u8;
        if field_num == target_field {
            if wire_type != 2 {
                return None;
            }
            let len = read_varint(buf, &mut pos)? as usize;
            let end = pos.saturating_add(len).min(buf.len());
            return Some(&buf[pos..end]);
        }
        skip_proto_field(buf, &mut pos, wire_type)?;
    }
    None
}

pub fn decode_enqueue_ns(compressed_body: &[u8]) -> Option<u64> {
    let mut buf = Vec::new();
    snap::read::FrameDecoder::new(compressed_body)
        .read_to_end(&mut buf)
        .ok()?;
    // WriteRequestNominal.series[0] (field 1)
    //   .points (field 3)
    //   .double_points (field 1)
    //   .points[0] (field 1)
    //   .timestamp (field 1) → seconds (field 1, varint) + nanos (field 2, varint)
    let series = proto_descend(&buf, 1)?;
    let points_msg = proto_descend(series, 3)?;
    let double_points = proto_descend(points_msg, 1)?;
    let first_point = proto_descend(double_points, 1)?;
    let ts = proto_descend(first_point, 1)?;
    let mut pos = 0;
    let mut seconds = 0i64;
    let mut nanos = 0i64;
    while pos < ts.len() {
        let tag = read_varint(ts, &mut pos)? as u32;
        match (tag >> 3, (tag & 7) as u8) {
            (1, 0) => seconds = read_varint(ts, &mut pos)? as i64,
            (2, 0) => nanos = read_varint(ts, &mut pos)? as i64,
            (_, wt) => skip_proto_field(ts, &mut pos, wt)?,
        }
    }
    Some(seconds as u64 * 1_000_000_000 + nanos as u64)
}
