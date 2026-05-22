use ligo_lw::{parse_bytes, LigoType, Value};

const SAMPLE: &[u8] = include_bytes!("fixtures/sample_coinc.xml");

#[test]
fn parses_table_metadata() {
    let doc = parse_bytes(SAMPLE).expect("parse should succeed");
    let coinc = doc.require_table("coinc_inspiral").unwrap();
    assert_eq!(coinc.columns.len(), 8);
    assert_eq!(coinc.columns[0].name, "coinc_event_id");
    assert_eq!(coinc.columns[0].ty, LigoType::Ilwd);
    assert_eq!(coinc.columns[1].name, "ifos");
    assert_eq!(coinc.columns[1].ty, LigoType::Str);
    assert_eq!(coinc.columns[7].name, "combined_far");
    assert_eq!(coinc.columns[7].ty, LigoType::Real8);
    assert_eq!(coinc.rows.len(), 1);
}

#[test]
fn parses_stream_values_with_quoted_ifos() {
    let doc = parse_bytes(SAMPLE).unwrap();
    let coinc = doc.require_table("coinc_inspiral").unwrap();
    // Quoted string with embedded commas must be preserved as one cell.
    let ifos = coinc.require_cell(0, "ifos").unwrap();
    assert_eq!(ifos, &Value::Str("H1,L1,V1".to_string()));
    let far = coinc.require_cell(0, "combined_far").unwrap();
    match far {
        Value::Real(v) => assert!((v - 1.23e-9).abs() < 1e-20),
        other => panic!("expected real, got {:?}", other),
    }
}

#[test]
fn parses_sngl_inspiral_rows() {
    let doc = parse_bytes(SAMPLE).unwrap();
    let sngl = doc.require_table("sngl_inspiral").unwrap();
    assert_eq!(sngl.rows.len(), 3);
    let h1_snr = sngl.require_cell(0, "snr").unwrap();
    assert_eq!(h1_snr, &Value::Real(12.3));
    let l1_ifo = sngl.require_cell(1, "ifo").unwrap();
    assert_eq!(l1_ifo, &Value::Str("L1".to_string()));
}

#[test]
fn coinc_inspirals_accessor_joins_sngls() {
    let doc = parse_bytes(SAMPLE).unwrap();
    let events = doc.coinc_inspirals().unwrap();
    assert_eq!(events.len(), 1);
    let ev = &events[0];
    assert_eq!(ev.ifos, "H1,L1,V1");
    assert!((ev.combined_far - 1.23e-9).abs() < 1e-20);
    assert!((ev.snr - 17.6).abs() < 1e-9);
    // end_time = 1187008882 + 0.42 sec
    assert!((ev.end_time - 1_187_008_882.42).abs() < 1e-6);
    assert_eq!(ev.sngls.len(), 3);
    let h1 = ev.sngls.iter().find(|s| s.ifo == "H1").unwrap();
    assert!((h1.snr - 12.3).abs() < 1e-9);
    assert_eq!(h1.mass1, Some(1.4));
}

#[test]
fn parses_process_metadata() {
    let doc = parse_bytes(SAMPLE).unwrap();
    let process = doc.require_table("process").unwrap();
    assert_eq!(process.rows.len(), 1);
    let program = process.require_cell(0, "program").unwrap();
    assert_eq!(program, &Value::Str("gstlal_inspiral".to_string()));
}

#[test]
fn empty_field_between_delimiters_is_null() {
    // Three columns, second cell empty -> Null.
    let xml = br#"<?xml version='1.0'?>
<LIGO_LW>
  <Table Name="demo:table">
    <Column Name="demo:a" Type="real_8"/>
    <Column Name="demo:b" Type="real_8"/>
    <Column Name="demo:c" Type="real_8"/>
    <Stream Name="demo:table" Type="Local" Delimiter=",">1.0,,3.0</Stream>
  </Table>
</LIGO_LW>
"#;
    let doc = parse_bytes(xml).unwrap();
    let t = doc.require_table("demo").unwrap();
    assert_eq!(t.rows.len(), 1);
    assert_eq!(t.rows[0][0], Value::Real(1.0));
    assert_eq!(t.rows[0][1], Value::Null);
    assert_eq!(t.rows[0][2], Value::Real(3.0));
}

#[test]
fn doubled_quote_inside_string_is_literal_quote() {
    let xml = br#"<?xml version='1.0'?>
<LIGO_LW>
  <Table Name="demo:table">
    <Column Name="demo:s" Type="lstring"/>
    <Stream Name="demo:table" Type="Local" Delimiter=",">"a ""b"" c"</Stream>
  </Table>
</LIGO_LW>
"#;
    let doc = parse_bytes(xml).unwrap();
    let t = doc.require_table("demo").unwrap();
    assert_eq!(t.rows[0][0], Value::Str("a \"b\" c".to_string()));
}
