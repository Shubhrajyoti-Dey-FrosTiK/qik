use prost_types::Struct;
use serde_json::Value as JsonValue;

pub fn struct_to_json(proto_struct: &Struct) -> JsonValue {
    let mut map = serde_json::Map::new();
    for (key, value) in &proto_struct.fields {
        map.insert(key.clone(), prost_value_to_json_value(value));
    }
    JsonValue::Object(map)
}

// Convert a prost_types::Value to serde_json::Value
pub fn prost_value_to_json_value(proto_value: &prost_types::Value) -> JsonValue {
    if let Some(kind) = &proto_value.kind {
        match kind {
            prost_types::value::Kind::NullValue(_) => JsonValue::Null,
            prost_types::value::Kind::BoolValue(b) => JsonValue::Bool(*b),
            prost_types::value::Kind::NumberValue(n) => JsonValue::Number(
                serde_json::Number::from_f64(*n).unwrap_or(serde_json::Number::from(0)),
            ),
            prost_types::value::Kind::StringValue(s) => JsonValue::String(s.clone()),
            prost_types::value::Kind::ListValue(list) => {
                JsonValue::Array(list.values.iter().map(prost_value_to_json_value).collect())
            }
            prost_types::value::Kind::StructValue(s) => struct_to_json(s),
        }
    } else {
        JsonValue::Null
    }
}
