use std::collections::BTreeMap;

use prost_types::{ListValue, Struct, Value};
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

// Convert a JSON string to prost_types::Struct
pub fn string_to_struct(json_string: &str) -> Result<Struct, serde_json::Error> {
    // Parse the string to a serde_json::Value
    let json_value: JsonValue = serde_json::from_str(json_string)?;

    // Convert to Struct
    Ok(json_to_struct(json_value))
}

// Convert serde_json::Value to prost_types::Struct
fn json_to_struct(json_value: JsonValue) -> Struct {
    match json_value {
        JsonValue::Object(map) => {
            let mut fields = BTreeMap::new();
            for (key, value) in map {
                fields.insert(key, json_value_to_prost_value(value));
            }
            Struct { fields }
        }
        _ => Struct {
            fields: BTreeMap::new(),
        },
    }
}

// Convert a serde_json::Value to prost_types::Value
fn json_value_to_prost_value(json_value: JsonValue) -> Value {
    let kind = match json_value {
        JsonValue::Null => prost_types::value::Kind::NullValue(0),
        JsonValue::Bool(b) => prost_types::value::Kind::BoolValue(b),
        JsonValue::Number(n) => {
            if let Some(f) = n.as_f64() {
                prost_types::value::Kind::NumberValue(f)
            } else {
                prost_types::value::Kind::NullValue(0)
            }
        }
        JsonValue::String(s) => prost_types::value::Kind::StringValue(s),
        JsonValue::Array(arr) => {
            let values: Vec<Value> = arr.into_iter().map(json_value_to_prost_value).collect();
            prost_types::value::Kind::ListValue(ListValue { values })
        }
        JsonValue::Object(_) => prost_types::value::Kind::StructValue(json_to_struct(json_value)),
    };
    Value { kind: Some(kind) }
}
