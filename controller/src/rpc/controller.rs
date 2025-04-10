use super::util::struct_to_json;
use anyhow::Result;
use serde_json::to_string;
tonic::include_proto!("controller");

impl AddJobRequest {
    pub fn get_item_string(&self) -> Result<String> {
        if self.item.is_none() {
            return Ok(String::new());
        }
        let item_json = struct_to_json(&self.item.clone().unwrap());
        Ok(to_string(&item_json).unwrap())
    }
}
