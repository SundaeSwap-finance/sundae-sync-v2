use super::filter::FilterConfig;
use anyhow::Result;
use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient};
use bytes::Bytes;
use hex::ToHex;
use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use utxorpc::spec::sync::BlockRef;

#[derive(Serialize, Deserialize, Debug)]
pub struct Destination {
    pub pk: String,
    pub sk: String,
    pub stream_arn: String,
    pub filter: Option<FilterConfig>,
    pub sequence_number: Option<String>,
    #[serde(
        serialize_with = "serialize_point",
        deserialize_with = "deserialize_point"
    )]
    pub last_seen_point: BlockRef,
    #[serde(
        serialize_with = "serialize_points",
        deserialize_with = "deserialize_points"
    )]
    pub recovery_points: Vec<BlockRef>,
    pub enabled: bool,
}

impl Destination {
    /// Commit a new point / sequence number back to the destinations table
    pub async fn commit(
        &mut self,
        dynamo: &DynamoClient,
        table: &String,
        point: BlockRef,
        seq_num: Option<String>,
    ) -> Result<()> {
        // Rotate the point into the list of 15 rollback points
        // TODO: make this more sophisticated, so that we stagger points further and further back
        // rather than just using the most recent 15
        let previous_point = self.last_seen_point.clone();
        self.last_seen_point = point.clone();
        self.recovery_points.push(point.clone());
        if self.recovery_points.len() > 15 {
            self.recovery_points.remove(0);
        }
        let recovery_points = self
            .recovery_points
            .iter()
            .map(|p| AttributeValue::S(point_to_string(&p)))
            .collect();
        // Update the destination (assuming someone else hasn't already updated it) to
        // - set the last seen point
        // - update the list of recovery points, for finding an intersect if we restart
        // - set the kinesis sequence number so we can inspect the queue on restart
        dynamo
            .update_item()
            .table_name(table)
            .key("pk", AttributeValue::S(self.pk.clone()))
            .key("sk", AttributeValue::S(self.sk.clone()))
            .condition_expression("last_seen_point = :last_point")
            .update_expression(
                "SET last_seen_point = :new_point, sequence_number = :seq, recovery_points = :rotated_points",
            )
            .expression_attribute_values(
                ":last_point",
                AttributeValue::S(point_to_string(&previous_point)),
            )
            .expression_attribute_values(":seq", seq_num.map_or(AttributeValue::Null(true), |s| AttributeValue::S(s)))
            .expression_attribute_values(":new_point", AttributeValue::S(point_to_string(&point)))
            .expression_attribute_values(":rotated_points", AttributeValue::L(recovery_points))
            .send()
            .await?;
        Ok(())
    }
}

pub fn point_to_string(point: &BlockRef) -> String {
    format!("{}/{}", point.index, point.hash.encode_hex::<String>())
}
pub fn serialize_point<S>(point: &BlockRef, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(point_to_string(point).as_str())
}
pub fn serialize_points<S>(
    points: &Vec<BlockRef>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut s = serializer.serialize_seq(Some(points.len()))?;
    for point in points {
        s.serialize_element(point_to_string(point).as_str())?;
    }
    s.end()
}

pub fn string_to_point(s: String) -> Result<BlockRef> {
    let parts: Vec<_> = s.split('/').collect();
    let index = parts[0].parse()?;
    let hash = Bytes::from_iter(hex::decode(parts[1])?.into_iter());
    Ok(BlockRef { index, hash })
}
pub fn deserialize_point<'de, D>(deserializer: D) -> std::result::Result<BlockRef, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer).and_then(|string| {
        string_to_point(string).map_err(|err| {
            serde::de::Error::custom(format!("failed to deserialize point: {}", err))
        })
    })
}
pub fn deserialize_points<'de, D>(deserializer: D) -> std::result::Result<Vec<BlockRef>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer).and_then(|strings| {
        strings
            .into_iter()
            .map(|s| {
                string_to_point(s).map_err(|err| {
                    serde::de::Error::custom(format!("failed to deserialize point: {}", err))
                })
            })
            .into_iter()
            .collect()
    })
}
