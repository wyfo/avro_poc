use std::{collections::HashMap, io};

use apache_avro::{AvroResult, Error, Schema};
use error::SerializationError;
use ser::SerializerRef;
use serde::Serialize;

use crate::schema::{SerializationSchemaKind, SerializationSchemaWithRefs};

mod error;
mod schema;
mod ser;
mod utils;

pub struct Serializer(SerializationSchemaWithRefs);

impl Serializer {
    pub fn new(schema: &Schema) -> AvroResult<Self> {
        let mut ref_indexes = HashMap::new();
        let optimized_schema = schema::to_serialization_schema(schema, &mut ref_indexes, &None);
        let internal = SerializationSchemaWithRefs::try_new(optimized_schema, move |s| {
            let mut refs = vec![None; ref_indexes.len()];
            schema::set_refs(s, &ref_indexes, &mut refs);
            refs.into_iter()
                .enumerate()
                .map(|(index, r#ref)| {
                    r#ref.ok_or_else(|| {
                        Error::SchemaResolutionError(
                            ref_indexes
                                .iter()
                                .find_map(|(name, i)| (index == *i).then_some(name))
                                .unwrap()
                                .clone(),
                        )
                    })
                })
                .collect()
        })?;
        Ok(Self(internal))
    }

    pub fn write(
        &self,
        value: &impl Serialize,
        writer: impl io::Write,
    ) -> Result<(), SerializationError> {
        SerializerRef {
            writer,
            schema: self.0.borrow_owner(),
            refs: self.0.borrow_dependent(),
        }
        .serialize(value)
    }

    pub fn serialize(&self, value: &impl Serialize) -> Result<Vec<u8>, SerializationError> {
        let mut vec = Vec::new();
        self.write(value, &mut vec)?;
        Ok(vec)
    }
}

#[cfg(test)]
mod test {
    use apache_avro::{to_avro_datum, to_value, Schema};

    use crate::Serializer;

    #[derive(serde::Serialize)]
    struct Plop {
        foo: String,
        bar: Option<i32>,
        baz: bool,
    }

    const SCHEMA: &str = r#"{"type": "record", "name": "Plop", "fields": [{"name": "foo", "type": "string"}, {"name": "bar", "type": ["null", "int"]}]}"#;

    #[test]
    fn test_plop() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let plop = Plop {
            foo: "foo".into(),
            bar: Some(42),
            baz: true,
        };
        println!("{:?}", to_avro_datum(&schema, to_value(&plop).unwrap()));
        let serializer = Serializer::new(&schema).unwrap();
        println!("{:?}", serializer.serialize(&plop));
    }
}
