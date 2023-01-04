use std::{
    collections::{BTreeMap, HashMap},
    slice,
};

use apache_avro::{
    schema::{Name, Namespace},
    Schema,
};

#[derive(Debug, Clone, strum::EnumDiscriminants)]
#[strum_discriminants(
    name(SerializationSchemaKind),
    derive(Hash, Ord, PartialOrd, strum::Display)
)]
pub enum SerializationSchema {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(Box<SerializationSchema>),
    Map(Box<SerializationSchema>),
    Union {
        schemas: Vec<SerializationSchema>,
        variant_index: BTreeMap<SerializationSchemaKind, usize>,
    },
    Record {
        name: Name,
        fields: Vec<(String, SerializationSchema)>,
    },
    Enum {
        name: Name,
        symbols: BTreeMap<String, usize>,
    },
    Fixed {
        name: Name,
        size: usize,
    },
    Decimal {
        precision: usize,
        scale: usize,
        inner: Box<SerializationSchema>,
    },
    Uuid,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    Duration,
    Ref {
        name: Name,
        index: usize,
    },
}

type Refs<'a> = Vec<&'a SerializationSchema>;

self_cell::self_cell!(
    pub(crate) struct SerializationSchemaWithRefs {
        owner: SerializationSchema,
        #[covariant]
        dependent: Refs,
    }
    impl {Debug}
);

pub fn to_serialization_schema(
    schema: &Schema,
    ref_indexes: &mut HashMap<Name, usize>,
    enclosing_namespace: &Namespace,
) -> SerializationSchema {
    match schema {
        Schema::Null => SerializationSchema::Null,
        Schema::Boolean => SerializationSchema::Boolean,
        Schema::Int => SerializationSchema::Int,
        Schema::Long => SerializationSchema::Long,
        Schema::Float => SerializationSchema::Float,
        Schema::Double => SerializationSchema::Double,
        Schema::Bytes => SerializationSchema::Bytes,
        Schema::String => SerializationSchema::String,
        Schema::Array(schema) => SerializationSchema::Array(Box::new(to_serialization_schema(
            schema,
            ref_indexes,
            enclosing_namespace,
        ))),
        Schema::Map(schema) => SerializationSchema::Map(Box::new(to_serialization_schema(
            schema,
            ref_indexes,
            enclosing_namespace,
        ))),
        Schema::Union(schema) => {
            let schemas: Vec<_> = schema
                .variants()
                .iter()
                .map(|s| to_serialization_schema(s, ref_indexes, enclosing_namespace))
                .collect();
            let variant_index = schemas
                .iter()
                .enumerate()
                .map(|(i, s)| (SerializationSchemaKind::from(s), i))
                .collect();
            SerializationSchema::Union {
                schemas,
                variant_index,
            }
        }
        Schema::Record { name, fields, .. } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            let optimized_fields = fields
                .iter()
                .map(|field| {
                    (
                        field.name.clone(),
                        to_serialization_schema(
                            &field.schema,
                            ref_indexes,
                            &fully_qualified_name.namespace,
                        ),
                    )
                })
                .collect();
            SerializationSchema::Record {
                name: fully_qualified_name,
                fields: optimized_fields,
            }
        }
        Schema::Enum { name, symbols, .. } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            SerializationSchema::Enum {
                name: fully_qualified_name,
                symbols: symbols
                    .iter()
                    .enumerate()
                    .map(|(i, s)| (s.clone(), i))
                    .collect(),
            }
        }
        Schema::Fixed { name, size, .. } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            SerializationSchema::Fixed {
                name: fully_qualified_name,
                size: *size,
            }
        }
        Schema::Decimal {
            precision,
            scale,
            inner,
        } => SerializationSchema::Decimal {
            precision: *precision,
            scale: *scale,
            inner: Box::new(to_serialization_schema(
                inner,
                ref_indexes,
                enclosing_namespace,
            )),
        },
        Schema::Uuid => SerializationSchema::Uuid,
        Schema::Date => SerializationSchema::Date,
        Schema::TimeMillis => SerializationSchema::TimeMillis,
        Schema::TimeMicros => SerializationSchema::TimeMicros,
        Schema::TimestampMillis => SerializationSchema::TimestampMillis,
        Schema::TimestampMicros => SerializationSchema::TimestampMicros,
        Schema::Duration => SerializationSchema::Duration,
        Schema::Ref { name } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            let nb_refs = ref_indexes.len();
            let resolved_index = *ref_indexes
                .entry(fully_qualified_name.clone())
                .or_insert(nb_refs);
            SerializationSchema::Ref {
                name: fully_qualified_name,
                index: resolved_index,
            }
        }
    }
}

pub fn set_refs<'a>(
    schema: &'a SerializationSchema,
    ref_indexes: &HashMap<Name, usize>,
    refs: &mut [Option<&'a SerializationSchema>],
) {
    match schema {
        SerializationSchema::Array(schema) | SerializationSchema::Map(schema) => {
            set_refs(schema, ref_indexes, refs)
        }
        SerializationSchema::Union { schemas, .. } => {
            schemas.iter().for_each(|s| set_refs(s, ref_indexes, refs))
        }
        SerializationSchema::Record { name, .. }
        | SerializationSchema::Enum { name, .. }
        | SerializationSchema::Fixed { name, .. } => {
            if let Some(&index) = ref_indexes.get(name) {
                refs[index] = Some(schema)
            }
        }
        _ => {}
    }
}

impl AsRef<[SerializationSchemaKind]> for SerializationSchemaKind {
    fn as_ref(&self) -> &[SerializationSchemaKind] {
        slice::from_ref(self)
    }
}
