use std::io;

use integer_encoding::{VarInt, VarIntWriter};
use serde::Serialize;

use crate::{
    error::SerializationError,
    schema::{SerializationSchema, SerializationSchemaKind},
    utils::StringChecker,
};

pub(crate) struct SerializerRef<'a, W> {
    pub(crate) writer: W,
    pub(crate) schema: &'a SerializationSchema,
    pub(crate) refs: &'a [&'a SerializationSchema],
}

impl<'a, W> SerializerRef<'a, W> {
    fn with_schema(&mut self, schema: &'a SerializationSchema) -> &mut Self {
        self.schema = schema;
        self
    }
}

impl<'a, W> SerializerRef<'a, W>
where
    W: io::Write,
{
    pub(crate) fn serialize(&mut self, value: impl Serialize) -> Result<(), SerializationError> {
        match self.schema {
            SerializationSchema::Ref { index, .. } => {
                let schema = self.refs[*index];
                self.with_schema(schema).serialize(value)
            }
            SerializationSchema::Union {
                schemas,
                variant_index,
            } if schemas.len() != variant_index.len() => {
                todo!()
            }
            _ => value.serialize(self),
        }
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), SerializationError> {
        self.writer.write_all(bytes)?;
        Ok(())
    }

    fn write_varint(&mut self, n: impl VarInt) -> Result<(), SerializationError> {
        self.writer.write_varint(n)?;
        Ok(())
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), SerializationError> {
        self.write_varint(bytes.len() as i64)?;
        self.write(bytes)
    }

    fn write_tag(
        &mut self,
        fields: &'a [(String, SerializationSchema)],
        variant: &'static str,
    ) -> Result<&'a SerializationSchema, SerializationError> {
        if fields.len() != 2 || fields[0].0 != "type" || fields[1].0 != "value" {
            return Err("tag record must have two fields: \"type\" and \"value\"".into());
        }
        self.with_schema(&fields[0].1).serialize(variant)?;
        Ok(&fields[1].1)
    }

    fn collection<'b>(
        &'b mut self,
        len: impl Into<Option<usize>>,
    ) -> Result<CollectionSerializer<'a, 'b, W>, SerializationError> {
        let Some(len) = len.into() else {
            return Err("array/map without len".into());
        };
        self.write_varint(len as i64)?;
        Ok(CollectionSerializer {
            serializer: self,
            empty: len == 0,
        })
    }

    fn record<'b>(
        &'b mut self,
        name: &'static str,
        fields: &'a [(String, SerializationSchema)],
    ) -> RecordSerializer<'a, 'b, W> {
        RecordSerializer {
            serializer: self,
            type_name: name,
            fields,
        }
    }
}

pub(crate) struct CollectionSerializer<'a, 'b, W> {
    serializer: &'b mut SerializerRef<'a, W>,
    empty: bool,
}

impl<'a, 'b, W> serde::ser::SerializeSeq for CollectionSerializer<'a, 'b, W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = SerializationError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.serializer.serialize(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if !self.empty {
            self.serializer.write(&[0])?;
        }
        Ok(())
    }
}

impl<'a, 'b, W> serde::ser::SerializeTuple for CollectionSerializer<'a, 'b, W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = SerializationError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        serde::ser::SerializeSeq::end(self)
    }
}

impl<'a, 'b, W> serde::ser::SerializeMap for CollectionSerializer<'a, 'b, W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = SerializationError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        key.serialize(StringChecker)?;
        self.serializer.serialize(key)
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        serde::ser::SerializeSeq::end(self)
    }
}

pub(crate) struct RecordSerializer<'a, 'b, W> {
    serializer: &'b mut SerializerRef<'a, W>,
    type_name: &'static str,
    fields: &'a [(String, SerializationSchema)],
}

impl<'a, 'b, W> serde::ser::SerializeStruct for RecordSerializer<'a, 'b, W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if self.fields.is_empty() {
            return Err("unexpected field".into()).with_path(self.type_name, key);
        }
        if key != self.fields[0].0 {
            return Err(format!("expected field {}", self.fields[0].0).into())
                .with_path(self.type_name, key);
        }
        self.serializer
            .with_schema(&self.fields[0].1)
            .serialize(value)
            .with_path(self.type_name, key)?;
        self.fields = &self.fields[1..];
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if !self.fields.is_empty() {
            return Err(format!("missing field {}", self.fields[0].0).into())
                .with_path(self.type_name, "");
        }
        Ok(())
    }
}

trait WithPath {
    type Output;
    fn with_path(self, type_name: &'static str, field: &'static str) -> Self::Output;
}

impl<T> WithPath for Result<T, SerializationError> {
    type Output = Result<T, SerializationError>;
    fn with_path(mut self, type_name: &'static str, field: &'static str) -> Self::Output {
        if let Err(
            SerializationError::SchemaMismatch { ref mut path, .. }
            | SerializationError::Custom { ref mut path, .. },
        ) = self
        {
            if !field.is_empty() {
                path.push_front(field)
            }
            path.push_front(type_name);
        }
        self
    }
}

pub(crate) struct WithPathSerializer<S> {
    serializer: S,
    type_name: &'static str,
    field: &'static str,
}

impl<'a, 'b, W> WithPath for &'b mut SerializerRef<'a, W> {
    type Output = WithPathSerializer<Self>;
    fn with_path(self, type_name: &'static str, field: &'static str) -> Self::Output {
        WithPathSerializer {
            serializer: self,
            type_name,
            field,
        }
    }
}

impl<'a, 'b, W> WithPath for CollectionSerializer<'a, 'b, W> {
    type Output = WithPathSerializer<Self>;
    fn with_path(self, type_name: &'static str, field: &'static str) -> Self::Output {
        WithPathSerializer {
            serializer: self,
            type_name,
            field,
        }
    }
}

impl<'a, 'b, W> WithPath for RecordSerializer<'a, 'b, W> {
    type Output = WithPathSerializer<Self>;
    fn with_path(self, type_name: &'static str, field: &'static str) -> Self::Output {
        WithPathSerializer {
            serializer: self,
            type_name,
            field,
        }
    }
}

impl<'a, 'b, W> serde::ser::SerializeTupleStruct
    for WithPathSerializer<CollectionSerializer<'a, 'b, W>>
where
    W: io::Write,
{
    type Ok = ();
    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        serde::ser::SerializeSeq::serialize_element(&mut self.serializer, value)
            .with_path(self.type_name, self.field)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        serde::ser::SerializeSeq::end(self.serializer).with_path(self.type_name, self.field)
    }
}

impl<'a, 'b, W> serde::ser::SerializeTupleVariant
    for WithPathSerializer<CollectionSerializer<'a, 'b, W>>
where
    W: io::Write,
{
    type Ok = ();
    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        serde::ser::SerializeSeq::serialize_element(&mut self.serializer, value)
            .with_path(self.type_name, self.field)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        serde::ser::SerializeSeq::end(self.serializer).with_path(self.type_name, self.field)
    }
}

impl<'a, 'b, W> serde::ser::SerializeStructVariant
    for WithPathSerializer<RecordSerializer<'a, 'b, W>>
where
    W: io::Write,
{
    type Ok = ();
    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        serde::ser::SerializeStruct::serialize_field(&mut self.serializer, key, value)
            .with_path(self.type_name, self.field)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        serde::ser::SerializeStruct::end(self.serializer).with_path(self.type_name, self.field)
    }
}

macro_rules! match_schema {
    ($self:expr, $($expected:ident),*; $stmt:expr $(;$kind2:ident, $expected2:pat => $stmt2:expr)*) => {{
        match $self.schema {
            $(SerializationSchema::$expected => {#[allow(unreachable_code)] return $stmt;})*
            $($expected2 => {#[allow(unreachable_code)] return $stmt2;})*
            #[allow(unused_variables)]
            SerializationSchema::Union {schemas, variant_index} => {
                $(if let Some(index) = variant_index.get(&SerializationSchemaKind::$expected) {
                    $self.writer.write_varint(*index as i64)?;
                    #[allow(unreachable_code)]
                    return $stmt;
                })*
                $(if let Some(index) = variant_index.get(&SerializationSchemaKind::$kind2) {
                    match &schemas[*index] {
                        $expected2 => {
                            $self.writer.write_varint(*index as i64)?;
                            #[allow(unreachable_code)]
                            return $stmt2;
                        }
                        _ => {}
                    }
                })*
            }
            _ => {}
        }
        return Err(SerializationError::SchemaMismatch {
                expected: $self.schema.clone(),
                found: [$(SerializationSchemaKind::$expected,)*$(SerializationSchemaKind::$kind2,)*][0],
                path: Default::default(),
        });
    }};
}

impl<'a, 'b, W> serde::Serializer for &'b mut SerializerRef<'a, W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = SerializationError;
    type SerializeSeq = CollectionSerializer<'a, 'b, W>;
    type SerializeTuple = CollectionSerializer<'a, 'b, W>;
    type SerializeTupleStruct = WithPathSerializer<CollectionSerializer<'a, 'b, W>>;
    type SerializeTupleVariant = WithPathSerializer<CollectionSerializer<'a, 'b, W>>;
    type SerializeMap = CollectionSerializer<'a, 'b, W>;
    type SerializeStruct = RecordSerializer<'a, 'b, W>;
    type SerializeStructVariant = WithPathSerializer<RecordSerializer<'a, 'b, W>>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Boolean; self.write(&[u8::from(v)]))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Int, Long; self.write_varint(v));
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Int, Long; self.write_varint(v));
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Int, Long; self.write_varint(v));
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Long; self.write_varint(v));
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Int, Long; self.write_varint(v));
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Int, Long; self.write_varint(v));
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Int, Long; self.write_varint(v));
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Long; self.write_varint(v));
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Float; self.write(&v.to_le_bytes()));
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Double; self.write(&v.to_le_bytes()));
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, String; self.write_bytes(&[v as u8]));
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        match_schema!(
            self, String;
            self.write_bytes(v.as_bytes());
            Enum, SerializationSchema::Enum {symbols, ..} => {
                let index = *symbols.get(v).ok_or_else(|| format!("unexpected {v} in enum"))?;
                self.write_varint(index as i64)
            }
        );
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        match_schema!(
            self, Bytes, Uuid;
            self.write_bytes(v);
            Fixed, SerializationSchema::Fixed {size, ..} => {
                if *size != v.len() {
                    return Err(format!("expected fixed {size}, found {}", v.len()).into())
                }
                self.write(v)
            }
        );
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Null; Ok(()));
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Null; Ok(()));
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        match_schema!(self, Null; Ok(()));
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        variant.serialize(self).with_path(name, variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self).with_path(name, "")
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        match_schema!(self, ; (); Record, SerializationSchema::Record {fields, ..} => {
            let schema = self.write_tag(fields, variant).with_path(name, variant)?;
            self.with_schema(schema).serialize(value).with_path(name, variant)
        });
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        match_schema!(self, ; (); Array, SerializationSchema::Array(schema) => {
            self.with_schema(schema).collection(len)
        });
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        match_schema!(self, ; (); Array, SerializationSchema::Array(schema) => {
            self.with_schema(schema).collection(len)
        });
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        match_schema!(self, ; (); Array, SerializationSchema::Array(schema) => {
            Ok(self.with_schema(schema).collection(len)?.with_path(name, ""))
        });
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        match_schema!(self, ; (); Record, SerializationSchema::Record {fields, ..} => {
            let schema = self.write_tag(fields, variant).with_path(name, variant)?;
            let serializer = self.with_schema(schema);
            match_schema!(serializer, ; (); Array, SerializationSchema::Array(schema) => {
                Ok(serializer.with_schema(schema).collection(len)?.with_path(name, variant))
            });
        });
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        match_schema!(self, ; (); Map, SerializationSchema::Map(schema) => {
            self.with_schema(schema).collection(len)
        });
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        match_schema!(self, ; (); Record, SerializationSchema::Record {fields, ..} => {
            Ok(self.record(name, fields))
        });
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        match_schema!(self, ; (); Record, SerializationSchema::Record {fields, ..} => {
            let schema = self.write_tag(fields, variant).with_path(name, variant)?;
            let serializer = self.with_schema(schema);
            match_schema!(serializer, ; (); Record, SerializationSchema::Record {fields, ..} => {
                Ok(serializer.record(name, fields).with_path(name, variant))
            })
        });
    }
}
