use serde::ser::{Impossible, Serialize};

use crate::error::SerializationError;

pub struct StringChecker;

fn not_a_string<Ok>() -> Result<Ok, SerializationError> {
    Err("map key is not a string".into())
}

impl serde::Serializer for StringChecker {
    type Ok = ();
    type Error = SerializationError;
    type SerializeSeq = Impossible<(), SerializationError>;
    type SerializeTuple = Impossible<(), SerializationError>;
    type SerializeTupleStruct = Impossible<(), SerializationError>;
    type SerializeTupleVariant = Impossible<(), SerializationError>;
    type SerializeMap = Impossible<(), SerializationError>;
    type SerializeStruct = Impossible<(), SerializationError>;
    type SerializeStructVariant = Impossible<(), SerializationError>;

    fn serialize_bool(self, _: bool) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_i8(self, _: i8) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_i16(self, _: i16) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_i32(self, _: i32) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_i64(self, _: i64) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_u8(self, _: u8) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_u16(self, _: u16) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_u32(self, _: u32) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_u64(self, _: u64) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_f32(self, _: f32) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_f64(self, _: f64) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_char(self, _: char) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_str(self, _: &str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_some<T: ?Sized>(self, _: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        not_a_string()
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        not_a_string()
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        not_a_string()
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        not_a_string()
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
        not_a_string()
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        not_a_string()
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        not_a_string()
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        not_a_string()
    }

    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        not_a_string()
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        not_a_string()
    }
}
