use std::{collections::VecDeque, fmt::Display, io};

use crate::{schema::SerializationSchema, SerializationSchemaKind};

#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("expected {expected:?}, found {found} (path: {path:?})")]
    SchemaMismatch {
        expected: SerializationSchema,
        found: SerializationSchemaKind,
        path: VecDeque<&'static str>,
    },
    #[error("{error} (path: {path:?})")]
    Custom {
        error: String,
        path: VecDeque<&'static str>,
    },
}

impl serde::ser::Error for SerializationError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        SerializationError::Custom {
            error: msg.to_string(),
            path: Default::default(),
        }
    }
}

impl From<&str> for SerializationError {
    fn from(s: &str) -> Self {
        use serde::ser::Error;
        Self::custom(s)
    }
}

impl From<String> for SerializationError {
    fn from(s: String) -> Self {
        use serde::ser::Error;
        Self::custom(s)
    }
}
