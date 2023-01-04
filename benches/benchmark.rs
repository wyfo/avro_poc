use apache_avro::{to_avro_datum, to_value, Schema};
use avro_poc::Serializer;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use serde::Serialize;

const SCHEMA_INT: &str = r#"{"type": "int"}"#;

const SCHEMA_ARRAY: &str = r#"{"type": "array", "items": "string"}"#;

const SCHEMA_RECORD: &str = r#"{
    "type": "record", 
    "name": "Foo", 
    "fields": [
        {"name": "bar", "type": "string"}, 
        {"name": "baz", "type": ["null", "int"]}
    ]
}"#;

const SCHEMA_RECURSIVE: &str = r#"{
    "type" : "record",
    "name" : "Tree",
    "fields" : [ 
        {"name": "value", "type": "int"}, 
        {"name" : "children", "type" : {"type" : "array", "items": "Tree"}}
    ]
}"#;

const SCHEMA_COMPLEX: &str = r#"{
    "type" : "record",
    "name" : "userInfo",
    "namespace" : "my.example",
    "fields" : [
        {"name" : "username","type" : "string"},
        {"name" : "age", "type" : ["null", "int"]},
        {"name" : "phone", "type" : ["null", "string"]},
        {"name" : "housenum", "type" : ["null", "string"]},
        {
            "name" : "address",
            "type" : [
                "null",
                {
                    "type": "record",
                    "name": "mailing_address",
                    "fields": [
                        {"name": "street", "type": ["null", "string"]},
                        {"name": "city", "type": ["null", "string"]},
                        {"name": "zip", "type": ["null", "string"]},
                        {"name": "country", "type": "string"}
                    ]
                }
            ]
        }
    ]
}"#;

fn bench(c: &mut Criterion, category: &'static str, schema: &'static str, value: impl Serialize) {
    let schema = Schema::parse_str(schema).unwrap();
    let serializer = Serializer::new(&schema).unwrap();
    assert_eq!(
        to_avro_datum(&schema, to_value(&value).unwrap()).unwrap(),
        serializer.serialize(&value).unwrap()
    );
    c.bench_with_input(
        BenchmarkId::new(category, "apache_avro"),
        &schema,
        |b, schema| {
            b.iter(|| to_avro_datum(&schema, to_value(black_box(&value)).unwrap()).unwrap());
        },
    );
    c.bench_with_input(
        BenchmarkId::new(category, "avro_poc"),
        &serializer,
        |b, serializer| {
            b.iter(|| serializer.serialize(black_box(&value)).unwrap());
        },
    );
}

fn int(c: &mut Criterion) {
    bench(c, "int", SCHEMA_INT, 329847);
}

fn string_array(c: &mut Criterion) {
    bench(c, "array", SCHEMA_ARRAY, vec!["foo".to_string(); 50]);
}

fn simple(c: &mut Criterion) {
    #[derive(serde::Serialize)]
    struct Foo {
        bar: String,
        baz: Option<i32>,
    }
    let value = Foo {
        bar: "bar".into(),
        baz: Some(42),
    };
    bench(c, "record", SCHEMA_RECORD, value);
}

fn recursive(c: &mut Criterion) {
    #[derive(serde::Serialize)]
    struct Tree {
        value: i32,
        children: Vec<Tree>,
    }
    let value = Tree {
        value: 0,
        children: vec![
            Tree {
                value: 1,
                children: vec![],
            },
            Tree {
                value: 2,
                children: vec![Tree {
                    value: 3,
                    children: vec![Tree {
                        value: 4,
                        children: vec![],
                    }],
                }],
            },
            Tree {
                value: 5,
                children: vec![],
            },
        ],
    };
    bench(c, "recursive", SCHEMA_RECURSIVE, value);
}

fn complex(c: &mut Criterion) {
    #[derive(serde::Serialize)]
    struct UserInfo {
        username: String,
        age: Option<i32>,
        phone: Option<String>,
        housenum: Option<String>,
        address: Option<Address>,
    }
    #[derive(serde::Serialize)]
    struct Address {
        street: Option<String>,
        city: Option<String>,
        zip: Option<String>,
        country: String,
    }
    let value = UserInfo {
        username: "wyfo".into(),
        age: Some(29),
        phone: None,
        housenum: None,
        address: Some(Address {
            street: None,
            city: Some("Chaville".into()),
            zip: None,
            country: "France".into(),
        }),
    };
    bench(c, "complex", SCHEMA_COMPLEX, value);
}

criterion_group!(benches, int, string_array, simple, recursive, complex);
criterion_main!(benches);
