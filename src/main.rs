use std::fmt::Write;

use anyhow::{anyhow, Result};
use semver::Version;
use wit_encoder::{
    Field, Ident, Interface, InterfaceItem, Package, PackageName, Record, Tuple, Type, TypeDef, TypeDefKind,
};

use crate::wasmcloud::postgres::types::{
    Date, Interval, MacAddressEui48, MacAddressEui64, Offset, PgValue, Time, TimeTz, Timestamp, TimestampTz,
};

wit_bindgen::generate!({
    generate_unused_types: true,
    with: { "wasmcloud:postgres/types@0.1.0-draft": generate, }
});

struct SqlColumn {
    name: String,
    data_type: String,
}

struct SqlTable {
    name: String,
    columns: Vec<SqlColumn>,
}

fn main() -> Result<()> {
    // Create a new package
    let package_name = PackageName::new("example-namespace", Ident::new("example"), Some(Version::new(0, 1, 0)));
    let mut package = Package::new(package_name);

    // Create the SQL table definition
    let sql_table = SqlTable {
        name: "users".to_string(),
        columns: vec![
            SqlColumn {
                name: "id".to_string(),
                data_type: "integer".to_string(),
            },
            SqlColumn {
                name: "name".to_string(),
                data_type: "text".to_string(),
            },
            SqlColumn {
                name: "age".to_string(),
                data_type: "integer".to_string(),
            },
            SqlColumn {
                name: "is_active".to_string(),
                data_type: "boolean".to_string(),
            },
            SqlColumn {
                name: "balance".to_string(),
                data_type: "numeric".to_string(),
            },
            SqlColumn {
                name: "created_at".to_string(),
                data_type: "timestamp".to_string(),
            },
        ],
    };

    // Create the "sql-types" interface
    let mut sql_types_interface = Interface::new(Ident::new("sql-types"));

    // Create a WIT record for the SQL table
    let record = create_wit_record(sql_table.columns)?;

    // TypeDef for the record
    let type_def = TypeDef::new(Ident::new(sql_table.name.clone()), TypeDefKind::Record(record));

    sql_types_interface.item(InterfaceItem::TypeDef(type_def));

    // Add interface to package
    package.interface(sql_types_interface);

    let mut output = String::new();
    write!(output, "{}", package)?;

    println!("Rendered WIT:\n{}", output);

    Ok(())
}

fn sql_type_to_pg_value(sql_type: &str) -> PgValue {
    match sql_type.to_lowercase().as_str() {
        // Numeric types
        "bigint" | "int8" => PgValue::BigInt(0),
        "bigserial" | "serial8" => PgValue::BigSerial(0),
        "boolean" | "bool" => PgValue::Bool(false),
        "double precision" | "float8" => PgValue::Double((0, 0, 0)),
        "real" | "float4" => PgValue::Real((0, 0, 0)),
        "integer" | "int" | "int4" => PgValue::Integer(0),
        "numeric" | "decimal" => PgValue::Numeric("0".to_string()),
        "serial" | "serial4" => PgValue::Serial(0),
        "smallint" | "int2" => PgValue::SmallInt(0),
        "smallserial" | "serial2" => PgValue::SmallSerial(0),

        // Array types
        "bigint[]" | "int8[]" => PgValue::Int8Array(vec![]),
        "boolean[]" => PgValue::BoolArray(vec![]),
        "double precision[]" | "float8[]" => PgValue::Float8Array(vec![]),
        "real[]" | "float4[]" => PgValue::Float4Array(vec![]),
        "integer[]" | "int[]" | "int4[]" => PgValue::Int4Array(vec![]),
        "numeric[]" | "decimal[]" => PgValue::NumericArray(vec![]),
        "smallint[]" | "int2[]" => PgValue::Int2Array(vec![]),

        // Byte-related types
        "bit" => PgValue::Bit((0, vec![])),
        "bit[]" => PgValue::BitArray(vec![]),
        "bit varying" | "varbit" => PgValue::BitVarying((None, vec![])),
        "bit varying[]" | "varbit[]" => PgValue::VarbitArray(vec![]),
        "bytea" => PgValue::Bytea(vec![]),
        "bytea[]" => PgValue::ByteaArray(vec![]),

        // Special types
        "int2vector" => PgValue::Int2Vector(vec![]),
        "int2vector[]" => PgValue::Int2VectorArray(vec![]),

        // Geometric types
        "box" => PgValue::Box(Default::default()),
        "box[]" => PgValue::BoxArray(vec![]),
        "circle" => PgValue::Circle(Default::default()),
        "circle[]" => PgValue::CircleArray(vec![]),
        "line" => PgValue::Line(Default::default()),
        "line[]" => PgValue::LineArray(vec![]),
        "lseg" => PgValue::Lseg(Default::default()),
        "lseg[]" => PgValue::LsegArray(vec![]),
        "path" => PgValue::Path(vec![]),
        "path[]" => PgValue::PathArray(vec![]),
        "point" => PgValue::Point(Default::default()),
        "point[]" => PgValue::PointArray(vec![]),
        "polygon" => PgValue::Polygon(vec![]),
        "polygon[]" => PgValue::PolygonArray(vec![]),

        // JSON types
        "json" => PgValue::Json("{}".to_string()),
        "json[]" => PgValue::JsonArray(vec![]),
        "jsonb" => PgValue::Jsonb("{}".to_string()),
        "jsonb[]" => PgValue::JsonbArray(vec![]),

        // Money type
        "money" => PgValue::Money("0".to_string()),
        "money[]" => PgValue::MoneyArray(vec![]),

        // Postgres-internal types
        "pg_lsn" => PgValue::PgLsn(0),
        "pg_lsn[]" => PgValue::PgLsnArray(vec![]),
        "pg_snapshot" => PgValue::PgSnapshot((0, 0, vec![])),
        "txid_snapshot" => PgValue::TxidSnapshot(0),

        // Text types
        "char" => PgValue::Char(Default::default()),
        "char[]" => PgValue::CharArray(vec![]),
        "varchar" => PgValue::Varchar(Default::default()),
        "varchar[]" => PgValue::VarcharArray(vec![]),
        "name" => PgValue::Name("".to_string()),
        "name[]" => PgValue::NameArray(vec![]),
        "text" => PgValue::Text("".to_string()),
        "text[]" => PgValue::TextArray(vec![]),
        "xml" => PgValue::Xml("".to_string()),
        "xml[]" => PgValue::XmlArray(vec![]),

        // Full Text Search types
        "tsquery" => PgValue::TsQuery("".to_string()),
        "tsvector" => PgValue::TsVector(vec![]),

        // UUID types
        "uuid" => PgValue::Uuid("00000000-0000-0000-0000-000000000000".to_string()),
        "uuid[]" => PgValue::UuidArray(vec![]),

        // Container types
        "hstore" => PgValue::Hstore(vec![]),

        // Date and Time types
        "date" => PgValue::Date(Date::Ymd((1970, 1, 1))),
        "date[]" => PgValue::DateArray(vec![]),
        "time" => PgValue::Time(Time {
            hour: 0,
            min: 0,
            sec: 0,
            micro: 0,
        }),
        "time[]" => PgValue::TimeArray(vec![]),
        "time with time zone" | "timetz" => PgValue::TimeTz(TimeTz {
            timesonze: "UTC".to_string(),
            time: Time {
                hour: 0,
                min: 0,
                sec: 0,
                micro: 0,
            },
        }),
        "time with time zone[]" | "timetz[]" => PgValue::TimeTzArray(vec![]),
        "timestamp" => PgValue::Timestamp(Timestamp {
            date: Date::Ymd((1970, 1, 1)),
            time: Time {
                hour: 0,
                min: 0,
                sec: 0,
                micro: 0,
            },
        }),
        "timestamp[]" => PgValue::TimestampArray(vec![]),
        "timestamp with time zone" | "timestamptz" => PgValue::TimestampTz(TimestampTz {
            timestamp: Timestamp {
                date: Date::Ymd((1970, 1, 1)),
                time: Time {
                    hour: 0,
                    min: 0,
                    sec: 0,
                    micro: 0,
                },
            },
            offset: Offset::EasternHemisphereSecs(0),
        }),
        "timestamp with time zone[]" | "timestamptz[]" => PgValue::TimestampTzArray(vec![]),
        "interval" => PgValue::Interval(Interval {
            start: Date::Ymd((1970, 1, 1)),
            start_inclusive: true,
            end: Date::Ymd((1970, 1, 1)),
            end_inclusive: true,
        }),
        "interval[]" => PgValue::IntervalArray(vec![]),

        // Network Address types
        "inet" => PgValue::Inet("0.0.0.0".to_string()),
        "inet[]" => PgValue::InetArray(vec![]),
        "cidr" => PgValue::Cidr("0.0.0.0/0".to_string()),
        "cidr[]" => PgValue::CidrArray(vec![]),
        "macaddr" => PgValue::Macaddr(MacAddressEui48 {
            bytes: (0, 0, 0, 0, 0, 0),
        }),
        "macaddr[]" => PgValue::MacaddrArray(vec![]),
        "macaddr8" => PgValue::Macaddr8(MacAddressEui64 {
            bytes: (0, 0, 0, 0, 0, 0, 0, 0),
        }),
        "macaddr8[]" => PgValue::Macaddr8Array(vec![]),

        // Default case
        _ => PgValue::Null,
    }
}

// Helper function to create a WIT record from SQL table columns
fn create_wit_record(columns: Vec<SqlColumn>) -> Result<Record> {
    let fields = columns
        .into_iter()
        .map(|column| {
            let pg_value = sql_type_to_pg_value(&column.data_type);
            let wit_type = pg_value_to_wit_type(&pg_value)?;
            Ok(Field::new(Ident::new(column.name), wit_type))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Record::new(fields))
}

fn pg_value_to_wit_type(pg_value: &PgValue) -> Result<Type> {
    match pg_value {
        PgValue::Null => Err(anyhow!("Unable to map null to WIT type")),
        PgValue::BigInt(_) | PgValue::Int8(_) | PgValue::BigSerial(_) | PgValue::Serial8(_) => Ok(Type::S64),
        PgValue::Bool(_) | PgValue::Boolean(_) => Ok(Type::Bool),
        PgValue::Double(_) | PgValue::Float8(_) => Ok(Type::Named(Ident::new("hashable-f64"))),
        PgValue::Real(_) | PgValue::Float4(_) => Ok(Type::Named(Ident::new("hashable-f32"))),
        PgValue::Integer(_) | PgValue::Int(_) | PgValue::Int4(_) => Ok(Type::S32),
        PgValue::Numeric(_) | PgValue::Decimal(_) => Ok(Type::Named(Ident::new("numeric"))),
        PgValue::Serial(_) | PgValue::Serial4(_) => Ok(Type::U32),
        PgValue::SmallInt(_) | PgValue::Int2(_) | PgValue::SmallSerial(_) | PgValue::Serial2(_) => Ok(Type::S16),
        PgValue::Bit(_) => {
            let mut tuple = Tuple::empty();
            tuple.type_(Type::U32);
            tuple.type_(Type::List(Box::new(Type::U8)));
            Ok(Type::Tuple(tuple))
        }
        PgValue::BitVarying(_) | PgValue::Varbit(_) => {
            let mut tuple = Tuple::empty();
            tuple.type_(Type::Option(Box::new(Type::U32)));
            tuple.type_(Type::List(Box::new(Type::U8)));
            Ok(Type::Tuple(tuple))
        }
        PgValue::Bytea(_) => Ok(Type::List(Box::new(Type::U8))),
        PgValue::Int8Array(_) => Ok(Type::List(Box::new(Type::S64))),
        PgValue::BoolArray(_) => Ok(Type::List(Box::new(Type::Bool))),
        PgValue::Float8Array(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("hashable-f64"))))),
        PgValue::Float4Array(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("hashable-f32"))))),
        PgValue::Int4Array(_) => Ok(Type::List(Box::new(Type::S32))),
        PgValue::NumericArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("numeric"))))),
        PgValue::Int2Array(_) | PgValue::Int2Vector(_) => Ok(Type::List(Box::new(Type::S16))),
        PgValue::Int2VectorArray(_) => Ok(Type::List(Box::new(Type::List(Box::new(Type::S16))))),
        PgValue::BitArray(_) => {
            let mut inner_tuple = Tuple::empty();
            inner_tuple.type_(Type::U32);
            inner_tuple.type_(Type::List(Box::new(Type::U8)));
            Ok(Type::List(Box::new(Type::Tuple(inner_tuple))))
        }
        PgValue::VarbitArray(_) => {
            let mut inner_tuple = Tuple::empty();
            inner_tuple.type_(Type::Option(Box::new(Type::U32)));
            inner_tuple.type_(Type::List(Box::new(Type::U8)));
            Ok(Type::List(Box::new(Type::Tuple(inner_tuple))))
        }
        PgValue::ByteaArray(_) => Ok(Type::List(Box::new(Type::List(Box::new(Type::U8))))),
        PgValue::Char(_) | PgValue::Varchar(_) | PgValue::Text(_) | PgValue::Name(_) => Ok(Type::String),
        PgValue::CharArray(_) | PgValue::VarcharArray(_) | PgValue::TextArray(_) | PgValue::NameArray(_) => {
            Ok(Type::List(Box::new(Type::String)))
        }
        PgValue::Date(_) => Ok(Type::Named(Ident::new("date"))),
        PgValue::DateArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("date"))))),
        PgValue::Time(_) => Ok(Type::Named(Ident::new("time"))),
        PgValue::TimeArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("time"))))),
        PgValue::TimeTz(_) => Ok(Type::Named(Ident::new("time-tz"))),
        PgValue::TimeTzArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("time-tz"))))),
        PgValue::Timestamp(_) => Ok(Type::Named(Ident::new("timestamp"))),
        PgValue::TimestampArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("timestamp"))))),
        PgValue::TimestampTz(_) => Ok(Type::Named(Ident::new("timestamp-tz"))),
        PgValue::TimestampTzArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("timestamp-tz"))))),
        PgValue::Interval(_) => Ok(Type::Named(Ident::new("interval"))),
        PgValue::IntervalArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("interval"))))),
        PgValue::Uuid(_) => Ok(Type::String),
        PgValue::UuidArray(_) => Ok(Type::List(Box::new(Type::String))),
        PgValue::Xml(_) => Ok(Type::String),
        PgValue::XmlArray(_) => Ok(Type::List(Box::new(Type::String))),
        PgValue::Point(_) => Ok(Type::Named(Ident::new("point"))),
        PgValue::PointArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("point"))))),
        PgValue::Box(_) => {
            let mut tuple = Tuple::empty();
            tuple.type_(Type::Named(Ident::new("point")));
            tuple.type_(Type::Named(Ident::new("point")));
            Ok(Type::Tuple(tuple))
        }
        PgValue::BoxArray(_) => {
            let mut inner_tuple = Tuple::empty();
            inner_tuple.type_(Type::Named(Ident::new("point")));
            inner_tuple.type_(Type::Named(Ident::new("point")));
            Ok(Type::List(Box::new(Type::Tuple(inner_tuple))))
        }
        PgValue::Line(_) | PgValue::Lseg(_) => {
            let mut tuple = Tuple::empty();
            tuple.type_(Type::Named(Ident::new("point")));
            tuple.type_(Type::Named(Ident::new("point")));
            Ok(Type::Tuple(tuple))
        }
        PgValue::LineArray(_) | PgValue::LsegArray(_) => {
            let mut inner_tuple = Tuple::empty();
            inner_tuple.type_(Type::Named(Ident::new("point")));
            inner_tuple.type_(Type::Named(Ident::new("point")));
            Ok(Type::List(Box::new(Type::Tuple(inner_tuple))))
        }
        PgValue::Path(_) | PgValue::Polygon(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("point"))))),
        PgValue::PathArray(_) | PgValue::PolygonArray(_) => Ok(Type::List(Box::new(Type::List(Box::new(
            Type::Named(Ident::new("point")),
        ))))),
        PgValue::Circle(_) => {
            let mut tuple = Tuple::empty();
            tuple.type_(Type::Named(Ident::new("point")));
            tuple.type_(Type::Named(Ident::new("radius")));
            Ok(Type::Tuple(tuple))
        }
        PgValue::CircleArray(_) => {
            let mut inner_tuple = Tuple::empty();
            inner_tuple.type_(Type::Named(Ident::new("point")));
            inner_tuple.type_(Type::Named(Ident::new("radius")));
            Ok(Type::List(Box::new(Type::Tuple(inner_tuple))))
        }
        PgValue::Money(_) => Ok(Type::Named(Ident::new("numeric"))),
        PgValue::MoneyArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("numeric"))))),
        PgValue::Macaddr(_) => Ok(Type::Named(Ident::new("mac-address-eui48"))),
        PgValue::MacaddrArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("mac-address-eui48"))))),
        PgValue::Macaddr8(_) => Ok(Type::Named(Ident::new("mac-address-eui64"))),
        PgValue::Macaddr8Array(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("mac-address-eui64"))))),
        PgValue::Inet(_) | PgValue::Cidr(_) => Ok(Type::String),
        PgValue::InetArray(_) | PgValue::CidrArray(_) => Ok(Type::List(Box::new(Type::String))),
        PgValue::Json(_) | PgValue::Jsonb(_) => Ok(Type::String),
        PgValue::JsonArray(_) | PgValue::JsonbArray(_) => Ok(Type::List(Box::new(Type::String))),
        PgValue::PgLsn(_) => Ok(Type::U64),
        PgValue::PgLsnArray(_) => Ok(Type::List(Box::new(Type::U64))),
        PgValue::TsQuery(_) => Ok(Type::String),
        PgValue::TsVector(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("lexeme"))))),
        PgValue::TxidSnapshot(_) => Ok(Type::S64),
        PgValue::PgSnapshot(_) => {
            let mut tuple = Tuple::empty();
            tuple.type_(Type::S64);
            tuple.type_(Type::S64);
            tuple.type_(Type::List(Box::new(Type::S64)));
            Ok(Type::Tuple(tuple))
        }
        PgValue::Hstore(_) => {
            let mut inner_tuple = Tuple::empty();
            inner_tuple.type_(Type::String);
            inner_tuple.type_(Type::Option(Box::new(Type::String)));
            Ok(Type::List(Box::new(Type::Tuple(inner_tuple))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_to_wit_record() -> Result<()> {
        let table_name = "users";
        let columns = vec![
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
            ("age".to_string(), "integer".to_string()),
            ("is_active".to_string(), "boolean".to_string()),
            ("balance".to_string(), "numeric".to_string()),
            ("created_at".to_string(), "timestamp".to_string()),
        ];

        let mut record = create_wit_record(table_name, columns)?;
        let fields = record.fields_mut();

        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0].name(), &Ident::new("id"));
        assert_eq!(fields[0].ty(), &Type::S32);
        assert_eq!(fields[1].name(), &Ident::new("name"));
        assert_eq!(fields[1].ty(), &Type::String);
        assert_eq!(fields[2].name(), &Ident::new("age"));
        assert_eq!(fields[2].ty(), &Type::S32);
        assert_eq!(fields[3].name(), &Ident::new("is_active"));
        assert_eq!(fields[3].ty(), &Type::Bool);
        assert_eq!(fields[4].name(), &Ident::new("balance"));
        assert_eq!(fields[4].ty(), &Type::Named(Ident::new("numeric"))); // Updated expectation
        assert_eq!(fields[5].name(), &Ident::new("created_at"));
        assert_eq!(fields[5].ty(), &Type::Named(Ident::new("timestamp"))); // Updated expectation

        Ok(())
    }
}
