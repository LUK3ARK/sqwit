use crate::wasmcloud::postgres::types::Numeric;
use anyhow::{anyhow, Result};
use serde_json::json;
use sqlparser::ast::CharacterLength;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::DataType;
use sqlparser::ast::ExactNumberInfo;
use sqlparser::ast::Statement;
use sqlparser::ast::StructField;
use sqlparser::ast::TimezoneInfo;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use wit_encoder::{Field, Ident, Record, Tuple, Type};

use crate::wasmcloud::postgres::types::{
    Date, Interval, MacAddressEui48, MacAddressEui64, Offset, PgValue, Time, TimeTz, Timestamp,
    TimestampTz,
};

wit_bindgen::generate!({
    generate_unused_types: true,
    with: { "wasmcloud:postgres/types@0.1.0-draft": generate, }
});

// TODO
fn main() -> Result<()> {
    // // Create a new package
    // let package_name = PackageName::new(
    //     "example-namespace",
    //     Ident::new("example"),
    //     Some(Version::new(0, 1, 0)),
    // );
    // let mut package = Package::new(package_name);

    // // Create the SQL table definition
    // let sql_table = SqlTable {
    //     name: Cow::Borrowed("users"),
    //     columns: Cow::Owned(vec![
    //         SqlColumn {
    //             name: "id",
    //             data_type: "integer",
    //         },
    //         SqlColumn {
    //             name: "name",
    //             data_type: "text",
    //         },
    //         SqlColumn {
    //             name: "age",
    //             data_type: "integer",
    //         },
    //         SqlColumn {
    //             name: "is_active",
    //             data_type: "boolean",
    //         },
    //         SqlColumn {
    //             name: "balance",
    //             data_type: "numeric",
    //         },
    //         SqlColumn {
    //             name: "created_at",
    //             data_type: "timestamp",
    //         },
    //     ]),
    // };

    // // Create the "sql-types" interface
    // let mut sql_types_interface = Interface::new(Ident::new("sql-types"));

    // // Create a WIT record for the SQL table
    // let record = Record::try_from(sql_table.clone())?;

    // // TypeDef for the record
    // let type_def = TypeDef::new(
    //     Ident::new(sql_table.name.to_string()),
    //     TypeDefKind::Record(record),
    // );

    // sql_types_interface.item(InterfaceItem::TypeDef(type_def));

    // // Add interface to package
    // package.interface(sql_types_interface);

    // let mut output = String::new();
    // write!(output, "{}", package)?;

    // println!("Rendered WIT:\n{}", output);

    Ok(())
}

#[derive(Debug, Clone)]
pub struct SqlColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct SqlTable {
    pub name: String,
    pub columns: Vec<SqlColumn>,
}

impl SqlTable {
    pub fn parse(sql: &str) -> Result<Self> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)?;

        if ast.len() != 1 {
            return Err(anyhow!("Expected a single CREATE TABLE statement"));
        }

        match &ast[0] {
            Statement::CreateTable(create_table) => {
                let table_name = create_table.name.to_string();
                let parsed_columns: Vec<SqlColumn> =
                    create_table.columns.iter().map(parse_column).collect();

                Ok(SqlTable {
                    name: table_name,
                    columns: parsed_columns,
                })
            }
            _ => Err(anyhow!("Expected CREATE TABLE statement")),
        }
    }
}

fn parse_column(col: &ColumnDef) -> SqlColumn {
    let name = col.name.value.to_string();
    let data_type = col.data_type.clone();
    let nullable = !col
        .options
        .iter()
        .any(|opt| matches!(opt.option, ColumnOption::NotNull));

    SqlColumn {
        name,
        data_type,
        nullable,
    }
}

impl From<DataType> for PgValue {
    fn from(data_type: DataType) -> Self {
        match data_type {
            // Numeric types
            DataType::UnsignedInt8(_) => PgValue::BigInt(0),
            DataType::TinyInt(_)
            | DataType::UInt8
            | DataType::Int2(_)
            | DataType::SmallInt(_)
            | DataType::Int16 => PgValue::SmallInt(0),
            DataType::UnsignedTinyInt(_)
            | DataType::UnsignedInt2(_)
            | DataType::UnsignedSmallInt(_)
            | DataType::UInt16
            | DataType::MediumInt(_) => PgValue::Integer(0), // Map to larger type to accommodate unsigned range
            DataType::Int(_) | DataType::Integer(_) | DataType::Int4(_) | DataType::Int32 => {
                PgValue::Integer(0)
            }
            DataType::BigInt(_)
            | DataType::Int8(_)
            | DataType::Int64
            | DataType::UnsignedInt(_)
            | DataType::UnsignedInteger(_)
            | DataType::UnsignedInt4(_)
            | DataType::UnsignedMediumInt(_)
            | DataType::UInt32
            | DataType::UInt64 => PgValue::BigInt(0),
            DataType::UnsignedBigInt(_)
            | DataType::UInt128
            | DataType::UInt256
            | DataType::Int128
            | DataType::Int256
            | DataType::BigNumeric(_)
            | DataType::BigDecimal(_) => PgValue::Numeric(Numeric::default()), // Use Numeric for very large integers
            DataType::Numeric(_) | DataType::Decimal(_) | DataType::Dec(_) => {
                PgValue::Numeric(Numeric::default())
            }

            // Floating point types
            DataType::Float(precision) => match precision {
                Some(p) if p <= 24 => PgValue::Real(Default::default()),
                _ => PgValue::Double(Default::default()),
            },
            DataType::Float4 | DataType::Float32 | DataType::Real => {
                PgValue::Real(Default::default())
            }
            DataType::Float8 | DataType::Float64 | DataType::Double | DataType::DoublePrecision => {
                PgValue::Double(Default::default())
            }

            // Character types
            DataType::Char(_) | DataType::Character(_) | DataType::FixedString(_) => {
                PgValue::Char((1, Vec::new()))
            }
            DataType::Varchar(_)
            | DataType::CharacterVarying(_)
            | DataType::CharVarying(_)
            | DataType::Nvarchar(_) => PgValue::Varchar((None, Vec::new())),
            DataType::Text
            | DataType::String(_)
            | DataType::CharacterLargeObject(_)
            | DataType::CharLargeObject(_)
            | DataType::Clob(_) => PgValue::Text(String::new()),

            // Date and Time types
            DataType::Date | DataType::Date32 => PgValue::Date(Date::default()),
            DataType::Time(_, timezone_info) => match timezone_info {
                TimezoneInfo::Tz => PgValue::TimeTz(TimeTz::default()),
                _ => PgValue::Time(Time::default()),
            },
            DataType::Timestamp(_, timezone_info) => match timezone_info {
                TimezoneInfo::Tz => PgValue::TimestampTz(TimestampTz::default()),
                _ => PgValue::Timestamp(Timestamp::default()),
            },
            DataType::Datetime(_) => PgValue::Timestamp(Timestamp::default()), // Datetime is typically without timezone
            DataType::Datetime64(_, _) => PgValue::TimestampTz(TimestampTz::default()),
            DataType::Interval => PgValue::Interval(Interval::default()),

            // Binary types
            DataType::Binary(_)
            | DataType::Varbinary(_)
            | DataType::Blob(_)
            | DataType::Bytes(_)
            | DataType::Bytea => PgValue::Bytea(Vec::new()),

            // Boolean type
            DataType::Bool | DataType::Boolean => PgValue::Bool(false),

            // UUID type
            DataType::Uuid => PgValue::Uuid(String::new()),

            // JSON types
            DataType::JSON => PgValue::Json(String::new()),
            DataType::JSONB => PgValue::Jsonb(String::new()),

            // Complex types
            DataType::Array(inner_type) => {
                Self::convert_to_array_type(sqlparser::ast::DataType::Array(inner_type))
            }
            DataType::Map(_, _) => PgValue::Jsonb(String::new()), // Represent maps as JSONB
            DataType::Tuple(fields) | DataType::Struct(fields) => {
                PgValue::Hstore(Self::convert_fields_to_hstore(fields))
            }
            DataType::Nested(columns) => {
                PgValue::JsonArray(Self::convert_nested_to_json_array(columns))
            }
            DataType::Set(variants) => {
                PgValue::JsonArray(Self::convert_set_to_json_array(variants))
            }
            DataType::Enum(_) | DataType::Union(_) => PgValue::Json(String::new()), // Represent enums and unions as JSON objects

            // Special types
            DataType::Regclass => PgValue::Integer(0), // Regclass is internally an OID, which is an unsigned 32-bit integer
            DataType::Custom(name, _) => PgValue::Text(name.to_string()), // Custom types are represented as text
            DataType::Nullable(inner_type) => PgValue::from(*inner_type), // Use the inner type, as all PgValues can be null
            DataType::LowCardinality(inner_type) => PgValue::from(*inner_type), // PostgreSQL doesn't have a low cardinality concept
            DataType::Unspecified => PgValue::Null,
        }
    }
}

impl PgValue {
    fn convert_fields_to_hstore(fields: Vec<StructField>) -> Vec<(String, Option<String>)> {
        fields
            .into_iter()
            .map(|field| {
                let name = field
                    .field_name
                    .map(|ident| ident.value)
                    .unwrap_or_else(|| String::from(""));
                let value = Some(format!("{:?}", PgValue::from(field.field_type)));
                (name, value)
            })
            .collect()
    }

    fn convert_nested_to_json_array(columns: Vec<ColumnDef>) -> Vec<String> {
        columns
            .into_iter()
            .map(|col| {
                let column_json = json!({
                    "name": col.name.value,
                    "data_type": format!("{:?}", col.data_type),
                    "collation": col.collation.map(|c| c.to_string()),
                    "options": col.options.iter().map(|opt| format!("{:?}", opt)).collect::<Vec<String>>()
                });
                serde_json::to_string(&column_json).unwrap_or_default()
            })
            .collect()
    }

    fn convert_set_to_json_array(variants: Vec<String>) -> Vec<String> {
        variants
    }

    fn convert_to_array_type(inner_type: DataType) -> Self {
        match PgValue::from(inner_type) {
            PgValue::BigInt(_) | PgValue::Int8(_) => PgValue::Int8Array(Vec::new()),
            PgValue::SmallInt(_) | PgValue::Int2(_) => PgValue::Int2Array(Vec::new()),
            PgValue::Integer(_) | PgValue::Int(_) | PgValue::Int4(_) => {
                PgValue::Int4Array(Vec::new())
            }
            PgValue::Bool(_) | PgValue::Boolean(_) => PgValue::BoolArray(Vec::new()),
            PgValue::Double(_) | PgValue::Float8(_) => PgValue::Float8Array(Vec::new()),
            PgValue::Real(_) | PgValue::Float4(_) => PgValue::Float4Array(Vec::new()),
            PgValue::Numeric(_) | PgValue::Decimal(_) => PgValue::NumericArray(Vec::new()),
            PgValue::Char(_) => PgValue::CharArray(Vec::new()),
            PgValue::Varchar(_) => PgValue::VarcharArray(Vec::new()),
            PgValue::Text(_) => PgValue::TextArray(Vec::new()),
            PgValue::Date(_) => PgValue::DateArray(Vec::new()),
            PgValue::Time(_) => PgValue::TimeArray(Vec::new()),
            PgValue::TimeTz(_) => PgValue::TimeTzArray(Vec::new()),
            PgValue::Timestamp(_) => PgValue::TimestampArray(Vec::new()),
            PgValue::TimestampTz(_) => PgValue::TimestampTzArray(Vec::new()),
            PgValue::Interval(_) => PgValue::IntervalArray(Vec::new()),
            PgValue::Bytea(_) => PgValue::ByteaArray(Vec::new()),
            PgValue::Uuid(_) => PgValue::UuidArray(Vec::new()),
            PgValue::Json(_) => PgValue::JsonArray(Vec::new()),
            PgValue::Jsonb(_) => PgValue::JsonbArray(Vec::new()),
            PgValue::Inet(_) => PgValue::InetArray(Vec::new()),
            PgValue::Cidr(_) => PgValue::CidrArray(Vec::new()),
            PgValue::Macaddr(_) => PgValue::MacaddrArray(Vec::new()),
            PgValue::Macaddr8(_) => PgValue::Macaddr8Array(Vec::new()),
            PgValue::Box(_) => PgValue::BoxArray(Vec::new()),
            PgValue::Circle(_) => PgValue::CircleArray(Vec::new()),
            PgValue::Line(_) => PgValue::LineArray(Vec::new()),
            PgValue::Lseg(_) => PgValue::LsegArray(Vec::new()),
            PgValue::Path(_) => PgValue::PathArray(Vec::new()),
            PgValue::Point(_) => PgValue::PointArray(Vec::new()),
            PgValue::Polygon(_) => PgValue::PolygonArray(Vec::new()),
            PgValue::Money(_) => PgValue::MoneyArray(Vec::new()),
            PgValue::PgLsn(_) => PgValue::PgLsnArray(Vec::new()),
            PgValue::Name(_) => PgValue::NameArray(Vec::new()),
            PgValue::Xml(_) => PgValue::XmlArray(Vec::new()),
            PgValue::Bit(_) => PgValue::BitArray(Vec::new()),
            PgValue::BitVarying(_) | PgValue::Varbit(_) => PgValue::VarbitArray(Vec::new()),
            PgValue::Int2Vector(_) => PgValue::Int2VectorArray(Vec::new()),
            _ => PgValue::Null,
        }
    }
}

impl From<&str> for PgValue {
    fn from(value: &str) -> Self {
        match value.to_lowercase().as_str() {
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
            "char" => PgValue::Char((1, vec![])),
            s if s.starts_with("char(") => {
                let size = s
                    .trim_start_matches("char(")
                    .trim_end_matches(')')
                    .parse::<u32>()
                    .unwrap_or(1);
                PgValue::Char((size, vec![]))
            }
            "char[]" => PgValue::CharArray(vec![]),
            "varchar" => PgValue::Varchar((None, vec![])),
            s if s.starts_with("varchar(") => {
                let size = s
                    .trim_start_matches("varchar(")
                    .trim_end_matches(')')
                    .parse::<u32>()
                    .ok();
                PgValue::Varchar((size, vec![]))
            }
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
}

impl TryFrom<SqlTable> for Record {
    type Error = anyhow::Error;

    fn try_from(table: SqlTable) -> Result<Self, Self::Error> {
        let fields = table
            .columns
            .iter()
            .map(|column| {
                let pg_value: PgValue = column.data_type.clone().into();
                let wit_type = Type::try_from(pg_value)?;
                Ok(Field::new(Ident::new(column.name.clone()), wit_type))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Record::new(fields))
    }
}

impl TryFrom<PgValue> for Type {
    type Error = anyhow::Error;

    fn try_from(pg_value: PgValue) -> Result<Self, Self::Error> {
        Type::try_from(&pg_value)
    }
}

impl TryFrom<&PgValue> for Type {
    type Error = anyhow::Error;

    fn try_from(pg_value: &PgValue) -> Result<Self, Self::Error> {
        match pg_value {
            PgValue::Null => Err(anyhow!("Unable to map null to WIT type")),
            PgValue::BigInt(_) | PgValue::Int8(_) | PgValue::BigSerial(_) | PgValue::Serial8(_) => {
                Ok(Type::S64)
            }
            PgValue::Bool(_) | PgValue::Boolean(_) => Ok(Type::Bool),
            PgValue::Double(_) | PgValue::Float8(_) => Ok(Type::Named(Ident::new("hashable-f64"))),
            PgValue::Real(_) | PgValue::Float4(_) => Ok(Type::Named(Ident::new("hashable-f32"))),
            PgValue::Integer(_) | PgValue::Int(_) | PgValue::Int4(_) => Ok(Type::S32),
            PgValue::Numeric(_) | PgValue::Decimal(_) => Ok(Type::Named(Ident::new("numeric"))),
            PgValue::Serial(_) | PgValue::Serial4(_) => Ok(Type::U32),
            PgValue::SmallInt(_)
            | PgValue::Int2(_)
            | PgValue::SmallSerial(_)
            | PgValue::Serial2(_) => Ok(Type::S16),
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
            PgValue::Float8Array(_) => Ok(Type::List(Box::new(Type::Named(Ident::new(
                "hashable-f64",
            ))))),
            PgValue::Float4Array(_) => Ok(Type::List(Box::new(Type::Named(Ident::new(
                "hashable-f32",
            ))))),
            PgValue::Int4Array(_) => Ok(Type::List(Box::new(Type::S32))),
            PgValue::NumericArray(_) => {
                Ok(Type::List(Box::new(Type::Named(Ident::new("numeric")))))
            }
            PgValue::Int2Array(_) | PgValue::Int2Vector(_) => Ok(Type::List(Box::new(Type::S16))),
            PgValue::Int2VectorArray(_) => {
                Ok(Type::List(Box::new(Type::List(Box::new(Type::S16)))))
            }
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
            PgValue::Char(_) | PgValue::Varchar(_) | PgValue::Text(_) | PgValue::Name(_) => {
                Ok(Type::String)
            }
            PgValue::CharArray(_)
            | PgValue::VarcharArray(_)
            | PgValue::TextArray(_)
            | PgValue::NameArray(_) => Ok(Type::List(Box::new(Type::String))),
            PgValue::Date(_) => Ok(Type::Named(Ident::new("date"))),
            PgValue::DateArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("date"))))),
            PgValue::Time(_) => Ok(Type::Named(Ident::new("time"))),
            PgValue::TimeArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("time"))))),
            PgValue::TimeTz(_) => Ok(Type::Named(Ident::new("time-tz"))),
            PgValue::TimeTzArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new("time-tz"))))),
            PgValue::Timestamp(_) => Ok(Type::Named(Ident::new("timestamp"))),
            PgValue::TimestampArray(_) => {
                Ok(Type::List(Box::new(Type::Named(Ident::new("timestamp")))))
            }
            PgValue::TimestampTz(_) => Ok(Type::Named(Ident::new("timestamp-tz"))),
            PgValue::TimestampTzArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new(
                "timestamp-tz",
            ))))),
            PgValue::Interval(_) => Ok(Type::Named(Ident::new("interval"))),
            PgValue::IntervalArray(_) => {
                Ok(Type::List(Box::new(Type::Named(Ident::new("interval")))))
            }
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
            PgValue::Path(_) | PgValue::Polygon(_) => {
                Ok(Type::List(Box::new(Type::Named(Ident::new("point")))))
            }
            PgValue::PathArray(_) | PgValue::PolygonArray(_) => Ok(Type::List(Box::new(
                Type::List(Box::new(Type::Named(Ident::new("point")))),
            ))),
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
            PgValue::MacaddrArray(_) => Ok(Type::List(Box::new(Type::Named(Ident::new(
                "mac-address-eui48",
            ))))),
            PgValue::Macaddr8(_) => Ok(Type::Named(Ident::new("mac-address-eui64"))),
            PgValue::Macaddr8Array(_) => Ok(Type::List(Box::new(Type::Named(Ident::new(
                "mac-address-eui64",
            ))))),
            PgValue::Inet(_) | PgValue::Cidr(_) => Ok(Type::String),
            PgValue::InetArray(_) | PgValue::CidrArray(_) => Ok(Type::List(Box::new(Type::String))),
            PgValue::Json(_) | PgValue::Jsonb(_) => Ok(Type::String),
            PgValue::JsonArray(_) | PgValue::JsonbArray(_) => {
                Ok(Type::List(Box::new(Type::String)))
            }
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
}

impl Default for Interval {
    fn default() -> Self {
        Interval {
            end: Date::default(),
            end_inclusive: bool::default(),
            start: Date::default(),
            start_inclusive: bool::default(),
        }
    }
}

impl Default for TimestampTz {
    fn default() -> Self {
        TimestampTz {
            offset: Offset::default(),
            timestamp: Timestamp::default(),
        }
    }
}

impl Default for Offset {
    fn default() -> Self {
        // Default to UTC (no offset)
        Offset::WesternHemisphereSecs(0)
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Timestamp {
            date: Date::default(),
            time: Time::default(),
        }
    }
}

impl Default for TimeTz {
    fn default() -> Self {
        TimeTz {
            timesonze: String::default(),
            time: Time::default(),
        }
    }
}

impl Default for Date {
    fn default() -> Self {
        Date::PositiveInfinity
    }
}

impl Default for Time {
    fn default() -> Self {
        Time {
            hour: 0,
            min: 0,
            sec: 0,
            micro: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use semver::Version;
    use sqlparser::ast::DataType;
    use wit_encoder::{
        Ident, Interface, InterfaceItem, Package, PackageName, Type, TypeDef, TypeDefKind,
    };

    #[test]
    fn test_str_to_pg_value() {
        assert!(matches!(PgValue::from("integer"), PgValue::Integer(_)));
        assert!(matches!(PgValue::from("text"), PgValue::Text(_)));
        assert!(matches!(PgValue::from("boolean"), PgValue::Bool(_)));
        assert!(matches!(PgValue::from("numeric"), PgValue::Numeric(_)));
        assert!(matches!(PgValue::from("timestamp"), PgValue::Timestamp(_)));
        assert!(matches!(PgValue::from("varchar(100)"), PgValue::Varchar(_)));
        assert!(matches!(PgValue::from("unknown_type"), PgValue::Null));
    }

    #[test]
    fn test_pg_value_to_wit_type() -> Result<()> {
        assert_eq!(Type::try_from(&PgValue::Integer(0))?, Type::S32);
        assert_eq!(
            Type::try_from(&PgValue::Text("".to_string()))?,
            Type::String
        );
        assert_eq!(Type::try_from(&PgValue::Bool(false))?, Type::Bool);
        assert_eq!(
            Type::try_from(&PgValue::Numeric(Numeric::default()))?,
            Type::Named(Ident::new("numeric"))
        );
        assert_eq!(
            Type::try_from(&PgValue::Timestamp(Timestamp::default()))?,
            Type::Named(Ident::new("timestamp"))
        );
        assert_eq!(
            Type::try_from(&PgValue::Varchar((None, vec![])))?,
            Type::String
        );

        assert!(Type::try_from(&PgValue::Null).is_err());

        // Test some array types
        assert_eq!(
            Type::try_from(&PgValue::Int8Array(vec![]))?,
            Type::List(Box::new(Type::S64))
        );
        assert_eq!(
            Type::try_from(&PgValue::BoolArray(vec![]))?,
            Type::List(Box::new(Type::Bool))
        );

        // Test some geometric types
        assert_eq!(
            Type::try_from(&PgValue::Point(Default::default()))?,
            Type::Named(Ident::new("point"))
        );

        // Test some network address types
        assert_eq!(
            Type::try_from(&PgValue::Inet("".to_string()))?,
            Type::String
        );

        // Test JSON types
        assert_eq!(
            Type::try_from(&PgValue::Json("{}".to_string()))?,
            Type::String
        );

        Ok(())
    }

    #[test]
    fn test_create_wit_record() -> Result<()> {
        let columns = vec![
            SqlColumn {
                name: "id".to_string(),
                data_type: DataType::Int(None),
                nullable: false,
            },
            SqlColumn {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: false,
            },
            SqlColumn {
                name: "is_active".to_string(),
                data_type: DataType::Boolean,
                nullable: true,
            },
            SqlColumn {
                name: "balance".to_string(),
                data_type: DataType::Numeric(ExactNumberInfo::None),
                nullable: true,
            },
            SqlColumn {
                name: "created_at".to_string(),
                data_type: DataType::Timestamp(None, TimezoneInfo::None),
                nullable: true,
            },
        ];

        let table = SqlTable {
            name: "test_table".to_string(),
            columns,
        };

        let mut record = Record::try_from(table)?;
        let fields = record.fields_mut();

        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].name(), &Ident::new("id"));
        assert_eq!(fields[0].ty(), &Type::S32);
        assert_eq!(fields[1].name(), &Ident::new("name"));
        assert_eq!(fields[1].ty(), &Type::String);
        assert_eq!(fields[2].name(), &Ident::new("is_active"));
        assert_eq!(fields[2].ty(), &Type::Bool);
        assert_eq!(fields[3].name(), &Ident::new("balance"));
        assert_eq!(fields[3].ty(), &Type::Named(Ident::new("numeric")));
        assert_eq!(fields[4].name(), &Ident::new("created_at"));
        assert_eq!(fields[4].ty(), &Type::Named(Ident::new("timestamp")));

        Ok(())
    }

    #[test]
    fn test_sql_table_parse() -> Result<()> {
        let sql = r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE,
            age INTEGER,
            is_active BOOLEAN DEFAULT true,
            balance NUMERIC(10,2),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        "#;

        let table = SqlTable::parse(sql)?;

        assert_eq!(table.name, "users", "Table name mismatch");
        assert_eq!(
            table.columns.len(),
            7,
            "Expected 7 columns, but found {}",
            table.columns.len()
        );

        let expected_columns = vec![
            ("id", DataType::Integer(None), false), // Changed from Serial to Integer
            (
                "name",
                DataType::Varchar(Some(CharacterLength::IntegerLength {
                    length: 100,
                    unit: None,
                })),
                false,
            ),
            (
                "email",
                DataType::Varchar(Some(CharacterLength::IntegerLength {
                    length: 100,
                    unit: None,
                })),
                true,
            ),
            ("age", DataType::Integer(None), true),
            ("is_active", DataType::Boolean, true),
            (
                "balance",
                DataType::Numeric(ExactNumberInfo::PrecisionAndScale(10, 2)),
                true,
            ),
            (
                "created_at",
                DataType::Timestamp(None, TimezoneInfo::Tz),
                true,
            ),
        ];

        for (i, (expected_name, expected_type, expected_nullable)) in
            expected_columns.iter().enumerate()
        {
            assert_eq!(
                table.columns[i].name, *expected_name,
                "Column {} name mismatch",
                i
            );
            assert_eq!(
                table.columns[i].data_type, *expected_type,
                "Column {} type mismatch",
                i
            );
            assert_eq!(
                table.columns[i].nullable, *expected_nullable,
                "Column {} nullability mismatch",
                i
            );
        }

        Ok(())
    }

    #[test]
    fn test_pgvalue_from_datatype() {
        assert!(matches!(
            PgValue::from(DataType::Int(None)),
            PgValue::Integer(_)
        ));
        assert!(matches!(
            PgValue::from(DataType::Varchar(None)),
            PgValue::Varchar(_)
        ));
        assert!(matches!(PgValue::from(DataType::Boolean), PgValue::Bool(_)));
        assert!(matches!(PgValue::from(DataType::Date), PgValue::Date(_)));
        assert!(matches!(
            PgValue::from(DataType::Timestamp(None, TimezoneInfo::None)),
            PgValue::Timestamp(_)
        ));
        assert!(matches!(
            PgValue::from(DataType::Timestamp(None, TimezoneInfo::Tz)),
            PgValue::TimestampTz(_)
        ));
        assert!(matches!(
            PgValue::from(DataType::Numeric(ExactNumberInfo::None)),
            PgValue::Numeric(_)
        ));
        assert!(matches!(PgValue::from(DataType::JSON), PgValue::Json(_)));
        assert!(matches!(PgValue::from(DataType::Uuid), PgValue::Uuid(_)));
    }

    #[test]
    fn test_parse_table_with_array_and_custom_types() -> Result<()> {
        let sql = "CREATE TABLE advanced_sample (
            id INT PRIMARY KEY,
            int_array INT[],
            text_array TEXT[],
            point_type POINT,
            custom_enum MOOD
        )";

        let table = SqlTable::parse(sql)?;

        assert_eq!(table.name, "advanced_sample");
        assert_eq!(table.columns.len(), 5);

        assert_eq!(table.columns[1].name, "int_array");
        assert!(matches!(table.columns[1].data_type, DataType::Array(_)));

        assert_eq!(table.columns[2].name, "text_array");
        assert!(matches!(table.columns[2].data_type, DataType::Array(_)));

        assert_eq!(table.columns[3].name, "point_type");
        assert!(matches!(table.columns[3].data_type, DataType::Custom(_, _)));

        assert_eq!(table.columns[4].name, "custom_enum");
        assert!(matches!(table.columns[4].data_type, DataType::Custom(_, _)));

        Ok(())
    }
}
