pub enum Query {
    SELECT(SelectQuery),
    UPDATE(UpdateQuery),
    DELETE(DeleteQuery),
    INSERT(InsertQuery),
}

pub struct SelectQuery {
    table: Table,
    where: Where,
    order_by: OrderBy,
    limit: u32,
}

pub struct UpdateQuery {
    table: Table,
    pkey_value: Vec<Value>,
    record: Record,
}

pub struct DeleteQuery {
    table: Table,
    pkey_value: Vec<Value>,
}

pub struct InsertQuery {
    table: Table,
    pkey_value: Vec<Value>,
    record: Record,
}

pub struct Condition {
    column: String,
    comparison_operator: ComparisonOperator,
    value: Value,
}

pub enum Where {
    Condition(Condition),
    AND(Vec<Where>),
    OR(Vec<Where>),
}

pub enum ComparisonOperator {
    EQ,
    NEQ,
    GT,
    GTE,
    LT,
    LTE,
    IN,
    NOT_IN,
}

pub enum Value {
    Null,
    String(String),
    Int(i32),
    Float(f64),
    Bool(bool),
    Date(chrono::NaiveDate),
    Datetime(chrono::NaiveDateTime),
    List(Vec<Value>),
    Map(std::collections::HashMap<Value, Value>),
}

pub enum ValueType {
    String,
    Int,
    Float,
    Bool,
    Date,
    Datetime,
    List(Box<ValueType>),
    Map(Box<ValueType>, Box<ValueType>),
}


pub struct OrderBy {
    column: Column,
    direction: Order,
}

pub enum Order {
    ASC,
    DESC,
}

pub struct Table {
    name: String,
    pkey: Vec<Column>,
    columns: Vec<Column>,
}

pub struct Column {
    name: String,
    r#type: ValueType,
}

pub struct Record {
    table: Table,
    pkey_value: HashMap<Column, Value>,
    data: HashMap<Column, Value>,
}

pub struct DataFrameRecord {
    data: HashMap<Column, Value>,
}

pub struct DataFrameKey {
    pkey_value: HashMap<Column, Value>,
}

pub struct DataFrame {
    records: HashMap<DataFrameKey, DataFrameRecord>,
}

pub enum DataFrameOperation {
    Delete(DataFrameKey),
    Add(DataFrameKey, DataFrameRecord),
}

