use columnvalueops::{ColumnValueOps, ColumnValueOpsExt};
use super::super::sexpression::AggregateOp;

pub trait AggregateFunction<ColumnValue> {
    fn feed(&mut self, value: ColumnValue);
    fn finish(self: Box<Self>) -> ColumnValue;
}

struct Count {
    count: u64
}

impl<ColumnValue: ColumnValueOps> AggregateFunction<ColumnValue> for Count
{
    fn feed(&mut self, value: ColumnValue) {
        if !value.is_null() {
            self.count += 1;
        }
    }

    fn finish(self: Box<Self>) -> ColumnValue {
        ColumnValueOps::from_u64(self.count)
    }
}

struct Avg {
    sum: f64,
    count: u64
}

impl<ColumnValue: ColumnValueOps> AggregateFunction<ColumnValue> for Avg {
    fn feed(&mut self, value: ColumnValue) {
        if !value.is_null() {
            self.sum += value.to_f64().unwrap();
            self.count += 1;
        }
    }

    fn finish(self: Box<Self>) -> ColumnValue {
        if self.count == 0 {
            ColumnValueOpsExt::null()
        } else {
            ColumnValueOps::from_f64(self.sum / (self.count as f64))
        }
    }
}

struct Sum {
    sum: f64,
    count: u64
}

impl<ColumnValue: ColumnValueOps> AggregateFunction<ColumnValue> for Sum {
    fn feed(&mut self, value: ColumnValue) {
        if !value.is_null() {
            self.sum += value.to_f64().unwrap();
            self.count += 1;
        }
    }

    fn finish(self: Box<Self>) -> ColumnValue {
        if self.count == 0 {
            ColumnValueOpsExt::null()
        } else {
            ColumnValueOps::from_f64(self.sum)
        }
    }
}

struct Min<ColumnValue> {
    value: Option<ColumnValue>
}

impl<ColumnValue: ColumnValueOps> AggregateFunction<ColumnValue> for Min<ColumnValue> {
    fn feed(&mut self, value: ColumnValue) {
        let set = !value.is_null() && if let Some(r) = self.value.as_ref() {
            match value.compare(r) {
                Some(-1) => true,
                _ => false
            }
        } else {
            true
        };

        if set { self.value = Some(value); }
    }

    fn finish(self: Box<Self>) -> ColumnValue {
        self.value.unwrap_or_else(|| ColumnValueOpsExt::null())
    }
}

struct Max<ColumnValue> {
    value: Option<ColumnValue>
}

impl<ColumnValue: ColumnValueOps> AggregateFunction<ColumnValue> for Max<ColumnValue> {
    fn feed(&mut self, value: ColumnValue) {
        let set = !value.is_null() && if let Some(r) = self.value.as_ref() {
            match value.compare(r) {
                Some(1) => true,
                _ => false
            }
        } else {
            true
        };

        if set { self.value = Some(value); }
    }

    fn finish(self: Box<Self>) -> ColumnValue {
        self.value.unwrap_or_else(|| ColumnValueOpsExt::null())
    }
}

pub fn get_aggregate_function<ColumnValue>(op: AggregateOp) -> Box<AggregateFunction<ColumnValue> + 'static>
where ColumnValue: Sized + ColumnValueOps + 'static
{
    match op {
        AggregateOp::Count => Box::new(Count { count: 0 }),
        AggregateOp::Avg => Box::new(Avg { sum: 0.0, count: 0 }),
        AggregateOp::Sum => Box::new(Sum { sum: 0.0, count: 0 }),
        AggregateOp::Min => Box::new(Min { value: None }),
        AggregateOp::Max => Box::new(Max { value: None })
    }
}
