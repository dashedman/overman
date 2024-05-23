mod edge;
mod node;
mod cycle;
mod graph;


use std::str::FromStr;

use pyo3::exceptions::PyValueError;
use pyo3::types::PyAnyMethods;
use pyo3::Bound;
use pyo3::PyAny;
use pyo3::PyResult;
use rust_decimal::Decimal;

pub use self::edge::*;
pub use self::node::*;
pub use self::cycle::*;
pub use self::graph::*;


fn py_to_decimal(obj: &Bound<PyAny>) -> PyResult<Decimal>{
    let new_decimal;
    if let Ok(val) = obj.extract() {
        new_decimal = Decimal::new(val, 0);
    } else {
        let rs_str = obj.to_string();
        new_decimal = Decimal::from_str(&rs_str).or_else(|_| {
            Decimal::from_scientific(&rs_str).map_err(|e| PyValueError::new_err(e.to_string()))
        })?
    }
    Ok(new_decimal)
}