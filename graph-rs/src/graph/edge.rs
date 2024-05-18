use rust_decimal::prelude::*;
use pyo3::prelude::*;
use pyo3::{pyclass, pymethods, PyResult, Python};
use pyo3::exceptions::PyValueError;


#[pyclass]
#[derive(Copy, Clone)]
pub struct EdgeRS {
    #[pyo3(get, set)]
    pub origin_node_index: usize,
    #[pyo3(get, set)]
    pub next_node_index: usize,
    #[pyo3(get, set)]
    pub val: f64,
    #[pyo3(get, set)]
    pub volume: f64,
    #[pyo3(get, set)]
    pub inverted: bool,
    #[pyo3(get)]
    pub original_price: Decimal,
}

#[pymethods]
impl EdgeRS {
    #[new]
    #[pyo3(signature = (
        origin_node_index,
        next_node_index,
        val=1.0,
        volume=1.0,
        inverted=false,
        original_price=None,
    ))]
    fn new(
        origin_node_index: usize,
        next_node_index: usize,
        val: f64,
        volume: f64,
        inverted: bool,
        original_price: Option<Decimal>,
    ) -> Self {
        EdgeRS {
            origin_node_index,
            next_node_index,
            val,
            volume,
            inverted,
            original_price: original_price.unwrap_or(
                Decimal::new(1, 0)
            ),
        }
    }

    #[setter]
    fn original_price(&mut self, obj: &Bound<'_, PyAny>) -> PyResult<()> {
        let new_decimal;
        if let Ok(val) = obj.extract() {
            new_decimal = Decimal::new(val, 0);
        } else {
            let rs_str = obj.to_string();
            new_decimal = Decimal::from_str(&rs_str).or_else(|_| {
                Decimal::from_scientific(&rs_str).map_err(|e| PyValueError::new_err(e.to_string()))
            })?
        }

        self.original_price = new_decimal;
        Ok(())
    }

    fn py_copy(&self) -> PyResult<Self> {
        Ok((*self).clone())
    }
}