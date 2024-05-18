use pyo3::prelude::*;
use pyo3::{pyclass, pymethods, PyResult};
use crate::graph::edge::EdgeRS;


#[pyclass]
#[derive(Clone)]
pub struct GraphNodeRS {
    #[pyo3(get, set)]
    pub index: usize,
    #[pyo3(get, set)]
    pub edges: Vec<EdgeRS>,
    #[pyo3(get, set)]
    pub value: String,
}

#[pymethods]
impl GraphNodeRS {
    #[new]
    #[pyo3(signature = (
        index,
        edges,
        value,
    ))]
    fn new(
        index: usize,
        edges: Vec<EdgeRS>,
        value: String,
    ) -> Self {
        GraphNodeRS {
            index,
            edges,
            value,
        }
    }

    fn py_copy(&self) -> PyResult<Self> {
        Ok((*self).clone())
    }
}