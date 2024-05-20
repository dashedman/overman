use std::fmt;

use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::{pyclass, pymethods, PyResult};


#[pyclass]
#[derive(Clone)]
pub struct GraphNodeRS {
    #[pyo3(get, set)]
    pub index: usize,
    #[pyo3(get, set)]
    // Vec<Py<EdgeRS>>
    pub edges: Py<PyList>,
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
        edges: Bound<'_, PyList>,
        value: String,
    ) -> Self {
        let unbound_edge = edges.unbind();
        GraphNodeRS {
            index,
            edges: unbound_edge,
            value,
        }
    }

    fn __str__(&self) -> String {
        format!(
            "GraphNodeRS(index={}, edges={}, value={})",
            self.index, self.edges, self.value,
        )
    }

    fn py_copy(&self) -> PyResult<Self> {
        Ok((*self).clone())
    }
}


impl fmt::Display for GraphNodeRS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.__str__())
    }
}