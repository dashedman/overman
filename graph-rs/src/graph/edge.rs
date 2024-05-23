use std::fmt::{self, Display, Formatter};
use std::slice::Iter;

use pyo3::types::{PyIterator, PyList};
use rust_decimal::prelude::*;
use pyo3::prelude::*;
use pyo3::{pyclass, pymethods, PyResult};

use super::py_to_decimal;


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
    pub volume: Decimal,
    #[pyo3(get, set)]
    pub inverted: bool,
    #[pyo3(get)]
    pub original_price: Decimal,
}


#[pyclass(sequence)]
#[derive(Debug)]
pub struct Vec_EdgeRS {
    pub vec: Vec<Py<EdgeRS>>,
}


#[pymethods]
impl Vec_EdgeRS {
    fn __len__<'py>(&self) -> PyResult<usize> {
        Ok(self.vec.len())
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyIterator>> { 
        PyIterator::from_bound_object( &PyList::new_bound(py, self.vec.clone()) )
    }

    fn __getitem__<'py>(&self, py: Python<'py>, index: usize) -> PyResult<&Bound<'py, EdgeRS>> {
        Ok(self.vec[index].bind(py))
    }

    fn __delitem__<'py>(&mut self, index: usize) -> PyResult<()> {
        self.vec.remove(index);
        Ok(())
    }
}


impl Vec_EdgeRS {
    pub fn from_pylist(py: Python, edges_list: Bound<PyList>) -> PyResult<Py<Vec_EdgeRS>> {
        let mut edges_vec = Vec::with_capacity(edges_list.len());

        for edge_pa in edges_list.iter() {
            let edge = edge_pa.downcast_into::<EdgeRS>()?;
            edges_vec.push(edge.unbind());
        }

        Py::new(py, Vec_EdgeRS {vec: edges_vec})
    }

    pub fn iter(&self) -> Iter<Py<EdgeRS>> {
        self.vec.iter()
    }
}


#[pymethods]
impl EdgeRS {
    #[new]
    #[pyo3(signature = (
        origin_node_index,
        next_node_index,
        val=1.0,
        volume=None,
        inverted=false,
        original_price=None,
    ))]
    fn new(
        origin_node_index: usize,
        next_node_index: usize,
        val: f64,
        volume: Option<Decimal>,
        inverted: bool,
        original_price: Option<Decimal>,
    ) -> Self {
        EdgeRS {
            origin_node_index,
            next_node_index,
            val,
            volume: volume.unwrap_or(
                Decimal::new(1, 0)
            ),
            inverted,
            original_price: original_price.unwrap_or(
                Decimal::new(1, 0)
            ),
        }
    }

    fn __str__(&self) -> String {
        format!(
            "EdgeRS(origin_node_index={}, next_node_index={}, val={}, volume={}, inverted={}, original_price={})",
            self.origin_node_index, self.next_node_index,
            self.val, self.volume, self.inverted,
            self.original_price
        )
    }

    #[setter]
    fn original_price(&mut self, obj: &Bound<'_, PyAny>) -> PyResult<()> {
        self.original_price = py_to_decimal(obj)?;
        Ok(())
    }

    fn py_copy(&self) -> PyResult<Self> {
        Ok((*self).clone())
    }
}


impl Display for EdgeRS {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.__str__())
    }
}
