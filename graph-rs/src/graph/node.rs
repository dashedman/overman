use std::fmt;
use std::slice::Iter;

use pyo3::prelude::*;
use pyo3::types::{PyIterator, PyList};
use pyo3::{pyclass, pymethods, PyResult};

use super::Vec_EdgeRS;


#[pyclass]
#[derive(Clone)]
pub struct GraphNodeRS {
    #[pyo3(get, set)]
    pub index: usize,
    #[pyo3(get, set)]
    pub edges: Py<Vec_EdgeRS>,
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
        py: Python,
        index: usize,
        edges: Bound<'_, PyList>,
        value: String,
    ) -> PyResult<Self> {
        let edges = Vec_EdgeRS::from_pylist(py, edges)?;
        Ok(GraphNodeRS {
            index,
            edges,
            value,
        })
    }

    fn __str__(&self) -> String {
        format!(
            "GraphNodeRS(index={}, edges={:?}, value={})",
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


#[pyclass(sequence)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct Vec_NodeRS {
    pub vec: Vec<Py<GraphNodeRS>>,
}


#[pymethods]
impl Vec_NodeRS {
    fn __len__<'py>(&self) -> PyResult<usize> {
        Ok(self.vec.len())
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyIterator>> { 
        PyIterator::from_bound_object( &PyList::new_bound(py, self.vec.clone()) )
    }

    fn __getitem__<'py>(&self, py: Python<'py>, index: usize) -> PyResult<&Bound<'py, GraphNodeRS>> {
        Ok(self.vec[index].bind(py))
    }

    fn __delitem__<'py>(&mut self, index: usize) -> PyResult<()> {
        self.vec.remove(index);
        Ok(())
    }
}


impl Vec_NodeRS {
    pub fn from_pylist(py: Python, nodes_list: Bound<PyList>) -> PyResult<Py<Vec_NodeRS>> {
        let mut nodes_vec = Vec::with_capacity(nodes_list.len());

        for node_pa in nodes_list.iter() {
            let node = node_pa.downcast_into::<GraphNodeRS>()?;
            nodes_vec.push(node.unbind());
        }

        Py::new(py, Vec_NodeRS {vec: nodes_vec})
    }

    pub fn iter(&self) -> Iter<Py<GraphNodeRS>> {
        self.vec.iter()
    }
}