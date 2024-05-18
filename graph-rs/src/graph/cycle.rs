use std::collections::VecDeque;
use pyo3::prelude::*;
use pyo3::{pyclass, pymethods, PyResult};
use pyo3::types::{PySequence, PyTuple};
use crate::graph::edge::EdgeRS;
use crate::graph::node::GraphNodeRS;


#[pyclass]
pub struct CycleRS {
    q: VecDeque<(GraphNodeRS, EdgeRS)>,
}

#[pymethods]
impl CycleRS {
    #[new]
    fn new(
        py_q: PySequence,
    ) -> PyResult<Self> {
        let q: VecDeque<(GraphNodeRS, EdgeRS)> = py_q
            .iter()?
            .map(|item| {
                let tuple = item.unwrap();
                (
                    tuple.get_item(0).unwrap(),
                    tuple.get_item(1).unwrap()
                )
            })
            .collect();

        Ok(CycleRS { q: q })
    }

    fn __len__(&self) -> usize {
        self.q.len().unwrap()
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Py<CycleIterator>> {
        Py::new(py, CycleIterator {
            iter: Box::new(self.q.iter())
        })
    }

    fn __getitem__<'py>(&self, item: usize) -> PyResult<&'py PyAny> {
        self.q.get_item(item)
    }

    fn validate_cycle(&self) -> bool {
        let (_, mut prev_edge) = self.q.get(
            self.q.len() - 1
        ).unwrap();
        for (node, edge) in self.q.iter() {
            if prev_edge.next_node_index != node.index {
                return false
            }
            prev_edge = *edge
        };
        true
    }

    fn iter_by_pairs_reversed<'py>(&self, py: Python<'py>)  -> PyResult<Py<CyclePairsIterator>> {
        let q_iter = self.q.iter().rev();
        let (end_node, _) = self.q.get(0).unwrap();
        let &next_node = end_node;

        Py::new(py, CyclePairsIterator {
            next_node,
            rev_iter: Box::new(q_iter),
        })
    }

}


#[pyclass]
struct CycleIterator {
    iter: Box<dyn Iterator<Item = (GraphNodeRS, EdgeRS)> + Send>,
}


#[pymethods]
impl CycleIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        match slf.iter.next() {
            Some((node, edge)) => {
                let to_python_iter: Vec<dyn ToPyObject> = vec![node, edge];
                let py_tuple = PyTuple::new_bound(slf.py(), to_python_iter.iter());
                Some(py_tuple.to_object(slf.py()))
            },
            None => None
        }
    }
}


#[pyclass]
struct CyclePairsIterator {
    next_node: GraphNodeRS,
    rev_iter: Box<dyn Iterator<Item = (GraphNodeRS, EdgeRS)> + Send>,
}


#[pymethods]
impl CyclePairsIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        match slf.rev_iter.next() {
            Some((node, edge)) => {
                let to_python_iter: Vec<Box<dyn ToPyObject>> = vec![
                    Box::new(node),
                    Box::new(edge),
                    Box::new(slf.next_node)
                ];
                let py_tuple = PyTuple::new_bound(slf.py(), to_python_iter.iter());
                slf.next_node = node;
                Some(py_tuple.to_object(slf.py()))
            },
            None => None
        }
    }
}