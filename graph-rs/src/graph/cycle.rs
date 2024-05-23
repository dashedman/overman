use std::collections::vec_deque::Iter;
use std::collections::VecDeque;
use std::fmt;
use std::ops::{Index, IndexMut};
use pyo3::prelude::*;
use pyo3::{pyclass, pymethods, PyResult};
use pyo3::types::{PyIterator, PyList, PyTuple};
use crate::graph::edge::EdgeRS;
use crate::graph::node::GraphNodeRS;


#[pyclass(sequence)]
#[derive(Debug, Clone)]
pub struct Deque_NodeRS_EdgeRS {
    pub vec_deque: VecDeque<(Py<GraphNodeRS>, Py<EdgeRS>)>,
}


#[pymethods]
impl Deque_NodeRS_EdgeRS {
    fn __len__<'py>(&self) -> PyResult<usize> {
        Ok(self.vec_deque.len())
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyIterator>> { 
        PyIterator::from_bound_object( &PyList::new_bound(py, self.vec_deque.clone()) )
    }

    fn __getitem__<'py>(&self, py: Python<'py>, index: usize) -> PyResult<(&Bound<'py, GraphNodeRS>, &Bound<'py, EdgeRS>)> {
        let (node, edge) = &self.vec_deque[index];
        Ok((node.bind(py), edge.bind(py)))
    }

    fn __delitem__<'py>(&mut self, index: usize) -> PyResult<()> {
        self.vec_deque.remove(index);
        Ok(())
    }
}


impl Deque_NodeRS_EdgeRS {
    pub fn from_pylist(py: Python, node_edge_list: Bound<PyList>) -> PyResult<Py<Deque_NodeRS_EdgeRS>> {
        let mut vec_deque = VecDeque::with_capacity(node_edge_list.len());

        for node_edge_pa in node_edge_list.iter() {
            let tuple = node_edge_pa.downcast_into::<PyTuple>()?;
            let node = tuple.get_item(0)?.downcast_into::<GraphNodeRS>()?;
            let edge =  tuple.get_item(1)?.downcast_into::<EdgeRS>()?;
            vec_deque.push_back((node.unbind(), edge.unbind()));
        }

        Py::new(py, Deque_NodeRS_EdgeRS {vec_deque: vec_deque})
    }

    pub fn iter(&self) -> Iter<(Py<GraphNodeRS>, Py<EdgeRS>)> {
        self.vec_deque.iter()
    }
}

impl Index<usize> for Deque_NodeRS_EdgeRS {
    type Output = (Py<GraphNodeRS>, Py<EdgeRS>);

    fn index(&self, index: usize) -> &Self::Output {
        &self.vec_deque[index]
    }
}

impl IndexMut<usize> for Deque_NodeRS_EdgeRS {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.vec_deque[index]
    }
}


#[pyclass]
pub struct CycleRS {
    // deque[tuple[GraphNode, Edge]]
    #[pyo3(get)]
    // pub q: Py<PyAny>,
    pub q: Py<Deque_NodeRS_EdgeRS>,
}

#[pymethods]
impl CycleRS {
    #[new]
    pub fn new(
        py: Python,
        py_q: Bound<'_, PyList>,
    ) -> PyResult<Self> {
        // py_q: deque[tuple[GraphNode, Edge]]
        Ok(CycleRS { q: Deque_NodeRS_EdgeRS::from_pylist(py, py_q)? })
    }

    fn __str__(&self) -> String {
        format!("CycleRS(q={:?})", self.q)
    }

    fn __len__<'py>(&self, py: Python<'py>) -> PyResult<usize> {
        self.q.bind(py).len()
        // Ok(self.q.len())
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyIterator>> { 
    // fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Py<CycleIterator>> {
        // Py::new(py, CycleIterator {
        //     iter: self.q.iter()
        // })
        self.q.bind(py).iter()
    }

    fn __getitem__<'py>(&self, py: Python<'py>, item: usize) -> PyResult<Bound<'py, PyAny>> {
        self.q.bind(py).get_item(item)
    }

    pub fn validate_cycle<'py>(&self, py: Python<'py>) -> PyResult<bool> {
        // let (_, mut prev_edge) = self.q.get(
        //     self.q.len() - 1
        // ).unwrap();
        // for (node, edge) in self.q.iter() {
        //     if prev_edge.next_node_index != node.index {
        //         return false
        //     }
        //     prev_edge = *edge
        // };
        // Ok(true)

        let bound_q = self.q.bind(py).borrow();

        let (_, edge_pa) = bound_q.vec_deque.back().unwrap();
        let mut prev_edge = edge_pa.bind(py).borrow();

        for (node_py, edge_py) in bound_q.iter() {

            let node = node_py.bind(py).borrow();
            let edge = edge_py.bind(py).borrow();

            if prev_edge.next_node_index != node.index {
                return Ok(false)
            }
            prev_edge = edge;
        };

        Ok(true)
    }

    fn iter_by_pairs_reversed<'py>(&self, py: Python<'py>)  -> PyResult<Bound<'py, PyList>> {
        // deque[tuple[Node, Edge]]
        let bound_q = self.q.bind(py).borrow();

        let result_list = PyList::empty_bound(py);

        let (next_node_pa, _) = &bound_q[0];
        let mut next_node = next_node_pa.bind(py);

        for (prev_node_py, edge_py) in bound_q.iter().rev() {

            let prev_node = prev_node_py.bind(py);
            let edge = edge_py.bind(py);

            let new_tuple = PyTuple::new_bound(
                py, 
                [prev_node.as_any(), edge.as_any(), next_node.as_any()]
            );
            result_list.append(new_tuple)?;

            next_node = prev_node;
        }

        Ok(result_list)
    }
}


impl fmt::Display for CycleRS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.__str__())
    }
}



// #[pyclass]
// struct CycleIterator {
//     iter: std::collections::vec_deque::Iter<'a, (Py<GraphNodeRS>, Py<EdgeRS>)>,
// }


// #[pymethods]
// impl CycleIterator {
//     fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
//         slf
//     }

//     fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<(Py<GraphNodeRS>, Py<EdgeRS>)> {
//         if let Some(a) = slf.into().iter.next() {
//             let to_python_iter: Vec<dyn ToPyObject> = vec![node, edge];
//             let py_tuple = PyTuple::new_bound(slf.py(), to_python_iter.iter());
//             Some(py_tuple.to_object(slf.py()))
//         } else {
//             None
//         }
//     }
// }


// #[pyclass]
// struct CyclePairsIterator {
//     next_node: Py<GraphNodeRS>,
//     rev_iter: Py<PyIterator>,
// }


// #[pymethods]
// impl CyclePairsIterator {
//     fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
//         slf
//     }

//     fn __next__(&mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
//         match slf.rev_iter.next() {
//             Some((node, edge)) => {
//                 let to_python_iter: Vec<Box<dyn ToPyObject>> = vec![
//                     Box::new(node),
//                     Box::new(edge),
//                     Box::new(slf.next_node)
//                 ];
//                 let py_tuple = PyTuple::new_bound(slf.py(), to_python_iter.iter());
//                 slf.next_node = node;
//                 Some(py_tuple.to_object(slf.py()))
//             },
//             None => None
//         }
//     }
// }