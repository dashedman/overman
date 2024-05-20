use std::borrow::Borrow;
use std::collections::VecDeque;
use std::fmt;
use pyo3::prelude::*;
use pyo3::{pyclass, pymethods, PyResult};
use pyo3::types::{PyIterator, PyList, PySequence, PyTuple};
use crate::graph::edge::EdgeRS;
use crate::graph::node::GraphNodeRS;


#[pyclass]
pub struct CycleRS {
    // deque[tuple[GraphNode, Edge]]
    #[pyo3(get)]
    pub q: Py<PyAny>,
    // pub q: VecDeque<(Py<GraphNodeRS>, Py<EdgeRS>)>,
}

#[pymethods]
impl CycleRS {
    #[new]
    pub fn new(
        py_q: Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        // py_q: deque[tuple[GraphNode, Edge]]
        Ok(CycleRS { q: py_q.unbind() })

        // let mut q: VecDeque<(Py<GraphNodeRS>, Py<EdgeRS>)> = VecDeque::with_capacity(py_q.len()?);
        
        // for py_tuple in py_q.iter()? {
        //     let py_tuple_unwinded = py_tuple?;
        //     let tuple = py_tuple_unwinded.downcast::<PyTuple>()?;

        //     let node_pa = tuple.get_item(0)?;
        //     let edge_pa = tuple.get_item(1)?;

        //     let node = node_pa.downcast::<GraphNodeRS>()?.unbind();
        //     let edge = edge_pa.downcast::<EdgeRS>()?.unbind();
                
        //     let rs_tuple = (node, edge);

        //     q.push_back(rs_tuple);
        // }

        // Ok(CycleRS { q: q })
    }

    fn __str__(&self) -> String {
        format!("CycleRS(q={})", self.q)
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

    fn validate_cycle<'py>(&self, py: Python<'py>) -> PyResult<bool> {
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

        let bound_q = self.q.bind(py);

        let fpa = bound_q.get_item(-1)?;
        let first_pair = fpa.downcast::<PyTuple>()?;
        let mut prev_edge = first_pair.get_item(1)?.downcast::<EdgeRS>()?.borrow();

        for pair in bound_q.iter()? {
            let tpa = pair?;
            let tuple = tpa.downcast::<PyTuple>()?;

            let node = tuple.get_item(0)?.downcast::<GraphNodeRS>()?.borrow();
            let edge = tuple.get_item(1)?.downcast::<EdgeRS>()?.borrow();

            if prev_edge.next_node_index != node.index {
                return Ok(false)
            }
            prev_edge = edge;
        };

        Ok(true)
    }

    fn iter_by_pairs_reversed<'py>(&self, py: Python<'py>)  -> PyResult<Bound<'py, PyList>> {
        // deque[tuple[Node, Edge]]
        let bound_q = self.q.bind(py);

        let result_list = PyList::empty_bound(py);

        let q_iter_pa = bound_q.call_method0("__reversed__")?;

        let first_tuple = bound_q.get_item(0)?;
        let next_node_pa = first_tuple.get_item(0)?;
        let mut next_node = next_node_pa.downcast_into::<GraphNodeRS>()?;

        for item in q_iter_pa.iter()? {
            let tuple_pa = item?;
            let tuple = tuple_pa.downcast::<PyTuple>()?;

            let new_tuple = tuple.add(PyTuple::new_bound(py, [next_node.as_ref()]))?;
            result_list.append(new_tuple)?;

            let prev_node_pa = tuple.get_item(0)?;
            let prev_node = prev_node_pa.downcast::<GraphNodeRS>()?;
            next_node = prev_node.clone();
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