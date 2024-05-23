use std::collections::{HashMap, HashSet, VecDeque};
use pyo3::{pyclass, pymethods, types::{PyAnyMethods, PyList, PyListMethods, PyString}, Bound, Py, PyRef, PyResult, Python};
use crate::graph::{EdgeRS, GraphNodeRS};

use super::{CycleRS, Vec_NodeRS};


#[repr(u8)]
#[derive(Clone, Copy)]
#[derive(PartialEq)]
enum VisitStatus {
    NotVisited,
    // InProcessing,
    // Visited,
    InCycle,
    InBranch,
}
    

#[pyclass]
#[derive(Clone)]
pub struct GraphRS {
    #[pyo3(get)]
    // Vec<Py<GraphNodeRS>>
    pub nodes: Py<Vec_NodeRS>,
    names_to_index: HashMap<String, usize>,
    need_update: bool,
}


#[pymethods]
impl GraphRS {
    #[new]
    #[pyo3(signature = (nodes))]
    fn new<'py>(py: Python<'py>, nodes: Bound<'py, PyList>) -> PyResult<Self> {
        Ok(GraphRS {
            nodes: Vec_NodeRS::from_pylist(py, nodes)?,
            names_to_index: HashMap::new(),
            need_update: true,
        })
    }

    fn __getitem__<'py>(&self, py: Python<'py>, index: usize) -> PyResult<Bound<'py, GraphNodeRS>> {
        let nodes = self.nodes.bind(py).borrow();
        let node = &nodes.vec[index];
        Ok(node.bind(py).clone())
    }

    fn delete_node<'py>(&mut self, py: Python<'py>, node_index: usize) -> PyResult<()> {
        let mut bound_nodes = self.nodes.bind(py).borrow_mut();
        
        bound_nodes.vec.remove(node_index);

        for py_node in bound_nodes.vec.iter_mut() {
            let mut node = py_node.bind(py).borrow_mut();

            if node.index >= node_index {
                node.index -= 1;
            }

            // delete edges
            let mut edges_to_del = Vec::new();
            let mut edges_to_decrease = Vec::new();

            let mut edges_list = node.edges.bind(py).borrow_mut();
            for (index, py_edge_tail) in edges_list.vec.iter().enumerate() {
                let edge_tail = py_edge_tail.bind(py).borrow();

                if edge_tail.next_node_index == node_index {
                    edges_to_del.push(index);
                } else if edge_tail.next_node_index > node_index {
                    edges_to_decrease.push(index);
                }
            }

            for index in edges_to_decrease.iter() {
                edges_list.vec[*index].bind(py).borrow_mut().next_node_index -= 1;
            }

            edges_to_del.sort();
            for index in edges_to_del.iter().rev() {
                edges_list.vec.remove(*index);
            }

            for edge_pa in edges_list.vec.iter() {
                edge_pa.bind(py).borrow_mut().origin_node_index = node.index;
            }
        }

        self.need_update = true;

        Ok(())
    }

    fn __len__<'py>(&self, py: Python<'py>) -> usize {
        self.nodes.bind(py).borrow().vec.len()
    }

    #[getter]
    fn edges<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        // let edges = self.nodes
        //     .iter()
        //     .map(|node| node.edges.iter())
        //     .flatten()
        //     .collect();
        let bound_nodes = self.nodes.bind(py).borrow();
        let edges_list = PyList::empty_bound(py);

        for node in bound_nodes.iter() {
            let borrowed_node = node.bind(py).borrow();
            let edges = borrowed_node.edges.bind(py).borrow();
            for edge in edges.vec.iter() {
                edges_list.append(edge.bind(py))?;
            }
        }
        
        Ok(edges_list)
    }

    fn py_copy(&self) -> PyResult<Self> {
        Ok(self.clone())
    }

    fn delete_nodes<'py>(
        &mut self, 
        py: Python<'py>, 
        mut node_indexes_to_del: Vec<usize>
    ) -> PyResult<()> {
        node_indexes_to_del.sort();

        for index in node_indexes_to_del.iter().rev() {
            self.delete_node(py, *index)?;
        }

        Ok(())
    }

    fn get_index_for_coin_name<'py>(
        &mut self, 
        py: Python<'py>, 
        coin: Bound<PyString>,
    ) -> PyResult<usize> {
        if self.need_update {
            self.names_to_index.clear();
            
            for (index, node_pa) in self.nodes.bind(py).borrow().iter().enumerate() {
                let node = node_pa.bind(py).borrow();
                self.names_to_index.insert( node.value.clone(), index );
            }
                
            self.need_update = false;
        }
            
        Ok(*self.names_to_index.get(&coin.extract::<String>()?).unwrap())
    }

    fn get_node_for_coin<'py>(
        &mut self, 
        py: Python<'py>, 
        coin: Bound<PyString>,
    ) -> PyResult<Bound<'py, GraphNodeRS>> {
        let index = self.get_index_for_coin_name(py, coin)?;
        Ok(self.nodes.bind(py).borrow().vec[index].bind(py).clone())
    }

    fn get_edges_for_pair<'py>(
        &mut self, 
        py: Python<'py>, 
        coin1: Bound<PyString>, 
        coin2: Bound<PyString>,
    ) -> PyResult<Bound<'py, PyList>> {
        let edges = PyList::empty_bound(py);

        let coin1_raw = coin1.extract::<String>()?;
        let coin2_raw = coin2.extract::<String>()?;

        {
            let node_1 = self.get_node_for_coin(py, coin1)?;

            let bound_nodes = self.nodes.bind(py).borrow();
            let node_1_borrowed = node_1.borrow();
            let edges_1 = node_1_borrowed.edges.bind(py).borrow();
            for edge_pa in edges_1.vec.iter() {
                let edge = edge_pa.bind(py).borrow();
                let node_2_pa = &bound_nodes.vec[edge.next_node_index];
                let node_2 = node_2_pa.bind(py).borrow();
                if node_2.value == *coin2_raw {
                    edges.append(edge_pa)?;
                }
            } 
        } 

        {
            let node_2 = self.get_node_for_coin(py, coin2)?;

            let bound_nodes = self.nodes.bind(py).borrow();
            let node_2_borrowed = node_2.borrow();
            let edges_2 = node_2_borrowed.edges.bind(py).borrow();
            for edge_pa in edges_2.vec.iter() {
                let edge = edge_pa.bind(py).borrow();
                let node_1_pa = &bound_nodes.vec[edge.next_node_index];
                let node_1 = node_1_pa.bind(py).borrow();
                if node_1.value == *coin1_raw {
                    edges.append(edge_pa)?;
                }
            } 
        } 
        Ok(edges)
    }

    fn filter_from_noncycle_nodes(
        &mut self, 
        py: Python,
        base_nodes: Bound<PyList>,
    ) -> PyResult<()> {
        let nodes_len = self.nodes.bind(py).borrow().vec.len();
        let mut nodes_in_cycle = HashSet::new();

        // checked_nodes = [False] * len(self)
        for base_node_pa in base_nodes.iter() {
            let base_node = base_node_pa.downcast::<GraphNodeRS>()?.borrow();

            for node in self.get_nodes_in_cycles(py, base_node.index, 4)? {
                nodes_in_cycle.insert(node);
            }
        }

        let nodes_to_del: Vec<usize> = HashSet::from_iter(
            0_usize..nodes_len
        ).difference(
            &nodes_in_cycle
        ).map(
            |index| *index
        ).collect();

        self.delete_nodes(py, nodes_to_del)?;

        Ok(())
    }

    
    // fn get_profit_3<'py>(&self, py: Python<'py>, start: usize) -> PyResult<Bound<'py, PyAny>> {
    //     pyo3_asyncio::tokio::future_into_py(py, async move {
    //         self.get_profit_3_async(start).await?;

    //         Ok(Python::with_gil(|py| py.None()))
    //     })
    // }

    fn check_profit_experimental_3(
        &self, 
        py: Python,
        pivot_indexes: Bound<PyList>
    ) -> PyResult<Option<(f64, CycleRS, f64)>> {
        let now = std::time::Instant::now();

        let mut best_profit = 10000000.0;
        let mut best_cycle = None;
        for pivot_coin_index in pivot_indexes.iter() {
            let (profit, cycle_opt) = self.get_profit_3(py, pivot_coin_index.extract::<usize>()?)?;

            match cycle_opt {
                Some(cycle) => {
                    if !cycle.validate_cycle(py)? {
                        continue
                    }

                    if profit != -1.0 && profit < best_profit {
                        best_profit = profit;
                        best_cycle = Some(cycle);
                    }
                },
                None => continue
            }
        }
            

        if best_cycle.is_some() { 
            let elapsed = now.elapsed();
            return Ok(Some((best_profit, best_cycle.unwrap(), elapsed.as_secs_f64())))
        }
        return Ok(None)
    }

    

    // async fn get_profit_3_async<'py>(&self, start: usize) -> PyResult<(f64, Option<CycleRS>)> {
    fn get_profit_3(&self, py: Python, start: usize) -> PyResult<(f64, Option<CycleRS>)> {
        let bound_nodes = self.nodes.bind(py).borrow();
        
        let mut koef_in_node = vec![100000000.0; bound_nodes.vec.len()];
        let mut visit_from = vec![-1_isize; bound_nodes.vec.len()];

        // let edge_from = PyList::empty_bound(py);
        // for _ in 0..bound_nodes.vec.len() {
        //     edge_from.append(None::<Bound<PyAny>>)?;
        // }
        let mut edge_from = vec![None::<Bound<EdgeRS>>; bound_nodes.vec.len()];

        let mut visited_before: Vec<HashSet<usize>> = Vec::new();
        visited_before.resize_with(bound_nodes.vec.len(), || { HashSet::new() });

        let mut is_start = true;


        let mut q = VecDeque::new();
        q.push_back(start);
        // bfs
        while !q.is_empty() {
            let curr_index = q.pop_front().unwrap();
            let curr_koef = if is_start { 1.0 } else { koef_in_node[curr_index] };
            let curr_visited_before = std::mem::take(&mut visited_before[curr_index]);
            
            let curr_node_pa = &bound_nodes.vec[curr_index];
            let curr_node = curr_node_pa.bind(py).borrow();

            is_start = false;

            let bound_edges = curr_node.edges.bind(py).borrow();
            for edge_pa in bound_edges.vec.iter() {
                let edge = edge_pa.bind(py).borrow();

                let new_koef = curr_koef * edge.val;
                let next_index = edge.next_node_index;
                if new_koef == 0.0 {
                    continue;
                }
                else if curr_visited_before.contains(&next_index)  
                {
                    continue;
                }
                else if new_koef < koef_in_node[next_index] 
                {
                    koef_in_node[next_index] = new_koef;
                    visit_from[next_index] = edge.origin_node_index as isize;
                    // edge_from.set_item(next_index, Some(edge_pa))?;
                    edge_from[next_index] = Some(edge_pa.bind(py).clone());
                    
                    let next_visited_before = &mut visited_before[next_index];

                    // must be next_visited_before be curr_visited_before + (next_index)
                    *next_visited_before = curr_visited_before.clone();
                    next_visited_before.insert(next_index);
                    // // delete that not fit to curr_visited_before
                    // for to_del in next_visited_before.difference(curr_visited_before) {
                    //     next_visited_before.remove(to_del);
                    // }
                    // add from curr_visited_before
                    // for to_add in curr_visited_before.iter() {
                    //     next_visited_before.insert(*to_add);
                    // }

                    if next_index != start {
                        q.push_back(next_index);
                    }
                }   
            }

            visited_before[curr_index] = curr_visited_before;
        }
        

        if visit_from[start] == -1 {
            return Ok((-1.0, None))
        }

        Ok((
            koef_in_node[start], 
            Some(self.restore_cycle(
                py,
                start as isize,
                visit_from,
                edge_from,
            )?)
        ))

    }

    fn restore_cycle<'py>(
        &self,
        py: Python<'py>,
        head_index: isize,
        visit_from: Vec<isize>,
        edge_from: Vec<Option<Bound<EdgeRS>>>,
    ) -> PyResult<CycleRS> {
        let tail_index = visit_from[head_index as usize];
        let last_edge = edge_from[head_index as usize].as_ref().unwrap();

        let bound_nodes = self.nodes.bind(py).borrow();
        let mut cycle = VecDeque::new();

        let mut already_visited = HashSet::new();
        let mut curr_index = tail_index;
        let mut curr_edge = last_edge;
        // unwinding cycle
        while
            curr_index != head_index && 
            curr_index != -1 && 
            !already_visited.contains(&curr_index)
        {
            cycle.push_front((&bound_nodes.vec[curr_index as usize], curr_edge));
            already_visited.insert(curr_index);
            curr_edge = edge_from[curr_index as usize].as_ref().unwrap();
            curr_index = visit_from[curr_index as usize];
        }
        cycle.push_front((&bound_nodes.vec[head_index as usize], curr_edge));

        // convert VecDeque to PyDeque
        CycleRS::new(py, PyList::new_bound(py, cycle))
    }
        
    fn get_nodes_in_cycles(
        &self,
        py: Python,
        start: usize,
        max_length: usize,
    ) -> PyResult<Vec<usize>> {
        let bound_nodes = self.nodes.bind(py).borrow();

        let mut visited = vec![VisitStatus::NotVisited; bound_nodes.vec.len()];
        let mut node_depth = vec![-1_isize; bound_nodes.vec.len()];
        let mut left_to_cycle_end = vec![100000_isize; bound_nodes.vec.len()];
        
        fn dfs_search(
            curr_index: usize, 
            curr_depth: isize, 
            // state
            start: usize,
            max_length: isize, 
            visited: &mut Vec<VisitStatus>, 
            node_depth: &mut Vec<isize>,
            left_to_cycle_end: &mut Vec<isize>,
            // py state
            py: Python,
            bound_nodes: &PyRef<Vec_NodeRS>,
        ) -> PyResult<VisitStatus> {
            if curr_depth > max_length {
                return Ok(VisitStatus::NotVisited)
            }
            let visit_status = visited[curr_index];
            
            match visit_status {
                VisitStatus::NotVisited => visited[curr_index] = VisitStatus::InBranch,
                VisitStatus::InCycle => {
                    if left_to_cycle_end[curr_index] + curr_depth > max_length {
                        return Ok(VisitStatus::NotVisited)
                    }
                    return Ok(VisitStatus::InCycle)
                },
                VisitStatus::InBranch => {
                    if curr_index == start && curr_depth > 2 {
                        left_to_cycle_end[curr_index] = 0;
                        return Ok(VisitStatus::InCycle)
                    }
                    return Ok(VisitStatus::NotVisited)
                },
                // _ => {}
            }
            
            node_depth[curr_index] = curr_depth;
            let mut new_statuses: Vec<(VisitStatus, usize)> = Vec::new();

            let curr_node = bound_nodes.vec[curr_index].bind(py).borrow();
            let bound_edges = curr_node.edges.bind(py).borrow();
            for next_edge_pa in bound_edges.vec.iter() {
                let next_edge = next_edge_pa.bind(py).borrow();
                let next_index = next_edge.next_node_index;
            
                let new_status = dfs_search(
                    next_index, 
                    curr_depth + 1,
                    // state
                    start,
                    max_length,
                    visited,
                    node_depth,
                    left_to_cycle_end,
                    // py state
                    py,
                    bound_nodes
                )?;
                new_statuses.push((new_status, next_index));
            }
            
            
            // in_cycle_statuses = [node for status, node in new_statuses if status == VisitStatus.InCycle]
            let in_cycle_statuses: Vec<usize> = new_statuses.iter().filter(
                |(status, _)| *status == VisitStatus::InCycle
            ).map(
                |(_, index)| *index
            ).collect();

            if in_cycle_statuses.len() > 0 {
                let nearest_cycle_node = in_cycle_statuses.iter().min_by_key(|index| left_to_cycle_end[**index]).unwrap();
                left_to_cycle_end[curr_index] = curr_depth.min(
                    left_to_cycle_end[*nearest_cycle_node] + 1,
                );
                // second try with nodes not inCycle

                // not_cycle = {node for status, node in new_statuses if status != VisitStatus.InCycle};
                let not_cycle: HashSet<usize> = new_statuses.iter().filter(
                    |(status, _)| *status != VisitStatus::InCycle
                ).map(
                    |(_, index)| *index
                ).collect();

                // not_cycle_edges = [e for e in self.nodes[curr_index].edges if e.next_node_index in not_cycle];
                let not_cycle_edges: Vec<PyRef<EdgeRS>> = bound_edges.vec.iter().map(
                    |edge_pa| edge_pa.bind(py).borrow()
                ).filter(
                    |edge| not_cycle.contains(&edge.next_node_index)
                ).collect();

                for next_edge in not_cycle_edges {
                    let next_index = next_edge.next_node_index;
                    dfs_search(
                        next_index, 
                        (curr_depth + 1).min(left_to_cycle_end[curr_index] + 1),
                        // state
                        start,
                        max_length,
                        visited,
                        node_depth,
                        left_to_cycle_end,
                        // py state
                        py,
                        bound_nodes,
                    )?;
                }
                
                visited[curr_index] = VisitStatus::InCycle;
                return Ok(VisitStatus::InCycle)
            }
            
            
            left_to_cycle_end[curr_index] = 100000;
            visited[curr_index] = VisitStatus::NotVisited;
            return Ok(VisitStatus::NotVisited)
        }
        
        dfs_search(
            start, 0,
            //state
            start,
            max_length as isize,
            &mut visited,
            &mut node_depth,
            &mut left_to_cycle_end,
            // py state 
            py,
            &bound_nodes,
        )?;

        // [
        //     index for index, status in enumerate(visited)
        //     if status == VisitStatus.InCycle
        // ]
        let visited_indexes = visited.iter().enumerate().filter(
            |(_, status)| **status == VisitStatus::InCycle
        ).map(
            |(index, _)| index
        ).collect();

        return Ok(visited_indexes)
    }
}


// #[pyclass]
// struct EdgesIterator {
//     iter: dyn Iterator<Item = EdgeRS>,
// }


// #[pymethods]
// impl EdgesIterator {
//     fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
//         slf
//     }

//     fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<EdgeRS> {
//         slf.iter.next()
//     }
// }
