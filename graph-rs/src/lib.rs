mod graph;

use pyo3::prelude::*;
use graph::{EdgeRS, GraphNodeRS, CycleRS};


/// A Python module implemented in Rust.
#[pymodule]
fn graph_rs(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<EdgeRS>()?;
    m.add_class::<GraphNodeRS>()?;
    m.add_class::<CycleRS>()?;
    Ok(())
}
