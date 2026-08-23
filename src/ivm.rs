use crate::data_structure::*; 

pub struct IVM {
    forward_index: HashMap<SelectQuery, DataFrame>,
    reverse_index: HashMap<DataFrameKey, Vec<SelectQuery>>,
}

impl IVM {

    pub fn new() -> Self {
        IVM {
            forward_index: HashMap::new(),
            reverse_index: HashMap::new(),
        }
    }

    pub fn incremental_update(select_queries: Vec<SelectQuery>, update_query: UpdateQuery) -> Vec<DataFrameOperation> {
        // !!! TODO
    }

    pub fn search_impacted_queries(&self, update_query: UpdateQuery) -> Vec<SelectQuery> {
        // !!! TODO
    }

}
