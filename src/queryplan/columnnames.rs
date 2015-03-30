use identifier::Identifier;

use std::ops::Deref;

/// Contains a vector of identifiers. Duplicates are allowed.
pub struct ColumnNames {
    column_names: Vec<Identifier>
}

impl ColumnNames {
    pub fn new(column_names: Vec<Identifier>) -> ColumnNames
    {
        ColumnNames {
            column_names: column_names
        }
    }

    pub fn get_column_offsets(&self, name: &Identifier) -> Vec<u32> {
        self.column_names.iter()
            .enumerate()
            .filter_map(|(i, ident)| if ident == name { Some(i as u32) } else { None })
            .collect()
    }
}

impl Deref for ColumnNames {
    type Target = [Identifier];

    fn deref(&self) -> &[Identifier] { &self.column_names }
}
