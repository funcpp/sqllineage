pub(crate) mod edge;
pub(crate) mod node;
pub(crate) mod scope;

use crate::types::TableLineage;
use crate::types::TableRef;
use edge::{EdgeKind, RawEdge};
use node::{NodeId, RawNode};
use scope::{ScopeId, ScopeTree};

pub(crate) struct RawGraph {
    pub nodes: Vec<RawNode>,
    pub edges: Vec<RawEdge>,
    pub scopes: ScopeTree,
    pub tables: TableLineage,
}

impl RawGraph {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            scopes: ScopeTree::new(),
            tables: TableLineage::default(),
        }
    }

    pub fn add_node(&mut self, node: RawNode) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(node);
        id
    }

    pub fn add_output(&mut self, name: String) -> NodeId {
        self.add_node(RawNode::Output { name })
    }

    pub fn add_ref(&mut self, name: String, qualifier: Option<String>, scope: ScopeId) -> NodeId {
        self.add_node(RawNode::Ref {
            name,
            qualifier,
            scope,
        })
    }

    pub fn add_unqualified(&mut self, name: String, scope: ScopeId) -> NodeId {
        self.add_node(RawNode::Unqualified { name, scope })
    }

    pub fn add_star(&mut self, table: Option<TableRef>, scope: ScopeId) -> NodeId {
        self.add_node(RawNode::Star { table, scope })
    }

    pub fn add_edge(&mut self, from: NodeId, to: NodeId, kind: EdgeKind) {
        self.edges.push(RawEdge {
            from,
            to,
            kind,
            is_recursive_back_edge: false,
        });
    }
}
