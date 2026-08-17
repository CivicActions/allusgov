# Exporters

Exporters are responsible for converting the organizational hierarchy tree structure into various output
formats for different use cases and visualization tools. The allusgov project includes multiple exporters
that support different data formats, from simple flat structures to complex graph representations.

## Overview

The exporter system is built on a plugin architecture using a registry pattern. All exporters inherit from
base classes that provide common functionality:

- **ExporterBase**: The fundamental abstract base class for all exporters
- **FlatBaseExporter**: Provides functionality for flattening the tree structure into a tabular format
- **NetworkXBaseExporter**: Provides functionality for building graph structures using the NetworkX library


## Available Exporters

### JSON Exporter

**File**: `json_exporter.py`

Exports the organizational tree in JSON format. Generates two variants:
- **Flat format** (`-flat` suffix): A flat list representation of all organizations
- **Tree format** (`-tree` suffix): A nested hierarchical representation preserving parent-child
  relationships

**Use case**: Universal data exchange, web applications, and API responses.

### CSV Exporter

**File**: `csv_exporter.py`

Exports the tree as a standard comma-separated values file where attributes are embedded as JSON within
cells. This produces more manageable file sizes compared to the wide CSV variant.

**Use case**: Import into spreadsheet applications and databases.

### Wide CSV Exporter

**File**: `widecsv_exporter.py`

Exports the tree as a wide CSV with individual columns for each attribute. Implements smart filtering to
avoid extremely large column counts by:
- Skipping attributes containing lists longer than 10 items
- Avoiding nested list attributes (multiple lists in the same attribute)

**Use case**: Detailed spreadsheet analysis where each attribute is a separate column.

### Text Exporter

**File**: `text_exporter.py`

Exports the tree as an ASCII text representation with tree-style branch characters showing the hierarchy.
For merged data sources, displays the source names inline.

**Use case**: Human-readable documentation, terminal output, and tree structure inspection.

### GraphML Exporter

**File**: `graphml_exporter.py`

Exports the organizational structure as a GraphML (Graph Markup Language) file. GraphML is an XML-based
format for representing directed and undirected graphs with attributes.

**Use case**: Network analysis tools, graph visualization software like yEd, and data import into graph
databases.

### GEXF Exporter

**File**: `gexf_exporter.py`

Exports the graph in GEXF (Graph Exchange XML Format), a format designed specifically for complex networks.
Includes post-processing to remove dated timestamps to avoid spurious version control diffs.

**Use case**: Network analysis platforms like Gephi, and complex graph visualizations.

### DOT Exporter

**File**: `dot_exporter.py`

Exports the tree in DOT format (used by Graphviz). Creates a visualization-friendly representation of the
hierarchy.

**Use case**: Graphviz rendering for publication-quality diagrams, including SVG and PDF output.

### Cytoscape JSON Exporter

**File**: `cytoscapejson_exporter.py`

Exports the graph in Cytoscape JSON format, which is optimized for interactive network visualization in web
browsers using the Cytoscape.js library.

**Use case**: Interactive web-based network visualization and exploration.

## Architecture

### Export Path Convention

All exporters use a standardized path convention for output files:
```
data/{source}/{source}[-{suffix}].{ext}
```

Where:
- `{source}`: Data source name (e.g., "samgov", "cisagov")
- `{suffix}`: Optional suffix for multiple exports (e.g., "flat", "tree", "wide")
- `{ext}`: File extension specific to the format

### Flattening Process

The `FlatBaseExporter` base class provides a `flatten()` method that:
1. Traverses the tree in level-order
2. Extracts all attributes from each node
3. Creates a flat dictionary representation with:
   - `path`: Full path to the node in the hierarchy
   - `name`: Node name
   - All attributes flattened from nested structures
4. Returns the list of organizations and attribute names

### Graph Building

The `NetworkXBaseExporter` base class provides a `build_graph()` method that:
1. Flattens the tree to a maximum depth of 2 levels
2. Creates a directed graph (DiGraph) using NetworkX
3. Adds nodes with all attributes
4. Creates edges between children and their parents

## Plugin Registration

All exporters are registered with the `@EXPORTERS.register()` decorator, making them automatically
discoverable by the application. This allows the system to dynamically load and use exporters without
explicit imports.

Exporters extend the ExporterBase class and must define a `format_key` class attribute that serves as
the identifier for the export format and must implement the `export()` method to perform the actual export
logic.

Example:
```python
from typing import Any
from bigtree import Node
from allusgov.exporter.exporter_base import ExporterBase
from allusgov.registry.registry import EXPORTERS

@EXPORTERS.register("json")
class JSONExporter(ExporterBase):
    format_key = "json"

    def export(self, source: str, tree: Node, **kwargs: Any) -> None:
        # Export logic here
        pass
```

## Usage

Exporters are typically invoked through the command-line interface or programmatically via the registry.
Both the `build` and `merge` commands support an `--export` option that allows users to specify which
exporters to run after the import and processing steps are complete. The system will automatically call
the appropriate `export()` method for each specified exporter, passing the source name and the
organizational tree.

Each exporter's `export()` method takes:
- `source`: The data source identifier
- `tree`: The root Node of the organizational hierarchy
- `**kwargs`: Additional format-specific parameters
