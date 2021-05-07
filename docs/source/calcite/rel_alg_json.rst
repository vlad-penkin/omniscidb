============================
Serialized JSON query format
============================

The top level object should hold the only field 'rels'. This field is an array of objects
describing relational algebra operations to perform.

Each relational operation has required 'id' and 'relOp' fields. 'id' field holds a string
with unique identifier of the operation. This identifier should be an unsigned integer
matching index of the operation in 'rels' array. 'relOp' field holds a name of the
operation and defines the rest of operation object structure. There are 11 operations
supported.

=================================================
Supported relational operations and their formats
=================================================

*************
Field formats
*************

In this section we describe formats used by various operation fields.

-----------
Input nodes
-----------

Operation nodes can reference their input nodes by their id. Input nodes are stored in
the 'inputs' field which is an array of string ids. For some nodes this field is optional
and when it is missing the previous node is used as an input node. Node can only use
preceeding nodes as its input, i.e. nodes cannot be forward-referenced.

-------------
Input columns
-------------

Input columns are referenced by their indices. Reference to N-th input column means a
reference to N-th output column of the input node. In case of multiple input nodes we
merge all input columns into a single array and a reference to columns would be its
position in this merged array. I.e. with two input nodes each having 3 columns we can
reference the last column of the second node using column index 5.

---------------
Operation hints
---------------

Hint are provided in a query and can be used by execution engine for more efficient query
execution. Hints are bound to operation nodes but currently all hints are treated as global
ones (affecting the whole query). Hints are passed in a string format. Hints string can
hold one or more serialized hints separated by '|' character. Each hint has a name and
optional parameters represented by either a list or key-value pairs.

Here is a high-level format description in EBNF notation:

::

  OPTIONS_LIST = '[', OPTION_NAME, { ", " OPTION_NAME }, ']'
  KV_OPTIONS = '{', OPTION_NAME, '=', OPTION_VALUE, { ", " OPTION_NAME, '=', OPTION_VALUE }, '}'
  OPTIONS = OPTIONS_LIST | KV_OPTIONS
  HINT = NAME, [OPTIONS]

Currently supported hints *(TODO: add descriptions)*:

- *cpu_mode*
- *overlaps_bucket_threshold*
- *overlaps_max_size*
- *overlaps_allow_gpu_build*

--------------------
Aggregate expression
--------------------

Aggregate expression is represented with an object with the following fields:

- *agg* - a name of the aggregate. Supported values are "COUNT", "MIN", "MAX", "SUM", "AVG",
  "APPROX_COUNT_DISTINCT", "APPROX_MEDIAN", "SAMPLE", "LAST_SAMPLE", "SINGLE_VALUE"
- *distinct* - a boolean field, True if the DISTINCT aggregate modifier is applied to the
  aggregate
- *type* - a type of the result
- *operands* - an array of input column indices

-----------
Type object
-----------

A type object is used to describe a column data type.

Fields:

- *type* - a string field holding type name. Supported names are "BIGINT", "INTEGER", "TINYINT",
  "SMALLINT", "FLOAT", "REAL", "DOUBLE", "DECIMAL", "CHAR", "VARCHAR", "BOOLEAN", "TIMESTAMP",
  "DATE", "TIME", "NULL", "ARRAY", "INTERVAL_DAY", "INTERVAL_HOUR", "INTERVAL_MINUTE",
  "INTERVAL_SECOND", "INTERVAL_MONTH", "INTERVAL_YEAR", "ANY", "TEXT", "POINT", "LINESTRING",
  "POLYGON", "MULTIPOLYGON", "GEOMETRY", "GEOGRAPHY"
- *nullable* - a boolean field indicating if the type is nullable
- *precision* - an optional integer field holding a precision of the type
- *scale* - an optional integer field holding a scale of the type

--------------
Literal values
--------------

Literal objects are used to represent literal values. Literal objects hold literal value
and type information in the following fields:

- *literal* - a literal value. Can be integer, float, boolean, or string
- *type* - a string field holding literal type. Supported values: "BIGINT", "INTEGER",
  "DOUBLE", "DECIMAL", "TIMESTAMP", "DATE", "TIME", "INTERVAL_DAY", "INTERVAL_HOUR",
  "INTERVAL_MINUTE", "INTERVAL_SECOND", "INTERVAL_MONTH", "INTERVAL_YEAR", "TEXT",
  "BOOLEAN", "NULL"
- *target_type* - a string field holding a target type
- *scale* - an integer field holding a scale of the value. Decimal values are passed using
  integer literal value and scale specifying a number of digits after the decimal point. For
  integer literals it should be 0. In other cases it should be MIN_INT (-2147483648)
- *precision* - an integer field holding a precision of the value. For numeric values it is
  a number of significant decimal digits. For Boolean it is always 1. For string CHAR literals
  it is a length of the string value, for VARCHAR literals it is -1
- *type_scale* - an integer field holding a scale of the *target_type*
- *type_precision* - an integer field holding a precision of the *target_type*

----------------
Collation object
----------------

Collation objects are used to specify sort order. Each collaction object consists of the
following fields:

- *field* - a reference to a column to sort by
- *direction* - a string field holding sort direction. Supported values are "DESCENDING" and
  "ASCENDING"
- *nulls* - a string field holding NULLs position in a sorted table. Supported values are
  "FIRST" and "LAST"

------------------------
Scalar expression object
------------------------

Scalar expression objects are used to represent column references, scalar literals and
expressions. Exactly one of fields *input*, *literal*, and *op* should present.

If an object has the *input* field then this field holds an input column reference.

If an object has the *literal* field then it is actually a lieral value object.

Otherwise it should has *op* field holding an operation name. Here are supported *op*
values and corresponding object layouts:

- **CASE** - a conditional operator. In this case *operands* field holds an array of
  scalar expressions with at least two elements. It has a format *[<condition1>, <value1>,
  <condition2>, <value2>, ...]*. If the number of elements is odd then the last expression
  is a value to use when all conditions result in *False* value
- **$SCALAR_QUERY** - a subquery operator. In this case *operands* is an empty array and
  *subquery* field holds an object which can be parsed as a query
- In all other cases we have an array of scalar expressions (length depends on operator
  arity) in *operands* field and the resulting type object in *type* field. Supported
  operations are ">", ">=", "<", "<=", "=", "<>", "IS NOT DISTINCT FROM" (bitwise equality),
  "+", "-", "*", "/", "MOD", "AND", "OR", "CAST", "NOT", "IS NULL", "IS NOT NULL", "IN",
  "PG_ANY", "PG_ALL", "PG_UNNEST". In case of "IN" operation we may have a subquery in the
  *subquery* field.
  *TODO: describe PG_* operations and window functions*

****************************************
EnumerableTableScan and LogicalTableScan
****************************************

Scan operation is a reference to existing table. OmniSci treats 'EnumerableTableScan'
and 'LogicalTableScan' equally.

Fields:

- *inputs* - should be an empty array
- *table* - a table reference. Field holds an array with two strings. The first string
  is a database name which is actually ignored by OmniSci (currently active database
  is always used). The second string is a table name
- *fieldNames* - an array of the resulting column names. Array length should match number
  of columns in the scanned table including the virtual 'rowid' at the end of the list.
- *hints* - optional hints field

**************
LogicalProject
**************

A projection operation.

Fields:

- *inputs* - an optional field with a single input node
- *fields* - an array of the resulting column names
- *expr* - an array of scalar expressions, one per output column
- *hints* - optional hints field


*************
LogicalFilter
*************

A filtering operation.

Fields:

- *inputs* - an optional field with a single input node
- *condition* - a filtering expression. Operation drops rows for which the filtering
  expression produces False.

****************
LogicalAggregate
****************

A groupby operation.

Fields:

- *inputs* - an optional field with a single input node
- *fields* - an array of the resulting column names
- *group* - an array of input column indices. Indices of input columns to be used as
  a group key. Operation requires that columns of the input node should be ordered so
  that the group key is a prefix of the input columns list. That means each element
  in *group* array is equal to its position in the array and *group* field just tells
  us the size of the group key. Key columns also form a prefix of output columns
  list.
- *aggs* - an array of aggregate expressions
- *hints* - optional hints field

***********
LogicalJoin
***********

A join operation.

Fields:

- *inputs* - an array with two input nodes
- *joinType* - a string field holding a join type. Supported values: "inner", "left"
- *condition* - a scalar expression with a join condition
- *hints* - optional hints field

***********
LogicalSort
***********

A sort operation.

Fields:

- *inputs* - an optional field with a single input node
- *collation* - an array of collation objects
- *fetch* - a literal object holding an output rows limit 
- *offset* - a literal object holding a number of rows to skip

*************
LogicalValues
*************

A set of tuple literals.

Fields:

- *inputs* - an empty array
- *type* - an array representing tuple type. Each element is a type object with an additional
  *name* field
- *tuples* - an array of tuple values. Each tuple is represented as an array of scalar expression
  objects holding literals

******************
LogicalTableModify
******************

************************
LogicalTableFunctionScan
************************

************
LogicalUnion
************

A union operation.

Fields:

- *inputs* - an array of input nodes
- *all* - a boolean field holding ALL modifier for the union operation