# join_with_assignments

This program pairs/aligns two files in CSV format with [Polars](https://www.pola.rs).

The Kuhn-Munkres algorithm ([Munkres Assignments](https://crates.io/crates/pathfinding)) is used to solve the assignment problem.

Groupby is obtained after the following procedures:

    let lf_groupby: LazyFrame = lazyframe
    .group_by([col(my_table.side_a.column_aggregation)])
    .agg([
        col(my_table.side_a.column_count_lines),
        col(my_table.side_a.column_item_values),
    ]);

And the values ​​are aggregated with Munkres Assignments.

To see the minimal Munkres Assignments from two sets of numbers, run the [test](https://github.com/claudiofsr/join_with_assignments/blob/master/src/munkres.rs):

```
git clone https://github.com/claudiofsr/join_with_assignments.git

cd join_with_assignments

cargo test -- --show-output munkres_assignments_example
```
