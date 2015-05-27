cleanup Bucket and Agg, unify code paths.
Making most usages of Bucket to drop down to HasValue
removing support of "bind" method for arbitrary streams, and only allow binding to an aggregator after a group or window term.
fix tests (e.g. TestMultiBucketing)